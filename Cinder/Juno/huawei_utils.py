# Copyright (c) 2016 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import contextlib
import six
import threading
import time

from oslo.utils import units

from cinder import exception
from cinder.i18n import _
from cinder.openstack.common import log as logging
from cinder.openstack.common import loopingcall
from cinder.volume.drivers.huawei import constants

try:
    import eventlet
    from eventlet import patcher as eventlet_patcher
except ImportError:
    eventlet = None
    eventlet_patcher = None

LOG = logging.getLogger(__name__)


def encode_name(name):
    pre_name = name.split("-")[0]
    vol_encoded = str(hash(name))
    if vol_encoded.startswith('-'):
        newuuid = pre_name + vol_encoded
    else:
        newuuid = pre_name + '-' + vol_encoded
    return newuuid


def encode_host_name(name):
    if name and (len(name) > constants.MAX_HOSTNAME_LENGTH):
        name = six.text_type(hash(name))
    return name


def wait_for_condition(func, interval, timeout):
    start_time = time.time()

    def _inner():
        try:
            res = func()
        except Exception as ex:
            raise exception.VolumeBackendAPIException(data=ex)

        if res:
            raise loopingcall.LoopingCallDone()

        if int(time.time()) - start_time > timeout:
            msg = (_('wait_for_condition: %s timed out.')
                   % func.__name__)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    timer = loopingcall.FixedIntervalLoopingCall(_inner)
    timer.start(interval=interval).wait()


def get_volume_size(volume):
    """Calculate the volume size.

    We should divide the given volume size by 512 for the 18000 system
    calculates volume size with sectors, which is 512 bytes.
    """
    volume_size = units.Gi / 512  # 1G
    if int(volume.size) != 0:
        volume_size = int(volume.size) * units.Gi / 512

    return volume_size


def get_volume_metadata(volume):
    if 'volume_metadata' in volume:
        metadata = volume.get('volume_metadata')
        return dict((item['key'], item['value']) for item in metadata)

    return {}


def get_admin_metadata(volume):
    admin_metadata = {}
    if 'admin_metadata' in volume:
        admin_metadata = volume.admin_metadata
    elif 'volume_admin_metadata' in volume:
        metadata = volume.get('volume_admin_metadata', [])
        admin_metadata = dict((item['key'], item['value']) for item in metadata)

    LOG.debug("Volume ID: %(id)s, admin_metadata: %(admin_metadata)s.",
              {"id": volume.id, "admin_metadata": admin_metadata})
    return admin_metadata


def get_snapshot_metadata_value(snapshot):
    if 'snapshot_metadata' in snapshot:
        metadata = snapshot.snapshot_metadata
        return dict((item['key'], item['value']) for item in metadata)

    return {}


class ReaderWriterLock(object):
    WRITER = 'w'
    READER = 'r'

    @staticmethod
    def _fetch_current_thread_functor():
        if eventlet is not None and eventlet_patcher is not None:
            if eventlet_patcher.is_monkey_patched('thread'):
                return eventlet.getcurrent
        return threading.current_thread

    def __init__(self):
        self._writer = None
        self._pending_writers = collections.deque()
        self._readers = {}
        self._cond = threading.Condition()
        self._current_thread = self._fetch_current_thread_functor()

    @property
    def has_pending_writers(self):
        """Returns if there are writers waiting to become the *one* writer."""
        return bool(self._pending_writers)

    def is_writer(self, check_pending=True):
        """Returns if the caller is the active writer or a pending writer."""
        me = self._current_thread()
        if self._writer == me:
            return True
        if check_pending:
            return me in self._pending_writers
        else:
            return False

    @property
    def owner(self):
        """Returns whether the lock is locked by a writer or reader."""
        if self._writer is not None:
            return self.WRITER
        if self._readers:
            return self.READER
        return None

    def is_reader(self):
        """Returns if the caller is one of the readers."""
        me = self._current_thread()
        return me in self._readers

    @contextlib.contextmanager
    def read_lock(self):
        """Context manager that grants a read lock.
        Will wait until no active or pending writers.
        Raises a ``RuntimeError`` if a pending writer tries to acquire
        a read lock.
        """
        me = self._current_thread()
        if me in self._pending_writers:
            raise RuntimeError("Writer %s can not acquire a read lock"
                               " while waiting for the write lock"
                               % me)
        with self._cond:
            while True:
                # No active writer, or we are the writer;
                # we are good to become a reader.
                if self._writer is None or self._writer == me:
                    try:
                        self._readers[me] = self._readers[me] + 1
                    except KeyError:
                        self._readers[me] = 1
                    break
                # An active writer; guess we have to wait.
                self._cond.wait()
        try:
            yield self
        finally:
            # I am no longer a reader, remove *one* occurrence of myself.
            # If the current thread acquired two read locks, then it will
            # still have to remove that other read lock; this allows for
            # basic reentrancy to be possible.
            with self._cond:
                try:
                    me_instances = self._readers[me]
                    if me_instances > 1:
                        self._readers[me] = me_instances - 1
                    else:
                        self._readers.pop(me)
                except KeyError:
                    pass
                self._cond.notify_all()

    @contextlib.contextmanager
    def write_lock(self):
        """Context manager that grants a write lock.
        Will wait until no active readers. Blocks readers after acquiring.
        Raises a ``RuntimeError`` if an active reader attempts to acquire
        a lock.
        """
        me = self._current_thread()
        i_am_writer = self.is_writer(check_pending=False)
        if self.is_reader() and not i_am_writer:
            raise RuntimeError("Reader %s to writer privilege"
                               " escalation not allowed" % me)
        if i_am_writer:
            # Already the writer; this allows for basic reentrancy.
            yield self
        else:
            with self._cond:
                self._pending_writers.append(me)
                while True:
                    # No readers, and no active writer, am I next??
                    if len(self._readers) == 0 and self._writer is None:
                        if self._pending_writers[0] == me:
                            self._writer = self._pending_writers.popleft()
                            break
                    self._cond.wait()
            try:
                yield self
            finally:
                with self._cond:
                    self._writer = None
                    self._cond.notify_all()
