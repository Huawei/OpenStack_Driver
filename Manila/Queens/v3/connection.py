# Copyright (c) 2015 Huawei Technologies Co., Ltd.
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

import json
import os
import random
import string
import tempfile
import time

from oslo_config import cfg
from oslo_log import log
import oslo_messaging as messaging
from oslo_serialization import jsonutils
from oslo_utils import excutils
from oslo_utils import strutils
from oslo_utils import units
import six

from manila.common import constants as common_constants
from manila import context as manila_context
from manila.data import utils as data_utils
from manila import exception
from manila.i18n import _
from manila import rpc
from manila.share.drivers.huawei import base as driver
from manila.share.drivers.huawei import constants
from manila.share.drivers.huawei import huawei_utils
from manila.share.drivers.huawei.v3 import helper
from manila.share.drivers.huawei.v3 import hypermetro
from manila.share.drivers.huawei.v3 import manager
from manila.share.drivers.huawei.v3 import replication
from manila.share.drivers.huawei.v3 import rpcapi as v3_rpcapi
from manila.share.drivers.huawei.v3 import smartx
from manila.share import share_types
from manila.share import utils as share_utils
from manila import utils

CONF = cfg.CONF


LOG = log.getLogger(__name__)


class V3StorageConnection(driver.HuaweiBase):
    """Helper class for Huawei OceanStor V3 storage system."""

    def __init__(self, configuration, **kwargs):
        super(V3StorageConnection, self).__init__(configuration)
        self.helper = helper.RestHelper(self.configuration)
        self.replica_mgr = replication.ReplicaPairManager(self.helper)
        self.metro_mgr = hypermetro.HyperPairManager(self.helper,
                                                     self.configuration)
        self.rpc_client = v3_rpcapi.HuaweiV3API()
        self.private_storage = kwargs.get('private_storage')
        self.qos_support = False
        self.snapshot_support = False
        self.replication_support = False
        self.metro_domain = None
        self.remote_backend = None
        self.vstore_pair_id = None

    def _setup_rpc_server(self, server_version, endpoints):
        host = "%s@%s" % (CONF.host, self.configuration.config_group)
        target = messaging.Target(topic=self.rpc_client.topic, server=host,
                                  version=server_version)
        self.rpc_server = rpc.get_server(target, endpoints)
        self.rpc_server.start()

    def connect(self):
        """Try to connect to V3 server."""
        self.helper.login()
        self.helper.get_product()
        self.check_storage_pools()
        rpc_manager = manager.HuaweiV3Manager(self, self.replica_mgr,
                                              self.metro_mgr)
        self._setup_rpc_server(rpc_manager.RPC_API_VERSION, [rpc_manager])
        self._setup_conf()

    def check_storage_pools(self):
        root = self.helper._read_xml()
        s_pools = []
        all_pool_info = self.helper._find_all_pool_info()
        for pool in all_pool_info['data']:
            if pool.get('USAGETYPE') in (constants.FILE_SYSTEM_POOL_TYPE,
                                         constants.DORADO_V6_POOL_TYPE):
                s_pools.append(pool['NAME'])

        pool_name_list = root.findtext('Filesystem/StoragePool')
        pool_name_list = pool_name_list.split(";")
        for pool_name in pool_name_list:
            pool_name = pool_name.strip().strip('\n')
            if pool_name not in s_pools:
                msg = (_("File system storage pool %s does not "
                         "exist on the array.") % pool_name)
                raise exception.InvalidHost(reason=msg)

    def _setup_conf(self):
        root = self.helper._read_xml()

        snapshot_support = root.findtext('Storage/SnapshotSupport')
        if snapshot_support:
            self.snapshot_support = strutils.bool_from_string(
                snapshot_support, strict=True)

        replication_support = root.findtext('Storage/ReplicationSupport')
        if replication_support:
            self.replication_support = strutils.bool_from_string(
                replication_support, strict=True)

    def check_is_active_client(self):
        vstore_pair_info = self.helper.get_hypermetro_vstore_by_pair_id(
            self.vstore_pair_id)
        active_flag = vstore_pair_info.get('ACTIVEORPASSIVE')
        if active_flag == '0':
            return True
        return False

    def create_share(self, share, share_server=None):
        """Create a share."""
        pool_name = share_utils.extract_host(share['host'], level='pool')
        if not pool_name:
            msg = _("Pool is not available in the share host field.")
            raise exception.InvalidHost(reason=msg)

        result = self.helper._find_all_pool_info()
        poolinfo = self.helper._find_pool_info(pool_name, result)
        if not poolinfo:
            msg = (_("Can not find pool info by pool name: %s.") % pool_name)
            raise exception.InvalidHost(reason=msg)

        fs_info = self._create_filesystem(share, poolinfo=poolinfo)

        fs_id = fs_info['ID']
        share_name = share['name']
        share_proto = share['share_proto']
        vstore_id = fs_info.get('vstoreId')

        try:
            self.helper.create_share(share_name, fs_id, share_proto, vstore_id)
        except Exception as err:
            self._delete_filesystem(fs_id)
            raise exception.InvalidShare(
                reason=(_('Failed to create share %(name)s. Reason: %(err)s.')
                        % {'name': share_name, 'err': err}))

        self.private_storage.update(share['id'], {'replica_pair_id': ''})

        ip = self._get_share_ip(share_server, fs_info, vstore_id)
        location = self._get_location_path(share_name, share_proto, ip)

        return location

    def _delete_filesystem(self, fs_id, context=None):
        if fs_id is not None:
            qos_id = self.helper.get_qosid_by_fsid(fs_id)
            if qos_id:
                self.remove_qos_fs(fs_id, qos_id)

            fs_info = self.helper._get_fs_info_by_id(fs_id)
            if json.loads(fs_info.get('HYPERMETROPAIRIDS')) and \
                    self.metro_domain:
                self._delete_metro_filesystem(fs_id, fs_info)
                return
            params = {"ID": fs_id}
            self.helper._delete_fs(params)

    def _create_filesystem(self, share, **kwargs):
        fs_id = None
        # We sleep here to ensure the newly created filesystem can be read.
        wait_interval = self._get_wait_interval()
        timeout = self._get_timeout()

        try:
            fs_id = self.allocate_container(share, **kwargs)
            fs = self.helper._get_fs_info_by_id(fs_id)
            end_time = time.time() + timeout

            while not (self.check_fs_status(fs['HEALTHSTATUS'],
                                            fs['RUNNINGSTATUS'])
                       or time.time() > end_time):
                time.sleep(wait_interval)
                fs = self.helper._get_fs_info_by_id(fs_id)

            if not self.check_fs_status(fs['HEALTHSTATUS'],
                                        fs['RUNNINGSTATUS']):
                raise exception.InvalidShare(
                    reason=(_('Invalid status of filesystem: '
                              'HEALTHSTATUS=%(health)s '
                              'RUNNINGSTATUS=%(running)s.')
                            % {'health': fs['HEALTHSTATUS'],
                               'running': fs['RUNNINGSTATUS']}))
        except Exception as err:
            self._delete_filesystem(fs_id)
            message = (_('Failed to create share %(name)s. '
                         'Reason: %(err)s.')
                       % {'name': share['name'],
                          'err': err})
            LOG.exception(message)
            raise exception.InvalidShare(reason=message)

        return fs

    def _get_share_ip(self, share_server, fs_info, vstore_id=None):
        """"Get share logical ip."""
        metro_id = self._get_metro_id_from_fs_info(fs_info)
        if share_server:
            if vstore_id:
                self.helper.modify_logical_port(
                    share_server['backend_details']['logical_port_id'],
                    vstore_id)
            ips = [share_server['backend_details'].get('ip')]
        else:
            if metro_id:
                ips = [self.metro_logic_ip]
            else:
                ips = huawei_utils.get_logical_ips(self.helper)

        return ips

    def _update_filesystem(self, fs_info, size):
        fs_id = fs_info.get('ID')
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            metro_id = self._get_metro_id_from_fs_info(fs_info)
            metro_info = self.helper.get_hypermetro_pair_by_id(metro_id)
            remote_fs_id = self._get_remote_fs_id(fs_id, metro_info)
            try:
                context = manila_context.get_admin_context()
                self.rpc_client.update_filesystem(context, self.remote_backend,
                                                  remote_fs_id, size)
            except Exception as err:
                msg = (_("Failed to update remote filesystem %(fs_id)s. "
                         "Reason: %(err)s")
                       % {"fs_id": remote_fs_id, "err": err})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        self.helper._change_share_size(fs_id, size)

    def extend_share(self, share, new_size, share_server):
        share_proto = share['share_proto']
        share_name = share['name']

        # The unit is in sectors.
        size = int(new_size) * units.Mi * 2
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_url_type = self.helper._get_share_url_type(share_proto)
        share = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)
        if not share:
            err_msg = (_("Can not get share ID by share %s.")
                       % share_name)
            LOG.error(err_msg)
            raise exception.InvalidShareAccess(reason=err_msg)

        fsid = share['FSID']
        fs_info = self.helper._get_fs_info_by_id(fsid)

        current_size = int(fs_info['CAPACITY']) / units.Mi / 2
        if current_size >= new_size:
            err_msg = (_("New size for extend must be bigger than "
                         "current size on array. (current: %(size)s, "
                         "new: %(new_size)s).")
                       % {'size': current_size, 'new_size': new_size})

            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)
        self._update_filesystem(fs_info, size)

    def shrink_share(self, share, new_size, share_server):
        """Shrinks size of existing share."""
        share_proto = share['share_proto']
        share_name = share['name']
        # The unit is in sectors.
        size = int(new_size) * units.Mi * 2
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_url_type = self.helper._get_share_url_type(share_proto)
        share_info = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)
        if not share_info:
            err_msg = (_("Can not get share ID by share %s.")
                       % share_name)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        fsid = share_info['FSID']
        fs_info = self.helper._get_fs_info_by_id(fsid)
        if not fs_info:
            err_msg = (_("Can not get filesystem info by filesystem ID: %s.")
                       % fsid)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        consumed_size = float(fs_info['MINSIZEFSCAPACITY'])
        if consumed_size > size:
            LOG.error('%(used).3fGB of share %(id)s is already used. '
                      'Cannot shrink to %(newsize)dGB.',
                      {'used': consumed_size / units.Mi / 2,
                       'id': share['id'],
                       'newsize': new_size})
            raise exception.ShareShrinkingPossibleDataLoss(
                share_id=share['id'])

        self._update_filesystem(fs_info, size)

    def check_fs_status(self, health_status, running_status):
        if (health_status == constants.STATUS_FS_HEALTH
                and running_status == constants.STATUS_FS_RUNNING):
            return True
        else:
            return False

    def assert_filesystem(self, fsid):
        fs = self.helper._get_fs_info_by_id(fsid)
        if not self.check_fs_status(fs['HEALTHSTATUS'],
                                    fs['RUNNINGSTATUS']):
            err_msg = (_('Invalid status of filesystem: '
                         'HEALTHSTATUS=%(health)s '
                         'RUNNINGSTATUS=%(running)s.')
                       % {'health': fs['HEALTHSTATUS'],
                          'running': fs['RUNNINGSTATUS']})
            raise exception.StorageResourceException(err_msg)

    def create_snapshot(self, snapshot, share_server=None):
        """Create a snapshot."""
        snap_name = snapshot['id']
        share_proto = snapshot['share']['share_proto']
        vstore_id = self.get_vstore_id_by_fs_info(snapshot['share_name'])
        share_url_type = self.helper._get_share_url_type(share_proto)
        share = self.helper._get_share_by_name(snapshot['share_name'],
                                               share_url_type, vstore_id)

        if not share:
            err_msg = _('Can not create snapshot,'
                        ' because share id is not provided.')
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        sharefsid = share['FSID']
        snapshot_name = "share_snapshot_" + snap_name
        snap_id = self.helper._create_snapshot(sharefsid,
                                               snapshot_name)
        LOG.info('Creating snapshot id %s.', snap_id)
        return snapshot_name.replace("-", "_")

    def delete_snapshot(self, snapshot, share_server=None):
        """Delete a snapshot."""
        LOG.debug("Delete a snapshot.")
        snap_name = snapshot['id']

        sharefsid = self.helper.get_fsid_by_name(snapshot['share_name'])

        if sharefsid is None:
            LOG.warning('Delete snapshot share id %s fs has been '
                        'deleted.', snap_name)
            return

        snapshot_id = self.helper._get_snapshot_id(sharefsid, snap_name)
        snapshot_info = self.helper._get_snapshot_by_id(snapshot_id)
        snapshot_flag = self.helper._check_snapshot_id_exist(snapshot_info)

        if snapshot_flag:
            self.helper._delete_snapshot(snapshot_id)
        else:
            LOG.warning("Can not find snapshot %s on array.", snap_name)

    def update_share_stats(self, stats_dict):
        """Retrieve status info from share group."""
        root = self.helper._read_xml()
        all_pool_info = self.helper._find_all_pool_info()
        stats_dict["pools"] = []

        pool_name_list = root.findtext('Filesystem/StoragePool')
        pool_name_list = pool_name_list.split(";")
        for pool_name in pool_name_list:
            pool_name = pool_name.strip().strip('\n')
            capacity = self._get_capacity(pool_name, all_pool_info)
            disk_type = self._get_disk_type(pool_name, all_pool_info)

            if capacity:
                pool = dict(
                    pool_name=pool_name,
                    total_capacity_gb=capacity['TOTALCAPACITY'],
                    free_capacity_gb=capacity['CAPACITY'],
                    provisioned_capacity_gb=(
                        capacity['PROVISIONEDCAPACITYGB']),
                    max_over_subscription_ratio=(
                        self.configuration.safe_get(
                            'max_over_subscription_ratio')),
                    allocated_capacity_gb=capacity['CONSUMEDCAPACITY'],
                    qos=[self._get_qos_capability(), False],
                    reserved_percentage=0,
                    dedupe=[True, False],
                    compression=[True, False],
                    huawei_smartcache=[True, False],
                    huawei_controller=[True, False],
                    huawei_smartpartition=[True, False],
                    huawei_sectorsize=[True, False],
                    huawei_share_privilege=[True, False],
                )

                if disk_type:
                    pool['huawei_disk_type'] = disk_type

                if self.configuration.nas_product != "Dorado":
                    pool['thin_provisioning'] = [True, False]
                else:
                    pool['thin_provisioning'] = True

                if self.metro_domain and self.check_is_active_client():
                    pool['hypermetro'] = True
                else:
                    pool['hypermetro'] = False

                stats_dict["pools"].append(pool)

        if not stats_dict["pools"]:
            err_msg = _("The StoragePool is None.")
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

    def _get_qos_capability(self):
        version = self.helper.find_array_version()
        if (self.configuration.nas_product == "Dorado" or
                version.upper() >= constants.MIN_ARRAY_VERSION_FOR_QOS):
            self.qos_support = True
        else:
            self.qos_support = False
        return self.qos_support

    def get_vstore_id_by_fs_info(self, share_name):
        fs_info = self.helper._get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.warning('FS %s to delete not exist.', share_name)
            return
        return fs_info.get('vstoreId')

    def rpc_delete_share(self, context, share_name, share_proto):
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_url_type = self.helper._get_share_url_type(share_proto)
        share_info = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)

        if not share_info:
            LOG.warning('The share was not found. Share name:%s',
                        share_name)
            fsid = self.helper.get_fsid_by_name(share_name)
            if fsid:
                self._delete_filesystem(fsid)
                return
            LOG.warning('The filesystem was not found.')
            return

        share_id = share_info['ID']
        share_fs_id = share_info['FSID']

        if share_id:
            self.helper._delete_share_by_id(
                share_id, share_url_type, vstore_id)
        self._delete_filesystem(share_fs_id)

    def delete_share(self, share, share_server=None):
        """Delete share."""
        context = manila_context.get_admin_context()
        share_name = share['name']
        share_proto = share['share_proto']
        fs_info = self.helper._get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.warning("FS %s to delete not exist.", share_name)
            return
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            self.rpc_client.get_remote_fs_info(
                context, share_name, self.remote_backend)
            if self.check_is_active_client():
                self.rpc_delete_share(context, share_name, share_proto)
            else:
                self.rpc_client.delete_share(
                    context, share_name, share_proto, self.remote_backend)
        else:
            self.rpc_delete_share(context, share_name, share_proto)
        self.private_storage.delete(share['id'])

    def _get_remote_fs_id(self, fs_id, metro_info):
        if fs_id == metro_info['LOCALOBJID']:
            remote_fs_id = metro_info['REMOTEOBJID']
        elif fs_id == metro_info['REMOTEOBJID']:
            remote_fs_id = metro_info['LOCALOBJID']
        else:
            msg = (_("Filesystem %s is not belong to a HyperMetro "
                     "filesystem.") % fs_id)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        return remote_fs_id

    def _get_metro_id_from_fs_info(self, fs_info):
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            metro_id = json.loads(fs_info.get('HYPERMETROPAIRIDS'))
            if not metro_id:
                msg = _("Filesystem is a HyperMetro, but failed to get the "
                        "metro id")
                LOG.error(msg)
                raise exception.ShareResourceNotFound(reason=msg)
            metro_id = metro_id[0]
            return metro_id

    def _delete_metro_filesystem(self, fs_id, fs_info):
        metro_id = self._get_metro_id_from_fs_info(fs_info)
        if not metro_id:
            return
        metro_info = self.helper.get_hypermetro_pair_by_id(metro_id)
        remote_fs_id = self._get_remote_fs_id(fs_id, metro_info)

        try:
            self.metro_mgr.delete_metro_pair(metro_id=metro_id)
        except Exception as err:
            msg = (_("Failed to delete HyperMetro filesystem pair "
                     "%(metro_id)s. Reason: %(err)s")
                   % {"metro_id": metro_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        vstore_info = self.helper.get_hypermetro_vstore_by_pair_id(
            self.vstore_pair_id)
        try:
            context = manila_context.get_admin_context()
            remote_vstore_id = vstore_info.get('REMOTEVSTOREID')
            if remote_vstore_id:
                params = {"ID": remote_fs_id, 'vstoreId': remote_vstore_id}
            else:
                params = {"ID": remote_fs_id}
            LOG.info("Start to delete remote filesystem: %s" % remote_fs_id)
            self.rpc_client.delete_remote_filesystem(
                context, self.remote_backend, params)
        except Exception as err:
            msg = (_("Failed to delete remote filesystem %(fs_id)s. "
                     "Reason: %(err)s") % {"fs_id": remote_fs_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        try:
            local_vstore_id = vstore_info.get('LOCALVSTOREID')
            if local_vstore_id:
                params = {"ID": fs_id, 'vstoreId': local_vstore_id}
            else:
                params = {"ID": fs_id}
            LOG.info("Start to delete local filesystem: %s" % fs_id)
            self.helper._delete_fs(params)
        except Exception as err:
            msg = (_("Failed to delete local filesystem %(fs_id)s. "
                     "Reason: %(err)s") % {"fs_id": fs_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

    def _create_from_snapshot_by_clone(self, share, parent_fs_id,
                                       parent_snap_id, share_server):
        fs_info = self._create_filesystem(
            share, parent_fs_id=parent_fs_id, parent_snap_id=parent_snap_id)
        fs_id = fs_info['ID']
        vstore_id = fs_info.get('vstoreId')

        clone_size = int(fs_info['CAPACITY'])
        new_size = int(share['size']) * units.Mi * 2

        try:
            if new_size != clone_size:
                self.helper._change_share_size(fs_id, new_size)

            self.helper.split_clone_fs(fs_id)

            def _split_done():
                fs_info = self.helper._get_fs_info_by_id(fs_id)
                return fs_info['ISCLONEFS'] != 'true'
            huawei_utils.wait_for_condition(_split_done, 5, 3600 * 24)
        except Exception:
            LOG.exception('Create clone FS %s error.', fs_id)
            self.delete_share(share)
            raise

        share_name = share['name']
        share_proto = share['share_proto']
        share_url_type = self.helper._get_share_url_type(share_proto)
        share_info = self.helper._get_share_by_name(share_name, share_url_type)
        if not share_info:
            self.helper.create_share(share_name, fs_id, share_proto)
        else:
            accesses = self.helper._get_all_access_from_share(
                share_info['ID'], share_proto)
            for i in accesses:
                self.helper._remove_access_from_share(i, share_proto)

        ips = self._get_share_ip(share_server, fs_info, vstore_id)
        locations = self._get_location_path(share_name, share_proto, ips)

        return locations

    def _create_from_snapshot_by_host(self, share, snapshot, share_server):
        old_share_name = self.helper.get_share_name_by_id(
            snapshot['share_id'])
        old_share_proto = self._get_share_proto(old_share_name)
        if not old_share_proto:
            err_msg = (_("Cannot find source share %(share)s of "
                         "snapshot %(snapshot)s on array.")
                       % {'share': snapshot['share_id'],
                          'snapshot': snapshot['snapshot_id']})
            LOG.error(err_msg)
            raise exception.ShareResourceNotFound(
                share_id=snapshot['share_id'])

        new_share_paths = self.create_share(share, share_server)
        new_share = {
            "share_proto": share['share_proto'],
            "size": share['size'],
            "name": share['name'],
            "mount_paths": [i.replace("\\", "/") for i in new_share_paths],
            "mount_src":
                tempfile.mkdtemp(prefix=constants.TMP_PATH_DST_PREFIX),
            "id": snapshot['share_id'],
        }

        old_share_paths = self._get_location_path(old_share_name,
                                                  old_share_proto)
        old_share = {
            "share_proto": old_share_proto,
            "name": old_share_name,
            "mount_paths": [i.replace("\\", "/") for i in old_share_paths],
            "mount_src":
                tempfile.mkdtemp(prefix=constants.TMP_PATH_SRC_PREFIX),
            "snapshot_name": ("share_snapshot_" +
                              snapshot['id'].replace("-", "_")),
            "id": snapshot['share_id'],
        }

        try:
            self.copy_data_from_parent_share(old_share, new_share)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.delete_share(new_share)
        finally:
            for item in (new_share, old_share):
                try:
                    os.rmdir(item['mount_src'])
                except Exception as err:
                    err_msg = (_('Failed to remove temp file. '
                                 'File path: %(file_path)s. Reason: %(err)s.')
                               % {'file_path': item['mount_src'],
                                  'err': six.text_type(err)})
                    LOG.warning(err_msg)

        return new_share_paths

    def create_share_from_snapshot(self, share, snapshot,
                                   share_server=None):
        """Create a share from snapshot."""
        share_fs_id = self.helper.get_fsid_by_name(snapshot['share_name'])
        if not share_fs_id:
            err_msg = (_("The source filesystem of snapshot %s "
                         "does not exist.")
                       % snapshot['snapshot_id'])
            LOG.error(err_msg)
            raise exception.StorageResourceNotFound(
                name=snapshot['share_name'])
        fs_info = self.helper._get_fs_info_by_id(share_fs_id)
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            msg = _("HyperMetro Pair Share does not support "
                    "create from snapshot")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        snapshot_id = self.helper._get_snapshot_id(share_fs_id, snapshot['id'])
        snapshot_info = self.helper._get_snapshot_by_id(snapshot_id)
        snapshot_flag = self.helper._check_snapshot_id_exist(snapshot_info)
        if not snapshot_flag:
            err_msg = (_("Cannot find snapshot %s on array.")
                       % snapshot['snapshot_id'])
            LOG.error(err_msg)
            raise exception.ShareSnapshotNotFound(
                snapshot_id=snapshot['snapshot_id'])

        self.assert_filesystem(share_fs_id)

        done = True

        if snapshot['snapshot']['share_proto'] == share['share_proto']:
            try:
                locations = self._create_from_snapshot_by_clone(
                    share, share_fs_id, snapshot_id, share_server)
            except Exception:
                LOG.warning('Create share by backend clone failed, '
                            'try host copy.')
                done = False
        else:
            LOG.info('Share protocol is inconsistent, will use host copy.')
            done = False

        if not done:
            locations = self._create_from_snapshot_by_host(
                share, snapshot, share_server)

        self.private_storage.update(share['id'], {'replica_pair_id': ''})

        return locations

    def copy_data_from_parent_share(self, old_share, new_share):
        old_access = self.get_access(old_share)
        old_access_id = self._get_access_id(old_share, old_access)
        if not old_access_id:
            try:
                self.allow_access(old_share, old_access)
            except exception.ManilaException as err:
                with excutils.save_and_reraise_exception():
                    LOG.error('Failed to add access to share %(name)s. '
                              'Reason: %(err)s.',
                              {'name': old_share['name'],
                               'err': six.text_type(err)})

        new_access = self.get_access(new_share)
        try:
            try:
                self.mount_share_to_host(old_share, old_access)
            except exception.ShareMountException as err:
                with excutils.save_and_reraise_exception():
                    LOG.error('Failed to mount old share %(name)s. '
                              'Reason: %(err)s.',
                              {'name': old_share['name'],
                               'err': six.text_type(err)})

            try:
                self.allow_access(new_share, new_access)
                self.mount_share_to_host(new_share, new_access)
            except Exception as err:
                with excutils.save_and_reraise_exception():
                    self.umount_share_from_host(old_share)
                    LOG.error('Failed to mount new share %(name)s. '
                              'Reason: %(err)s.',
                              {'name': new_share['name'],
                               'err': six.text_type(err)})

            copied = self.copy_snapshot_data(old_share, new_share)

            for item in (new_share, old_share):
                try:
                    self.umount_share_from_host(item)
                except exception.ShareUmountException as err:
                    LOG.warning('Failed to unmount share %(name)s. '
                                'Reason: %(err)s.',
                                {'name': item['name'],
                                 'err': six.text_type(err)})

            self.deny_access(new_share, new_access)

            if copied:
                LOG.debug("Created share from snapshot successfully, "
                          "new_share: %s, old_share: %s.",
                          new_share, old_share)
            else:
                message = (_('Failed to copy data from share %(old_share)s '
                             'to share %(new_share)s.')
                           % {'old_share': old_share['name'],
                              'new_share': new_share['name']})
                raise exception.ShareCopyDataException(reason=message)
        finally:
            if not old_access_id:
                self.deny_access(old_share, old_access)

    def get_access(self, share):
        share_proto = share['share_proto']
        access = {}
        root = self.helper._read_xml()

        if share_proto == 'NFS':
            access['access_to'] = root.findtext('Filesystem/NFSClient/IP')
            access['access_level'] = common_constants.ACCESS_LEVEL_RW
            access['access_type'] = 'ip'
        elif share_proto == 'CIFS':
            access['access_to'] = root.findtext(
                'Filesystem/CIFSClient/UserName')
            access['access_password'] = root.findtext(
                'Filesystem/CIFSClient/UserPassword')
            access['access_level'] = common_constants.ACCESS_LEVEL_RW
            access['access_type'] = 'user'

        LOG.debug("Get access for share: %s, access_type: %s, access_to: %s, "
                  "access_level: %s", share['name'], access['access_type'],
                  access['access_to'], access['access_level'])
        return access

    def _get_access_id(self, share, access):
        """Get access id of the share."""
        access_id = None
        share_name = share['name']
        share_proto = share['share_proto']
        share_url_type = self.helper._get_share_url_type(share_proto)
        access_to = access['access_to']
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share = self.helper._get_share_by_name(share_name, share_url_type,
                                               vstore_id)
        access_id = self.helper._get_access_from_share(share['ID'], access_to,
                                                       share_proto)
        if access_id is None:
            LOG.debug('Cannot get access ID from share. '
                      'share_name: %s', share_name)

        return access_id

    def copy_snapshot_data(self, old_share, new_share):
        src_path = '/'.join((old_share['mount_src'], '.snapshot',
                             old_share['snapshot_name']))
        dst_path = new_share['mount_src']
        copy_finish = False
        LOG.debug("Copy data from src_path: %s to dst_path: %s.",
                  src_path, dst_path)
        try:
            ignore_list = ''
            copy = data_utils.Copy(src_path, dst_path, ignore_list)
            copy.run()
            if copy.get_progress()['total_progress'] == 100:
                copy_finish = True
        except Exception as err:
            err_msg = (_("Failed to copy data, reason: %s.")
                       % six.text_type(err))
            LOG.error(err_msg)

        return copy_finish

    def umount_share_from_host(self, share):
        try:
            utils.execute('umount', share['mount_src'],
                          run_as_root=True)
        except Exception as err:
            message = (_("Failed to unmount share %(share)s. "
                         "Reason: %(reason)s.")
                       % {'share': share['name'],
                          'reason': six.text_type(err)})
            raise exception.ShareUmountException(reason=message)

    def mount_share_to_host(self, share, access):
        LOG.debug("Mounting share: %s to host, mount_src: %s",
                  share['name'], share['mount_src'])

        for mount_path in share['mount_paths']:
            try:
                if share['share_proto'] == 'NFS':
                    utils.execute('mount', '-t', 'nfs',
                                  mount_path, share['mount_src'],
                                  run_as_root=True)

                    LOG.debug("Execute mount. mount_src: %s",
                              share['mount_src'])
                elif share['share_proto'] == 'CIFS':
                    user = ('username=' + access['access_to'] + ',' +
                            'password=' + access['access_password'])
                    utils.execute('mount', '-t', 'cifs',
                                  mount_path, share['mount_src'],
                                  '-o', user, run_as_root=True)
            except Exception as err:
                LOG.error('Bad response from mount share %s to %s. '
                          'Reason: %s.',
                          share['name'], mount_path, err)
                continue
            else:
                return

        msg = (_('Mount share %(share)s through %(paths)s failed.')
               % {"share": share['name'], "paths": share['mount_paths']})
        LOG.error(msg)
        raise exception.ShareMountException(reason=msg)

    def get_network_allocations_number(self):
        """Get number of network interfaces to be created."""
        if self.configuration.driver_handles_share_servers:
            return constants.IP_ALLOCATIONS_DHSS_TRUE
        else:
            return constants.IP_ALLOCATIONS_DHSS_FALSE

    def _get_capacity(self, pool_name, result):
        """Get free capacity and total capacity of the pools."""
        poolinfo = self.helper._find_pool_info(pool_name, result)

        if poolinfo:
            total = float(poolinfo['TOTALCAPACITY']) / units.Mi / 2
            free = float(poolinfo['CAPACITY']) / units.Mi / 2
            consumed = float(poolinfo['CONSUMEDCAPACITY']) / units.Mi / 2
            provision = float(poolinfo['PROVISIONEDCAPACITY']) / units.Mi / 2
            poolinfo['TOTALCAPACITY'] = total
            poolinfo['CAPACITY'] = free
            poolinfo['CONSUMEDCAPACITY'] = consumed
            poolinfo['PROVISIONEDCAPACITYGB'] = provision

        return poolinfo

    def _get_disk_type(self, pool_name, result):
        """Get disk type of the pool."""
        pool_info = self.helper._find_pool_info(pool_name, result)
        if not pool_info:
            return None

        pool_disk = []
        for i, x in enumerate(['ssd', 'sas', 'nl_sas']):
            if ('TIER%dCAPACITY' % i in pool_info and
                    pool_info['TIER%dCAPACITY' % i] != '0'):
                pool_disk.append(x)

        if len(pool_disk) > 1:
            pool_disk = ['mix']

        return pool_disk[0] if pool_disk else None

    def _init_filesys_para(self, share, extra_specs, **kwargs):
        """Init basic filesystem parameters."""
        name = share['name']
        fileparam = {
            "NAME": name.replace("-", "_"),
            "ALLOCTYPE": extra_specs['LUNType'],
            "ENABLEDEDUP": extra_specs['dedupe'],
            "ENABLECOMPRESSION": extra_specs['compression'],
        }

        if kwargs.get('poolinfo'):
            fileparam["PARENTID"] = kwargs['poolinfo']['ID']
            fileparam["CAPACITY"] = int(share['size']) * units.Mi * 2
        if kwargs.get('parent_fs_id'):
            fileparam["PARENTFILESYSTEMID"] = kwargs['parent_fs_id']
        if kwargs.get('parent_snap_id'):
            fileparam["PARENTSNAPSHOTID"] = kwargs['parent_snap_id']

        if fileparam['ALLOCTYPE'] == constants.ALLOC_TYPE_THICK_FLAG:
            if (extra_specs['dedupe'] or
                    extra_specs['compression']):
                err_msg = _(
                    'The filesystem type is "Thick",'
                    ' so dedupe or compression cannot be set.')
                LOG.error(err_msg)
                raise exception.InvalidInput(reason=err_msg)
        if extra_specs['sectorsize']:
            fileparam['SECTORSIZE'] = extra_specs['sectorsize'] * units.Ki
        if extra_specs['controllerid']:
            fileparam['OWNINGCONTROLLER'] = extra_specs['controllerid']

        root = self.helper._read_xml()
        snapshot_reserve = root.findtext('Filesystem/SnapshotReserve')
        if snapshot_reserve:
            try:
                snapshot_reserve = int(snapshot_reserve.strip())
            except Exception as err:
                err_msg = _('Config snapshot reserve error. The reason is: '
                            '%s') % err
                LOG.error(err_msg)
                raise exception.InvalidInput(reason=err_msg)

            if 0 <= snapshot_reserve <= 50:
                fileparam['SNAPSHOTRESERVEPER'] = snapshot_reserve
            else:
                err_msg = _("The snapshot reservation percentage can only be "
                            "between 0 and 50%")
                LOG.error(err_msg)
                raise exception.InvalidInput(reason=err_msg)
        else:
            fileparam['SNAPSHOTRESERVEPER'] = 20

        return fileparam

    def rpc_deny_access(self, context, params):
        share_proto = params['share_proto']
        share_name = params['name']
        access_type = params['access_type']
        access_to = params['access_to']
        share_url_type = self.helper._get_share_url_type(share_proto)
        if share_proto == 'NFS' and access_type not in ('ip', 'user'):
            LOG.warning('Only IP or USER access types are allowed for '
                        'NFS shares.')
            return
        elif share_proto == 'CIFS' and access_type != 'user':
            LOG.warning('Only USER access type is allowed for'
                        ' CIFS shares.')
            return

        # Huawei array uses * to represent IP addresses of all clients
        if (share_proto == 'NFS' and access_type == 'ip' and
                access_to == '0.0.0.0/0'):
            access_to = '*'
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)
        if not share:
            LOG.warning('Can not get share %s.', share_name)
            return

        access_id = self.helper._get_access_from_share(
            share['ID'], access_to, share_proto, vstore_id)
        if not access_id:
            LOG.warning('Can not get access id from share. '
                        'share_name: %s', share_name)
            return

        self.helper._remove_access_from_share(
            access_id, share_proto, vstore_id)

    def deny_access(self, share, access, share_server=None):
        """Deny access to share."""
        context = manila_context.get_admin_context()
        params = {"name": share['name'],
                  "share_proto": share['share_proto'],
                  "access_to": access['access_to'],
                  "access_type": access['access_type']}
        fs_info = self.helper._get_fs_info_by_name(share['name'])
        if not fs_info:
            LOG.warning("FS %s is not exist.", share['name'])
            return
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            self.rpc_client.get_remote_fs_info(
                context, share['name'], self.remote_backend)
            if self.check_is_active_client():
                self.rpc_deny_access(context, params)
            else:
                self.rpc_client.deny_access(context, params,
                                            self.remote_backend)
        else:
            self.rpc_deny_access(context, params)

    def rpc_allow_access(self, context, params):
        """Allow access to the share."""
        share_name = params['name']
        share_proto = params['share_proto']
        access_to = params['access_to']
        access_type = params['access_type']
        access_level = params['access_level']
        share_type_id = params['share_type_id']
        share_url_type = self.helper._get_share_url_type(share_proto)
        if access_level not in common_constants.ACCESS_LEVELS:
            raise exception.InvalidShareAccess(
                reason=(_('Unsupported level of access was provided - %s') %
                        access_level))

        if share_proto == 'NFS':
            if access_type == 'user':
                # Use 'user' as 'netgroup' for NFS.
                # A group name starts with @.
                access_to = '@' + access_to
            elif access_type != 'ip':
                message = _('Only IP or USER access types '
                            'are allowed for NFS shares.')
                raise exception.InvalidShareAccess(reason=message)
            if access_level == common_constants.ACCESS_LEVEL_RW:
                access_level = constants.ACCESS_NFS_RW
            else:
                access_level = constants.ACCESS_NFS_RO
            # Huawei array uses * to represent IP addresses of all clients
            if access_to == '0.0.0.0/0':
                access_to = '*'

        elif share_proto == 'CIFS':
            if access_type == 'user':
                if access_level == common_constants.ACCESS_LEVEL_RW:
                    access_level = constants.ACCESS_CIFS_FULLCONTROL
                else:
                    access_level = constants.ACCESS_CIFS_RO
            else:
                message = _('Only USER access type is allowed'
                            ' for CIFS shares.')
                raise exception.InvalidShareAccess(reason=message)
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_stor = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)
        if not share_stor:
            err_msg = (_("Share %s does not exist on the backend.")
                       % share_name)
            LOG.error(err_msg)
            raise exception.ShareResourceNotFound(share_id=share_stor['ID'])

        share_id = share_stor['ID']

        # Check if access already exists
        access_id = self.helper._get_access_from_share(share_id,
                                                       access_to,
                                                       share_proto, vstore_id)
        if access_id:
            # Check if the access level equal
            level_exist = self.helper._get_level_by_access_id(
                access_id, share_proto, vstore_id)
            if level_exist != access_level:
                # Change the access level
                self.helper._change_access_rest(
                    access_id, share_proto, access_level, vstore_id)
        else:
            # Add this access to share
            self.helper._allow_access_rest(
                share_id, access_to, share_proto, access_level,
                share_type_id, vstore_id)

    def allow_access(self, share, access, share_server=None):
        """Allow access to the share."""
        context = manila_context.get_admin_context()
        params = {"name": share['name'],
                  "share_proto": share['share_proto'],
                  "access_to": access['access_to'],
                  "access_type": access['access_type'],
                  "access_level": access['access_level'],
                  "share_type_id": share.get("share_type_id")}
        fs_info = self.helper._get_fs_info_by_name(share['name'])
        if not fs_info:
            msg = _("FS %s to allow access not exist.") % share['name']
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            self.rpc_client.get_remote_fs_info(
                context, share['name'], self.remote_backend)
            if self.check_is_active_client():
                self.rpc_allow_access(context, params)
            else:
                self.rpc_client.allow_access(context, params,
                                             self.remote_backend)
        else:
            self.rpc_allow_access(context, params)

    def clear_access(self, share, share_server=None):
        """Remove all access rules of the share"""
        share_proto = share['share_proto']
        share_name = share['name']
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_url_type = self.helper._get_share_url_type(share_proto)
        share_stor = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)
        if not share_stor:
            LOG.warning('Cannot get share %s.', share_name)
            return
        share_id = share_stor['ID']
        all_accesses = self.helper._get_all_access_from_share(
            share_id, share_proto, vstore_id)
        for access_id in all_accesses:
            self.helper._remove_access_from_share(
                access_id, share_proto, vstore_id)

    def update_access(self, share, access_rules, add_rules,
                      delete_rules, share_server=None):
        """Update access rules list."""
        if not (add_rules or delete_rules):
            self.clear_access(share, share_server)
            for access in access_rules:
                self.allow_access(share, access, share_server)
        else:
            for access in delete_rules:
                self.deny_access(share, access, share_server)
            for access in add_rules:
                self.allow_access(share, access, share_server)

    def get_pool(self, share):
        pool_name = share_utils.extract_host(share['host'], level='pool')
        if pool_name:
            return pool_name
        share_name = share['name']
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_url_type = self.helper._get_share_url_type(share['share_proto'])
        share = self.helper._get_share_by_name(share_name, share_url_type,
                                               vstore_id)

        pool_name = None
        if share:
            pool = self.helper._get_fs_info_by_id(share['FSID'])
            pool_name = pool['POOLNAME']

        return pool_name

    def allocate_container(self, share, **kwargs):
        """Creates filesystem associated to share by name."""
        opts = huawei_utils.get_share_extra_specs_params(
            share['share_type_id'])

        if opts is None:
            opts = constants.OPTS_CAPABILITIES
        smart = smartx.SmartX(self.helper)
        smartx_opts, qos = smart.get_smartx_extra_specs_opts(opts)

        fileParam = self._init_filesys_para(share, smartx_opts, **kwargs)
        remote_vstore_id = None
        if opts.get('hypermetro'):
            vstore_info = self.helper.get_hypermetro_vstore_by_pair_id(
                self.vstore_pair_id)
            local_vstore_id = vstore_info.get('LOCALVSTOREID')
            remote_vstore_id = vstore_info.get('REMOTEVSTOREID')
            if local_vstore_id and remote_vstore_id:
                fileParam['vstoreId'] = local_vstore_id

        fsid = self.helper._create_filesystem(fileParam)

        try:
            if qos:
                smart_qos = smartx.SmartQos(self.helper)
                smart_qos.create_qos(qos, fsid)

            smartpartition = smartx.SmartPartition(self.helper)
            smartpartition.add(opts, fsid)

            smartcache = smartx.SmartCache(self.helper)
            smartcache.add(opts, fsid)
        except Exception as err:
            self._delete_filesystem(fsid)
            message = (_('Failed to add smartx. Reason: %(err)s.')
                       % {'err': err})
            raise exception.InvalidShare(reason=message)

        if opts.get('hypermetro'):
            context = manila_context.get_admin_context()
            try:
                fileParam.update({'vstoreId': remote_vstore_id})
                remote_fs_id = self.rpc_client.create_remote_filesystem(
                    context, self.remote_backend, fileParam)
            except Exception as err:
                self._delete_filesystem(fsid)
                LOG.exception('Failed to create remote filesystem.'
                              ' reason: %s', err)
                raise

            try:
                self.metro_mgr.create_metro_pair(
                    self.metro_domain, fsid, remote_fs_id,
                    self.vstore_pair_id)
            except Exception as err:
                self._delete_filesystem(fsid)
                params = {"ID": remote_fs_id, 'vstoreId': remote_vstore_id}
                self.rpc_client.delete_remote_filesystem(
                    context, self.remote_backend, params)
                LOG.exception('Failed to create HyperMetro filesystem pair'
                              '%(fs_id)s. reason: %(err)s'
                              % {"fs_id": fsid, "err": err})
                raise
        return fsid

    def manage_existing(self, share, driver_options):
        """Manage existing share."""

        share_proto = share['share_proto']
        share_name = share['name']
        old_export_location = share['export_locations'][0]['path']
        pool_name = share_utils.extract_host(share['host'], level='pool')
        share_url_type = self.helper._get_share_url_type(share_proto)
        old_share_name = self.helper._get_share_name_by_export_location(
            old_export_location, share_proto)

        replica_pair_info = self.helper.get_replication_pair_by_localres_name(
            old_share_name)
        if replica_pair_info:
            err_msg = (_("Cannot manage share %s because it's already "
                         "associated to replication pair at backend.")
                       % old_export_location)
            LOG.error(err_msg)
            raise exception.ManageInvalidShare(reason=err_msg)
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_storage = self.helper._get_share_by_name(old_share_name,
                                                       share_url_type,
                                                       vstore_id)
        if not share_storage:
            err_msg = (_("Can not get share ID by share %s.")
                       % old_export_location)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        fs_id = share_storage['FSID']
        fs = self.helper._get_fs_info_by_id(fs_id)
        if not self.check_fs_status(fs['HEALTHSTATUS'],
                                    fs['RUNNINGSTATUS']):
            raise exception.InvalidShare(
                reason=(_('Invalid status of filesystem: '
                          'HEALTHSTATUS=%(health)s '
                          'RUNNINGSTATUS=%(running)s.')
                        % {'health': fs['HEALTHSTATUS'],
                           'running': fs['RUNNINGSTATUS']}))

        if pool_name and pool_name != fs['POOLNAME']:
            raise exception.InvalidHost(
                reason=(_('The current pool(%(fs_pool)s) of filesystem '
                          'does not match the input pool(%(host_pool)s).')
                        % {'fs_pool': fs['POOLNAME'],
                           'host_pool': pool_name}))

        result = self.helper._find_all_pool_info()
        poolinfo = self.helper._find_pool_info(pool_name, result)

        opts = huawei_utils.get_share_extra_specs_params(
            share['share_type_id'])
        specs = share_types.get_share_type_extra_specs(share['share_type_id'])
        if ('capabilities:thin_provisioning' not in specs.keys()
                and 'thin_provisioning' not in specs.keys()):
            if fs['ALLOCTYPE'] == constants.ALLOC_TYPE_THIN_FLAG:
                opts['thin_provisioning'] = constants.THIN_PROVISIONING
            else:
                opts['thin_provisioning'] = constants.THICK_PROVISIONING

        change_opts = self.check_retype_change_opts(opts, poolinfo, fs)
        LOG.info('Retyping share (%(share)s), changed options are : '
                 '(%(change_opts)s).',
                 {'share': old_share_name, 'change_opts': change_opts})
        try:
            self.retype_share(change_opts, fs_id)
        except Exception as err:
            message = (_("Retype share error. Share: %(share)s. "
                         "Reason: %(reason)s.")
                       % {'share': old_share_name,
                          'reason': err})
            raise exception.InvalidShare(reason=message)

        self.private_storage.update(share['id'], {'replica_pair_id': ''})

        share_size = int(fs['CAPACITY']) / units.Mi / 2
        self.helper._change_fs_name(fs_id, share_name)

        locations = self._get_location_path(share_name, share_proto)
        return share_size, locations

    def unmanage(self, share):
        self.private_storage.delete(share['id'])

    def _check_snapshot_valid_for_manage(self, snapshot_info):
        snapshot_name = snapshot_info['data']['NAME']

        # Check whether the snapshot is normal.
        if (snapshot_info['data']['HEALTHSTATUS']
                != constants.STATUS_FSSNAPSHOT_HEALTH):
            msg = (_("Can't import snapshot %(snapshot)s to Manila. "
                     "Snapshot status is not normal, snapshot status: "
                     "%(status)s.")
                   % {'snapshot': snapshot_name,
                      'status': snapshot_info['data']['HEALTHSTATUS']})
            raise exception.ManageInvalidShareSnapshot(
                reason=msg)

    def manage_existing_snapshot(self, snapshot, driver_options):
        """Manage existing snapshot."""

        share_proto = snapshot['share']['share_proto']
        share_url_type = self.helper._get_share_url_type(share_proto)
        vstore_id = self.get_vstore_id_by_fs_info(snapshot['share_name'])
        share_storage = self.helper._get_share_by_name(snapshot['share_name'],
                                                       share_url_type,
                                                       vstore_id)
        if not share_storage:
            err_msg = (_("Failed to import snapshot %(snapshot)s to Manila. "
                         "Snapshot source share %(share)s doesn't exist "
                         "on array.")
                       % {'snapshot': snapshot['provider_location'],
                          'share': snapshot['share_name']})
            raise exception.InvalidShare(reason=err_msg)
        sharefsid = share_storage['FSID']

        provider_location = snapshot.get('provider_location')
        snapshot_id = sharefsid + "@" + provider_location
        snapshot_info = self.helper._get_snapshot_by_id(snapshot_id)
        snapshot_flag = self.helper._check_snapshot_id_exist(snapshot_info)
        if not snapshot_flag:
            err_msg = (_("Cannot find snapshot %s on array.")
                       % snapshot['provider_location'])
            raise exception.ManageInvalidShareSnapshot(reason=err_msg)
        else:
            self._check_snapshot_valid_for_manage(snapshot_info)
            snapshot_name = ("share_snapshot_"
                             + snapshot['id'].replace("-", "_"))
            self.helper._rename_share_snapshot(snapshot_id, snapshot_name)
        return snapshot_name

    def check_retype_change_opts(self, opts, poolinfo, fs):
        change_opts = {
            "partitionid": None,
            "cacheid": None,
            "dedupe&compression": None,
        }

        # SmartPartition
        old_partition_id = fs['SMARTPARTITIONID']
        old_partition_name = None
        new_partition_id = None
        new_partition_name = None
        if strutils.bool_from_string(opts['huawei_smartpartition']):
            if not opts['partitionname']:
                raise exception.InvalidInput(
                    reason=_('Partition name is None, please set '
                             'huawei_smartpartition:partitionname in key.'))
            new_partition_name = opts['partitionname']
            new_partition_id = self.helper._get_partition_id_by_name(
                new_partition_name)
            if new_partition_id is None:
                raise exception.InvalidInput(
                    reason=(_("Can't find partition name on the array, "
                              "partition name is: %(name)s.")
                            % {"name": new_partition_name}))

        if old_partition_id != new_partition_id:
            if old_partition_id:
                partition_info = self.helper.get_partition_info_by_id(
                    old_partition_id)
                old_partition_name = partition_info['NAME']
            change_opts["partitionid"] = ([old_partition_id,
                                           old_partition_name],
                                          [new_partition_id,
                                           new_partition_name])

        # SmartCache
        old_cache_id = fs['SMARTCACHEID']
        old_cache_name = None
        new_cache_id = None
        new_cache_name = None
        if strutils.bool_from_string(opts['huawei_smartcache']):
            if not opts['cachename']:
                raise exception.InvalidInput(
                    reason=_('Cache name is None, please set '
                             'huawei_smartcache:cachename in key.'))
            new_cache_name = opts['cachename']
            new_cache_id = self.helper._get_cache_id_by_name(
                new_cache_name)
            if new_cache_id is None:
                raise exception.InvalidInput(
                    reason=(_("Can't find cache name on the array, "
                              "cache name is: %(name)s.")
                            % {"name": new_cache_name}))

        if old_cache_id != new_cache_id:
            if old_cache_id:
                cache_info = self.helper.get_cache_info_by_id(
                    old_cache_id)
                old_cache_name = cache_info['NAME']
            change_opts["cacheid"] = ([old_cache_id, old_cache_name],
                                      [new_cache_id, new_cache_name])

        # SmartDedupe&SmartCompression
        smartx_opts = constants.OPTS_CAPABILITIES
        if opts is not None:
            smart = smartx.SmartX(self.helper)
            smartx_opts, qos = smart.get_smartx_extra_specs_opts(opts)

        old_compression = fs['COMPRESSION']
        new_compression = smartx_opts['compression']
        old_dedupe = fs['DEDUP']
        new_dedupe = smartx_opts['dedupe']

        if fs['ALLOCTYPE'] == constants.ALLOC_TYPE_THIN_FLAG:
            fs['ALLOCTYPE'] = constants.ALLOC_TYPE_THIN
        else:
            fs['ALLOCTYPE'] = constants.ALLOC_TYPE_THICK

        if strutils.bool_from_string(opts['thin_provisioning']):
            opts['thin_provisioning'] = constants.ALLOC_TYPE_THIN
        else:
            opts['thin_provisioning'] = constants.ALLOC_TYPE_THICK

        if fs['ALLOCTYPE'] != opts['thin_provisioning']:
            msg = (_("Manage existing share "
                     "fs type and new_share_type mismatch. "
                     "fs type is: %(fs_type)s, "
                     "new_share_type is: %(new_share_type)s")
                   % {"fs_type": fs['ALLOCTYPE'],
                      "new_share_type": opts['thin_provisioning']})
            raise exception.InvalidHost(reason=msg)
        else:
            if fs['ALLOCTYPE'] == constants.ALLOC_TYPE_THICK:
                if new_compression or new_dedupe:
                    raise exception.InvalidInput(
                        reason=_("Dedupe or compression cannot be set for "
                                 "thick filesystem."))
            else:
                if (old_dedupe != new_dedupe
                        or old_compression != new_compression):
                    change_opts["dedupe&compression"] = ([old_dedupe,
                                                          old_compression],
                                                         [new_dedupe,
                                                          new_compression])
        return change_opts

    def retype_share(self, change_opts, fs_id):
        if change_opts.get('partitionid'):
            old, new = change_opts['partitionid']
            old_id = old[0]
            old_name = old[1]
            new_id = new[0]
            new_name = new[1]

            if old_id:
                self.helper._remove_fs_from_partition(fs_id, old_id)
            if new_id:
                self.helper._add_fs_to_partition(fs_id, new_id)
                msg = (_("Retype FS(id: %(fs_id)s) smartpartition from "
                         "(name: %(old_name)s, id: %(old_id)s) to "
                         "(name: %(new_name)s, id: %(new_id)s) "
                         "performed successfully.")
                       % {"fs_id": fs_id,
                          "old_id": old_id, "old_name": old_name,
                          "new_id": new_id, "new_name": new_name})
                LOG.info(msg)

        if change_opts.get('cacheid'):
            old, new = change_opts['cacheid']
            old_id = old[0]
            old_name = old[1]
            new_id = new[0]
            new_name = new[1]
            if old_id:
                self.helper._remove_fs_from_cache(fs_id, old_id)
            if new_id:
                self.helper._add_fs_to_cache(fs_id, new_id)
                msg = (_("Retype FS(id: %(fs_id)s) smartcache from "
                         "(name: %(old_name)s, id: %(old_id)s) to "
                         "(name: %(new_name)s, id: %(new_id)s) "
                         "performed successfully.")
                       % {"fs_id": fs_id,
                          "old_id": old_id, "old_name": old_name,
                          "new_id": new_id, "new_name": new_name})
                LOG.info(msg)

        if change_opts.get('dedupe&compression'):
            old, new = change_opts['dedupe&compression']
            old_dedupe = old[0]
            old_compression = old[1]
            new_dedupe = new[0]
            new_compression = new[1]
            if ((old_dedupe != new_dedupe)
                    or (old_compression != new_compression)):

                new_smartx_opts = {"dedupe": new_dedupe,
                                   "compression": new_compression}

                self.helper._change_extra_specs(fs_id, new_smartx_opts)
                msg = (_("Retype FS(id: %(fs_id)s) dedupe from %(old_dedupe)s "
                         "to %(new_dedupe)s performed successfully, "
                         "compression from "
                         "%(old_compression)s to %(new_compression)s "
                         "performed successfully.")
                       % {"fs_id": fs_id,
                          "old_dedupe": old_dedupe,
                          "new_dedupe": new_dedupe,
                          "old_compression": old_compression,
                          "new_compression": new_compression})
                LOG.info(msg)

    def remove_qos_fs(self, fs_id, qos_id):
        fs_list = self.helper.get_fs_list_in_qos(qos_id)
        fs_count = len(fs_list)
        if fs_count <= 1:
            qos = smartx.SmartQos(self.helper)
            qos.delete_qos(qos_id)
        else:
            self.helper.remove_fs_from_qos(fs_id,
                                           fs_list,
                                           qos_id)

    def _get_location_path(self, share_name, share_proto, ips=None):
        if not ips:
            ips = huawei_utils.get_logical_ips(self.helper)

        path = share_name.replace("-", "_")
        if share_proto == 'NFS':
            locations = ['%s:/%s' % (ip, path) for ip in ips]
        elif share_proto == 'CIFS':
            locations = ['\\\\%s\\%s' % (ip, path) for ip in ips]
        else:
            raise exception.InvalidShareAccess(
                reason=_('Invalid NAS protocol supplied: %s.') % share_proto)

        return locations

    def _get_share_proto(self, share_name):
        share_proto = None
        for proto in ('NFS', 'CIFS'):
            vstore_id = self.get_vstore_id_by_fs_info(share_name)
            share_url_type = self.helper._get_share_url_type(proto)
            share = self.helper._get_share_by_name(share_name,
                                                   share_url_type, vstore_id)
            if share:
                share_proto = proto
                break
        return share_proto

    def _get_wait_interval(self):
        """Get wait interval from huawei conf file."""
        root = self.helper._read_xml()
        wait_interval = root.findtext('Filesystem/WaitInterval')
        if wait_interval:
            return int(wait_interval)
        else:
            LOG.info(
                "Wait interval is not configured in huawei "
                "conf file. Use default: %(default_wait_interval)d.",
                {"default_wait_interval": constants.DEFAULT_WAIT_INTERVAL})
            return constants.DEFAULT_WAIT_INTERVAL

    def _get_timeout(self):
        """Get timeout from huawei conf file."""
        root = self.helper._read_xml()
        timeout = root.findtext('Filesystem/Timeout')
        if timeout:
            return int(timeout)
        else:
            LOG.info(
                "Timeout is not configured in huawei conf file. "
                "Use default: %(default_timeout)d.",
                {"default_timeout": constants.DEFAULT_TIMEOUT})
            return constants.DEFAULT_TIMEOUT

    def check_conf_file(self):
        """Check the config file, make sure the essential items are set."""
        root = self.helper._read_xml()
        resturl = root.findtext('Storage/RestURL')
        username = root.findtext('Storage/UserName')
        pwd = root.findtext('Storage/UserPassword')
        pool_node = root.findtext('Filesystem/StoragePool')
        logical_port_ip = root.findtext('Storage/LogicalPortIP')

        if not (resturl and username and pwd):
            err_msg = (_(
                'check_conf_file: Config file invalid. RestURL,'
                ' UserName and UserPassword must be set.'))
            LOG.error(err_msg)
            raise exception.InvalidInput(err_msg)

        if not pool_node:
            err_msg = (_(
                'check_conf_file: Config file invalid. '
                'StoragePool must be set.'))
            LOG.error(err_msg)
            raise exception.InvalidInput(err_msg)

        logical_ips = []
        if logical_port_ip:
            logical_ips = [i.strip() for i in logical_port_ip.split(';')
                           if i.strip()]
        if not (self.configuration.driver_handles_share_servers
                or logical_ips):
            err_msg = (_(
                'check_conf_file: Config file invalid. LogicalPortIP '
                'must be set when driver_handles_share_servers is False.'))
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        if self.snapshot_support and self.replication_support:
            err_msg = _('Config file invalid. SnapshotSupport and '
                        'ReplicationSupport can not both be set to True.')
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)

        self.get_metro_info()

    def _get_metro_info(self):
        metro_infos = self.configuration.safe_get('metro_info')
        if not metro_infos:
            return []
        metro_configs = []
        for metro_info in metro_infos:
            metro_config = {}
            metro_config['metro_domain'] = metro_info['metro_domain']
            metro_config['local_vStore_name'] = metro_info['local_vStore_name']
            metro_config['remote_vStore_name'] = \
                metro_info['remote_vStore_name']
            metro_config['remote_backend'] = metro_info['remote_backend']
            metro_config['metro_logic_ip'] = metro_info['metro_logic_ip']
            metro_configs.append(metro_config)

        return metro_configs

    def get_metro_info(self):
        metro_infos = self._get_metro_info()
        metro_info = metro_infos[0] if metro_infos else {}
        if metro_info:
            self.metro_domain = metro_info.get("metro_domain")
            self.remote_backend = metro_info.get("remote_backend")
            local_vstore_name = metro_info.get("local_vStore_name")
            remote_vstore_name = metro_info.get("remote_vStore_name")
            self.metro_logic_ip = metro_info.get('metro_logic_ip')

            self.vstore_pair_id = huawei_utils.get_hypermetro_vstore_id(
                self.helper, self.metro_domain, local_vstore_name,
                remote_vstore_name)
            # check_remote_metro_info
            context = manila_context.get_admin_context()
            self.rpc_client.check_remote_metro_info(
                context, self.remote_backend, self.metro_domain,
                local_vstore_name, remote_vstore_name,
                self.vstore_pair_id)

    def setup_server(self, network_info, metadata=None):
        """Set up share server with given network parameters."""
        self._check_network_type_validate(network_info['network_type'])

        vlan_tag = network_info['segmentation_id'] or 0
        ip = network_info['network_allocations'][0]['ip_address']
        subnet = utils.cidr_to_netmask(network_info['cidr'])
        if not utils.is_valid_ip_address(ip, '4'):
            err_msg = (_(
                "IP (%s) is invalid. Only IPv4 addresses are supported.") % ip)
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        ad_created = False
        ldap_created = False
        try:
            if network_info.get('security_services'):
                active_directory, ldap = self._get_valid_security_service(
                    network_info.get('security_services'))

                # Configure AD or LDAP Domain.
                if active_directory:
                    self._configure_AD_domain(active_directory)
                    ad_created = True
                if ldap:
                    self._configure_LDAP_domain(ldap)
                    ldap_created = True

            # Create vlan and logical_port.
            vlan_id, logical_port_id = (
                self._create_vlan_and_logical_port(vlan_tag, ip, subnet))
        except exception.ManilaException:
            if ad_created:
                dns_ip_list = []
                user = active_directory['user']
                password = active_directory['password']
                self.helper.set_DNS_ip_address(dns_ip_list)
                self.helper.delete_AD_config(user, password)
                self._check_AD_expected_status(constants.STATUS_EXIT_DOMAIN)
            if ldap_created:
                self.helper.delete_LDAP_config()
            raise

        return {
            'share_server_name': network_info['server_id'],
            'share_server_id': network_info['server_id'],
            'vlan_id': vlan_id,
            'logical_port_id': logical_port_id,
            'ip': ip,
            'subnet': subnet,
            'vlan_tag': vlan_tag,
            'ad_created': ad_created,
            'ldap_created': ldap_created,
        }

    def _check_network_type_validate(self, network_type):
        if network_type not in ('flat', 'vlan', 'vxlan', None):
            err_msg = (_(
                'Invalid network type. Network type must be flat or vlan.'))
            raise exception.NetworkBadConfigurationException(reason=err_msg)

    def _get_valid_security_service(self, security_services):
        """Validate security services and return AD/LDAP config."""
        service_number = len(security_services)
        err_msg = _("Unsupported security services. "
                    "Only AD and LDAP are supported.")
        if service_number > 2:
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        active_directory = None
        ldap = None
        for ss in security_services:
            if ss['type'] == 'active_directory':
                active_directory = ss
            elif ss['type'] == 'ldap':
                ldap = ss
            else:
                LOG.error(err_msg)
                raise exception.InvalidInput(reason=err_msg)

        return active_directory, ldap

    def _configure_AD_domain(self, active_directory):
        dns_ip = active_directory['dns_ip']
        user = active_directory['user']
        password = active_directory['password']
        domain = active_directory['domain']
        if not (dns_ip and user and password and domain):
            raise exception.InvalidInput(
                reason=_("dns_ip or user or password or domain "
                         "in security_services is None."))

        # Check DNS server exists or not.
        ip_address = self.helper.get_DNS_ip_address()
        if ip_address and ip_address[0]:
            err_msg = (_("DNS server (%s) has already been configured.")
                       % ip_address[0])
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        # Check AD config exists or not.
        ad_exists, AD_domain = self.helper.get_AD_domain_name()
        if ad_exists:
            err_msg = (_("AD domain (%s) has already been configured.")
                       % AD_domain)
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        # Set DNS server ip.
        dns_ip_list = dns_ip.split(",")
        DNS_config = self.helper.set_DNS_ip_address(dns_ip_list)

        # Set AD config.
        digits = string.digits
        random_id = ''.join([random.choice(digits) for i in range(9)])
        system_name = constants.SYSTEM_NAME_PREFIX + random_id

        try:
            self.helper.add_AD_config(user, password, domain, system_name)
            self._check_AD_expected_status(constants.STATUS_JOIN_DOMAIN)
        except exception.ManilaException as err:
            if DNS_config:
                dns_ip_list = []
                self.helper.set_DNS_ip_address(dns_ip_list)
            raise exception.InvalidShare(
                reason=(_('Failed to add AD config. '
                          'Reason: %s.') % err))

    def _check_AD_expected_status(self, expected_status):
        wait_interval = self._get_wait_interval()
        timeout = self._get_timeout()
        retries = timeout / wait_interval
        interval = wait_interval
        backoff_rate = 1

        @utils.retry(exception.InvalidShare,
                     interval,
                     retries,
                     backoff_rate)
        def _check_AD_status():
            ad = self.helper.get_AD_config()
            if ad['DOMAINSTATUS'] != expected_status:
                raise exception.InvalidShare(
                    reason=(_('AD domain (%s) status is not expected.')
                            % ad['FULLDOMAINNAME']))

        _check_AD_status()

    def _configure_LDAP_domain(self, ldap):
        server = ldap['server']
        domain = ldap['domain']
        if not server or not domain:
            raise exception.InvalidInput(reason=_("Server or domain is None."))

        # Check LDAP config exists or not.
        ldap_exists, LDAP_domain = self.helper.get_LDAP_domain_server()
        if ldap_exists:
            err_msg = (_("LDAP domain (%s) has already been configured.")
                       % LDAP_domain)
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        # Set LDAP config.
        server_number = len(server.split(','))
        if server_number == 1:
            server = server + ",,"
        elif server_number == 2:
            server = server + ","
        elif server_number > 3:
            raise exception.InvalidInput(
                reason=_("Cannot support more than three LDAP servers."))

        self.helper.add_LDAP_config(server, domain)

    def _create_vlan_and_logical_port(self, vlan_tag, ip, subnet):
        optimal_port, port_type = self._get_optimal_port(vlan_tag)
        port_id = self.helper.get_port_id(optimal_port, port_type)
        home_port_id = port_id
        home_port_type = port_type
        vlan_id = 0
        vlan_exists = True

        if port_type is None or port_id is None:
            err_msg = _("No appropriate port found to create logical port.")
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)
        if vlan_tag:
            vlan_exists, vlan_id = self.helper.get_vlan(port_id, vlan_tag)
            if not vlan_exists:
                # Create vlan.
                vlan_id = self.helper.create_vlan(
                    port_id, port_type, vlan_tag)
            home_port_id = vlan_id
            home_port_type = constants.PORT_TYPE_VLAN

        logical_port_exists, logical_port_id = (
            self.helper.get_logical_port(home_port_id, ip, subnet))
        if not logical_port_exists:
            try:
                # Create logical port.
                logical_port_id = (
                    self.helper.create_logical_port(
                        home_port_id, home_port_type, ip, subnet))
            except exception.ManilaException as err:
                if not vlan_exists:
                    self.helper.delete_vlan(vlan_id)
                raise exception.InvalidShare(
                    reason=(_('Failed to create logical port. '
                              'Reason: %s.') % err))

        return vlan_id, logical_port_id

    def _get_optimal_port(self, vlan_tag):
        """Get an optimal physical port or bond port."""
        root = self.helper._read_xml()
        port_info = []
        port_list = root.findtext('Storage/Port')
        if port_list:
            port_list = port_list.split(";")
            for port in port_list:
                port = port.strip().strip('\n')
                if port:
                    port_info.append(port)

        eth_port, bond_port = self._get_online_port(port_info)
        if vlan_tag:
            optimal_port, port_type = (
                self._get_least_port(eth_port, bond_port,
                                     sort_type=constants.SORT_BY_VLAN))
        else:
            optimal_port, port_type = (
                self._get_least_port(eth_port, bond_port,
                                     sort_type=constants.SORT_BY_LOGICAL))

        if not optimal_port:
            err_msg = (_("Cannot find optimal port. port_info: %s.")
                       % port_info)
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        return optimal_port, port_type

    def _get_online_port(self, all_port_list):
        eth_port = self.helper.get_all_eth_port()
        bond_port = self.helper.get_all_bond_port()

        eth_status = constants.STATUS_ETH_RUNNING
        online_eth_port = []
        for eth in eth_port:
            if (eth_status == eth['RUNNINGSTATUS']
                    and not eth['IPV4ADDR'] and not eth['BONDNAME']):
                online_eth_port.append(eth['LOCATION'])

        online_bond_port = []
        for bond in bond_port:
            if eth_status == bond['RUNNINGSTATUS']:
                port_id = jsonutils.loads(bond['PORTIDLIST'])
                bond_eth_port = self.helper.get_eth_port_by_id(port_id[0])
                if bond_eth_port and not bond_eth_port['IPV4ADDR']:
                    online_bond_port.append(bond['NAME'])

        filtered_eth_port = []
        filtered_bond_port = []
        if len(all_port_list) == 0:
            filtered_eth_port = online_eth_port
            filtered_bond_port = online_bond_port
        else:
            all_port_list = list(set(all_port_list))
            for port in all_port_list:
                is_eth_port = False
                for eth in online_eth_port:
                    if port == eth:
                        filtered_eth_port.append(port)
                        is_eth_port = True
                        break
                if is_eth_port:
                    continue
                for bond in online_bond_port:
                    if port == bond:
                        filtered_bond_port.append(port)
                        break

        return filtered_eth_port, filtered_bond_port

    def _get_least_port(self, eth_port, bond_port, sort_type):
        sorted_eth = []
        sorted_bond = []

        if sort_type == constants.SORT_BY_VLAN:
            _get_sorted_least_port = self._get_sorted_least_port_by_vlan
        else:
            _get_sorted_least_port = self._get_sorted_least_port_by_logical

        if eth_port:
            sorted_eth = _get_sorted_least_port(eth_port)
        if bond_port:
            sorted_bond = _get_sorted_least_port(bond_port)

        if sorted_eth and sorted_bond:
            if sorted_eth[1] >= sorted_bond[1]:
                return sorted_bond[0], constants.PORT_TYPE_BOND
            else:
                return sorted_eth[0], constants.PORT_TYPE_ETH
        elif sorted_eth:
            return sorted_eth[0], constants.PORT_TYPE_ETH
        elif sorted_bond:
            return sorted_bond[0], constants.PORT_TYPE_BOND
        else:
            return None, None

    def _get_sorted_least_port_by_vlan(self, port_list):
        if not port_list:
            return None

        vlan_list = self.helper.get_all_vlan()
        count = {}
        for item in port_list:
            count[item] = 0

        for item in port_list:
            for vlan in vlan_list:
                pos = vlan['NAME'].rfind('.')
                if vlan['NAME'][:pos] == item:
                    count[item] += 1

        sort_port = sorted(count.items(), key=lambda count: count[1])

        return sort_port[0]

    def _get_sorted_least_port_by_logical(self, port_list):
        if not port_list:
            return None

        logical_list = self.helper.get_all_logical_port()
        count = {}
        for item in port_list:
            count[item] = 0
            for logical in logical_list:
                if logical['HOMEPORTTYPE'] == constants.PORT_TYPE_VLAN:
                    pos = logical['HOMEPORTNAME'].rfind('.')
                    if logical['HOMEPORTNAME'][:pos] == item:
                        count[item] += 1
                else:
                    if logical['HOMEPORTNAME'] == item:
                        count[item] += 1

        sort_port = sorted(count.items(), key=lambda count: count[1])

        return sort_port[0]

    def teardown_server(self, server_details, security_services=None):
        if not server_details:
            LOG.debug('Server details are empty.')
            return

        logical_port_id = server_details.get('logical_port_id')
        vlan_id = server_details.get('vlan_id')
        ad_created = server_details.get('ad_created')
        ldap_created = server_details.get('ldap_created')

        # Delete logical_port.
        if logical_port_id:
            logical_port_exists = (
                self.helper.check_logical_port_exists_by_id(logical_port_id))
            if logical_port_exists:
                self.helper.delete_logical_port(logical_port_id)

        # Delete vlan.
        if vlan_id and vlan_id != '0':
            vlan_exists = self.helper.check_vlan_exists_by_id(vlan_id)
            if vlan_exists:
                self.helper.delete_vlan(vlan_id)

        if security_services:
            active_directory, ldap = (
                self._get_valid_security_service(security_services))

            if ad_created and ad_created == '1' and active_directory:
                dns_ip = active_directory['dns_ip']
                user = active_directory['user']
                password = active_directory['password']
                domain = active_directory['domain']

                # Check DNS server exists or not.
                ip_address = self.helper.get_DNS_ip_address()
                if ip_address and ip_address[0] == dns_ip:
                    dns_ip_list = []
                    self.helper.set_DNS_ip_address(dns_ip_list)

                # Check AD config exists or not.
                ad_exists, AD_domain = self.helper.get_AD_domain_name()
                if ad_exists and AD_domain == domain:
                    self.helper.delete_AD_config(user, password)
                    self._check_AD_expected_status(
                        constants.STATUS_EXIT_DOMAIN)

            if ldap_created and ldap_created == '1' and ldap:
                server = ldap['server']
                domain = ldap['domain']

                # Check LDAP config exists or not.
                ldap_exists, LDAP_domain = (
                    self.helper.get_LDAP_domain_server())
                if ldap_exists:
                    LDAP_config = self.helper.get_LDAP_config()
                    if (LDAP_config['LDAPSERVER'] == server
                            and LDAP_config['BASEDN'] == domain):
                        self.helper.delete_LDAP_config()

    def ensure_share(self, share, share_server=None):
        """Ensure that share is exported."""
        share_proto = share['share_proto']
        share_name = share['name']
        share_id = share['id']
        vstore_id = self.get_vstore_id_by_fs_info(share_name)
        share_url_type = self.helper._get_share_url_type(share_proto)
        fs_info = self.helper._get_fs_info_by_name(share_name)
        if not fs_info:
            msg = _("FS %s is not exist.") % share_name
            LOG.error(msg)
            raise exception.ShareResourceNotFound(msg=msg)

        share_storage = self.helper._get_share_by_name(
            share_name, share_url_type, vstore_id)
        if not share_storage:
            raise exception.ShareResourceNotFound(share_id=share_id)

        fs_id = share_storage['FSID']
        self.assert_filesystem(fs_id)

        ips = self._get_share_ip(share_server, fs_info, vstore_id)
        locations = self._get_location_path(share_name, share_proto, ips)
        return locations

    def create_replica(self, context, replica_list, new_replica,
                       access_rules, replica_snapshots, share_server=None):
        """Create a new share, and create a remote replication pair."""

        active_replica = share_utils.get_active_replica(replica_list)

        def _check_replica_exists():
            pair_id = self.private_storage.get(active_replica['id'],
                                               'replica_pair_id')
            return True if pair_id else False

        if _check_replica_exists():
            # for huawei array, only one replication can be created for each
            # active replica, so if the active replica already has a replica,
            # can not create anymore.
            msg = _('Cannot create more than one replica for share %s.')
            LOG.error(msg, active_replica['share_id'])
            raise exception.ReplicationException(
                reason=msg % active_replica['share_id'])

        # Create a new share
        new_share_name = new_replica['name']
        locations = self.create_share(new_replica, share_server)

        # create a replication pair.
        # replication pair only can be created by master node,
        # so here is a remote call to trigger master node to
        # start the creating progress.
        try:
            active_replica_info = {
                'name': active_replica['name'],
            }

            (local_pair_id, remote_pair_id
             ) = self.rpc_client.create_replica_pair(
                context,
                active_replica['host'],
                local_share_info=active_replica_info,
                remote_device_wwn=self.helper.get_array_wwn(),
                remote_fs_id=self.helper.get_fsid_by_name(new_share_name),
                local_replication=self.configuration.local_replication,
            )
        except Exception:
            LOG.exception('Failed to create a replication pair '
                          'with host %s.', active_replica['host'])
            raise

        self.private_storage.update(
            active_replica['id'], {'replica_pair_id': local_pair_id})
        self.private_storage.update(
            new_replica['id'], {'replica_pair_id': remote_pair_id})

        # Get the state of the new created replica
        replica_state = self.replica_mgr.get_replica_state(local_pair_id)
        return {'export_locations': locations,
                'replica_state': replica_state}

    def update_replica_state(self, context, replica_list, replica,
                             access_rules, replica_snapshots,
                             share_server=None):
        replica_pair_id = self.private_storage.get(replica['id'],
                                                   'replica_pair_id')
        if not replica_pair_id:
            msg = "No replication pair ID recorded for share %s."
            LOG.error(msg, replica['share_id'])
            return common_constants.STATUS_ERROR

        self.replica_mgr.update_replication_pair_state(replica_pair_id)
        return self.replica_mgr.get_replica_state(replica_pair_id)

    def promote_replica(self, context, replica_list, replica, access_rules,
                        share_server=None):
        replica_pair_id = self.private_storage.get(replica['id'],
                                                   'replica_pair_id')
        if not replica_pair_id:
            msg = _("No replication pair ID recorded for share %s.")
            LOG.error(msg, replica['share_id'])
            raise exception.ReplicationException(
                reason=msg % replica['share_id'])

        try:
            self.replica_mgr.switch_over(replica_pair_id)
        except Exception:
            LOG.exception('Failed to promote replica %s.',
                          replica['id'])
            raise

        new_access_status = common_constants.STATUS_ACTIVE
        try:
            self.update_access(replica, access_rules, [], [], share_server)
        except Exception:
            LOG.warning('Failed to set access rules to '
                        'new active replica %s.',
                        replica['id'])
            new_access_status = common_constants.SHARE_INSTANCE_RULES_ERROR

        old_active_replica = share_utils.get_active_replica(replica_list)

        try:
            self.rpc_client.clear_share_access(context,
                                               old_active_replica['host'],
                                               old_active_replica)
        except Exception:
            LOG.warning("Failed to clear access rules from "
                        "old active replica %s.",
                        old_active_replica['id'])

        new_active_update = {
            'id': replica['id'],
            'replica_state': common_constants.REPLICA_STATE_ACTIVE,
            'access_rules_status': new_access_status,
        }

        # get replica state for new secondary after switch over
        replica_state = self.replica_mgr.get_replica_state(replica_pair_id)
        old_active_update = {
            'id': old_active_replica['id'],
            'replica_state': replica_state,
        }

        return [old_active_update, new_active_update]

    def delete_replica(self, context, replica_list, replica_snapshots,
                       replica, share_server=None):
        replica_pair_id = self.private_storage.get(replica['id'],
                                                   'replica_pair_id')
        if not replica_pair_id:
            msg = ("No replication pair ID recorded for share %(share)s. "
                   "Continue to delete replica %(replica)s.")
            LOG.warning(msg, {'share': replica['share_id'],
                              'replica': replica['id']})
        else:
            self.replica_mgr.delete_replication_pair(replica_pair_id)

            active_replica = share_utils.get_active_replica(replica_list)
            self.private_storage.update(active_replica['id'],
                                        {'replica_pair_id': ''})
            self.private_storage.update(replica['id'],
                                        {'replica_pair_id': ''})

        try:
            self.delete_share(replica, share_server)
        except Exception:
            LOG.exception('Failed to delete replica %s.',
                          replica['id'])
            raise

    def revert_to_snapshot(self, context, snapshot, share_access_rules,
                           snapshot_access_rules, share_server=None):
        fs_id = self.helper.get_fsid_by_name(snapshot['share_name'])
        if not fs_id:
            msg = _("The source filesystem of snapshot %s "
                    "not exist.") % snapshot['id']
            LOG.error(msg)
            raise exception.StorageResourceNotFound(
                name=snapshot['share_name'])

        snapshot_id = self.helper._get_snapshot_id(fs_id, snapshot['id'])
        self.helper.rollback_snapshot(snapshot_id)
