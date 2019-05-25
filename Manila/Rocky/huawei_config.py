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

import base64
import os

from oslo_log import log as logging
from oslo_utils import strutils
from xml.etree import ElementTree as ET

from manila import exception
from manila.i18n import _
from manila.share.drivers.huawei import constants

LOG = logging.getLogger(__name__)


class HuaweiConfig(object):
    def __init__(self, config):
        self.config = config
        self.last_modify_time = None
        self.update_configs()

    def update_configs(self):
        file_time = os.stat(self.config.manila_huawei_conf_file).st_mtime
        if self.last_modify_time == file_time:
            return

        self.last_modify_time = file_time

        tree = ET.parse(self.config.manila_huawei_conf_file)
        xml_root = tree.getroot()
        self._encode_authentication(tree, xml_root)

        attr_funcs = (
            self._nas_address,
            self._nas_user,
            self._nas_password,
            self._nas_product,
            self._ports,
            self._snapshot_support,
            self._replication_support,
            self._wait_interval,
            self._timeout,
            self._storage_pools,
            self._sector_size,
            self._nfs_client,
            self._cifs_client,
            self._logical_ip,
        )

        for f in attr_funcs:
            f(xml_root)

    def _encode_authentication(self, tree, xml_root):
        name_node = xml_root.find('Storage/UserName')
        pwd_node = xml_root.find('Storage/UserPassword')

        need_encode = False
        if name_node is not None and not name_node.text.startswith('!$$$'):
            name_node.text = '!$$$' + base64.b64encode(name_node.text)
            need_encode = True

        if pwd_node is not None and not pwd_node.text.startswith('!$$$'):
            pwd_node.text = '!$$$' + base64.b64encode(pwd_node.text)
            need_encode = True

        if need_encode:
            tree.write(self.config.manila_huawei_conf_file, 'UTF-8')

    def _nas_address(self, xml_root):
        text = xml_root.findtext('Storage/RestURL')
        if not text:
            msg = _("RestURL is not configured.")
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        addrs = set([x.strip() for x in text.split(';') if x.strip()])
        setattr(self.config, 'nas_address', list(addrs))

    def _nas_user(self, xml_root):
        text = xml_root.findtext('Storage/UserName')
        if not text:
            msg = _("UserName is not configured.")
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'nas_user', text.strip())

    def _nas_password(self, xml_root):
        text = xml_root.findtext('Storage/UserPassword')
        if not text:
            msg = _("UserPassword is not configured.")
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'nas_password', text.strip())

    def _nas_product(self, xml_root):
        text = xml_root.findtext('Storage/Product')
        if not text:
            msg = _("Storage product is not configured.")
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        if text not in constants.VALID_PRODUCTS:
            msg = _("Invalid storage product %(text)s, must be "
                    "in %(valid)s."
                    ) % {'text': text,
                         'valid': constants.VALID_PRODUCTS}
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'nas_product', text)

    def _wait_interval(self, xml_root):
        interval = constants.DEFAULT_WAIT_INTERVAL
        text = xml_root.findtext('Filesystem/WaitInterval')
        if text:
            interval = int(text.strip())
            if interval <= 0:
                msg = _("Invalid WaitInterval config %s, "
                        "must be a positive digit.") % text
                LOG.error(msg)
                raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'wait_interval', interval)

    def _timeout(self, xml_root):
        timeout = constants.DEFAULT_TIMEOUT
        text = xml_root.findtext('Filesystem/Timeout')
        if text:
            timeout = int(text.strip())
            if timeout <= 0:
                msg = _("Invalid Timeout config %s, must be "
                        "a positive digit.") % text
                LOG.error(msg)
                raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'timeout', timeout)

    def _storage_pools(self, xml_root):
        text = xml_root.findtext('Filesystem/StoragePool')
        if not text:
            msg = _('StoragePool must be configured.')
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        pools = set()
        for pool in text.split(';'):
            if pool.strip():
                pools.add(pool.strip())

        if not pools:
            msg = _('No valid storage pool configured.')
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'storage_pools', list(pools))

    def _logical_ip(self, xml_root):
        logical_ip = []
        text = xml_root.findtext('Storage/LogicalPortIP')
        if text:
            logical_ip = [i.strip() for i in text.split(";") if i.strip()]

        setattr(self.config, 'logical_ip', logical_ip)

    def _ports(self, xml_root):
        ports = []
        text = xml_root.findtext('Storage/Port')
        if text:
            for port in text.split(";"):
                if port.strip():
                    ports.append(port.strip())

        setattr(self.config, 'ports', ports)

    def _sector_size(self, xml_root):
        text = xml_root.findtext('Filesystem/SectorSize')
        if text and text.strip():
            setattr(self.config, 'sector_size', text.strip())

    def _snapshot_support(self, xml_root):
        snapshot_support = True
        text = xml_root.findtext('Storage/SnapshotSupport')
        if text:
            snapshot_support = strutils.bool_from_string(
                text.strip(), strict=True)
        setattr(self.config, 'snapshot_support', snapshot_support)

    def _replication_support(self, xml_root):
        replication_support = False
        text = xml_root.findtext('Storage/ReplicationSupport')
        if text:
            replication_support = strutils.bool_from_string(
                text.strip(), strict=True)
        setattr(self.config, 'replication_support', replication_support)

    def _nfs_client(self, xml_root):
        text = xml_root.findtext('Filesystem/NFSClient/IP')
        if text and text.strip():
            nfs_client_ip = text.strip()
        else:
            nfs_client_ip = None
        setattr(self.config, 'nfs_client_ip', nfs_client_ip)

    def _cifs_client(self, xml_root):
        text = xml_root.findtext('Filesystem/CIFSClient/UserName')
        if text and text.strip():
            cifs_client_name = text.strip()
        else:
            cifs_client_name = None
        setattr(self.config, 'cifs_client_name', cifs_client_name)

        text = xml_root.findtext('Filesystem/CIFSClient/UserPassword')
        if text and text.strip():
            cifs_client_password = text.strip()
        else:
            cifs_client_password = None
        setattr(self.config, 'cifs_client_password', cifs_client_password)
