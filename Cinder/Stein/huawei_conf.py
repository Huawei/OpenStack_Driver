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

"""
Set Huawei private configuration into Configuration object.

For conveniently get private configuration. We parse Huawei config file
and set every property into Configuration object as an attribute.
"""

import base64
from defusedxml import ElementTree as ET
import os
import re
import six

from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants

LOG = logging.getLogger(__name__)


class HuaweiConf(object):
    def __init__(self, conf):
        self.conf = conf
        self.last_modify_time = None

    def update_config_value(self):
        file_time = os.stat(self.conf.cinder_huawei_conf_file).st_mtime
        if self.last_modify_time == file_time:
            return

        self.last_modify_time = file_time
        tree = ET.parse(self.conf.cinder_huawei_conf_file)
        xml_root = tree.getroot()
        self._encode_authentication(tree, xml_root)

        attr_funcs = (
            self._san_address,
            self._san_user,
            self._san_password,
            self._san_vstore,
            self._san_product,
            self._ssl_cert_path,
            self._ssl_cert_verify,
            self._iscsi_info,
            self._fc_info,
            self._hyper_pair_sync_speed,
            self._replication_pair_sync_speed,
            self._hypermetro_devices,
            self._replication_devices,
            self._lun_type,
            self._lun_write_type,
            self._lun_prefetch,
            self._storage_pools,
            self._force_delete_volume,
            self._lun_copy_speed,
            self._lun_copy_mode,
            self._lun_copy_wait_interval,
            self._lun_timeout,
            self._get_minimum_fc_initiator,
            self._hyper_enforce_multipath,
            self._rollback_speed,
            self._get_local_in_band_or_not,
            self._get_local_storage_sn,
        )

        for f in attr_funcs:
            f(xml_root)

    def _encode_authentication(self, tree, xml_root):
        name_node = xml_root.find('Storage/UserName')
        pwd_node = xml_root.find('Storage/UserPassword')
        vstore_node = xml_root.find('Storage/vStoreName')

        need_encode = False
        if name_node is not None and not name_node.text.startswith('!$$$'):
            encoded = base64.b64encode(six.b(name_node.text)).decode()
            name_node.text = '!$$$' + encoded
            need_encode = True

        if pwd_node is not None and not pwd_node.text.startswith('!$$$'):
            encoded = base64.b64encode(six.b(pwd_node.text)).decode()
            pwd_node.text = '!$$$' + encoded
            need_encode = True

        if vstore_node is not None and not vstore_node.text.startswith('!$$$'):
            encoded = base64.b64encode(six.b(vstore_node.text)).decode()
            vstore_node.text = '!$$$' + encoded
            need_encode = True

        if need_encode:
            tree.write(self.conf.cinder_huawei_conf_file, 'UTF-8')

    def _san_address(self, xml_root):
        text = xml_root.findtext('Storage/RestURL')
        if not text:
            msg = _("RestURL is not configured.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        addrs = list(set([x.strip() for x in text.split(';') if x.strip()]))
        setattr(self.conf, 'san_address', addrs)

    def _san_user(self, xml_root):
        text = xml_root.findtext('Storage/UserName')
        if not text:
            msg = _("UserName is not configured.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        user = base64.b64decode(six.b(text[4:])).decode()
        setattr(self.conf, 'san_user', user)

    def _san_password(self, xml_root):
        text = xml_root.findtext('Storage/UserPassword')
        if not text:
            msg = _("UserPassword is not configured.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        pwd = base64.b64decode(six.b(text[4:])).decode()
        setattr(self.conf, 'san_password', pwd)

    def _san_vstore(self, xml_root):
        vstore = None
        text = xml_root.findtext('Storage/vStoreName')
        if text:
            vstore = base64.b64decode(six.b(text[4:])).decode()
        setattr(self.conf, 'vstore_name', vstore)

    def _ssl_cert_path(self, xml_root):
        text = xml_root.findtext('Storage/SSLCertPath')
        setattr(self.conf, 'ssl_cert_path', text)

    def _ssl_cert_verify(self, xml_root):
        value = False
        text = xml_root.findtext('Storage/SSLCertVerify')
        if text:
            if text.lower() in ('true', 'false'):
                value = text.lower() == 'true'
            else:
                msg = _("SSLCertVerify configured error.")
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        setattr(self.conf, 'ssl_cert_verify', value)

    def _set_extra_constants_by_product(self, product):
        extra_constants = {}
        if product in constants.DORADO_V6_AND_V6_PRODUCT:
            extra_constants['QOS_SPEC_KEYS'] = (
                'maxIOPS', 'minIOPS',
                'maxBandWidth', 'minBandWidth',
                'burstIOPS', 'burstBandWidth', 'burstTime',
                'IOType')
            extra_constants['QOS_IOTYPES'] = ('2',)
            extra_constants['SUPPORT_LUN_TYPES'] = ('Thin',)
            extra_constants['DEFAULT_LUN_TYPE'] = 'Thin'
            extra_constants['SUPPORT_CLONE_MODE'] = ('fastclone', 'luncopy')
        else:
            extra_constants['QOS_SPEC_KEYS'] = (
                'maxIOPS', 'minIOPS', 'minBandWidth',
                'maxBandWidth', 'latency', 'IOType')
            extra_constants['QOS_IOTYPES'] = ('0', '1', '2')
            extra_constants['SUPPORT_LUN_TYPES'] = ('Thick', 'Thin')
            extra_constants['DEFAULT_LUN_TYPE'] = 'Thick'
            extra_constants['SUPPORT_CLONE_MODE'] = ('luncopy',)

        for k in extra_constants:
            setattr(constants, k, extra_constants[k])

    def _san_product(self, xml_root):
        text = xml_root.findtext('Storage/Product')
        if not text:
            msg = _("SAN product is not configured.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        product = text.strip()
        if product not in constants.VALID_PRODUCT:
            msg = _("Invalid SAN product %(text)s, SAN product must be "
                    "in %(valid)s.") % {'text': product,
                                        'valid': constants.VALID_PRODUCT}
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        self._set_extra_constants_by_product(product)
        setattr(self.conf, 'san_product', product)

    def _lun_type(self, xml_root):
        lun_type = constants.DEFAULT_LUN_TYPE
        text = xml_root.findtext('LUN/LUNType')
        if text:
            lun_type = text.strip()
            if lun_type not in constants.LUN_TYPE_MAP:
                msg = _("Invalid lun type %s is configured.") % lun_type
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

            if lun_type not in constants.SUPPORT_LUN_TYPES:
                msg = _("%(array)s array requires %(valid)s lun type, "
                        "but %(conf)s is specified."
                        ) % {'array': self.conf.san_product,
                             'valid': constants.SUPPORT_LUN_TYPES,
                             'conf': lun_type}
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        setattr(self.conf, 'lun_type', constants.LUN_TYPE_MAP[lun_type])

    def _lun_write_type(self, xml_root):
        text = xml_root.findtext('LUN/WriteType')
        if text:
            write_type = text.strip()
            if write_type:
                setattr(self.conf, 'write_type', write_type)

    def _lun_prefetch(self, xml_root):
        node = xml_root.find('LUN/Prefetch')
        if node is not None:
            if 'Type' in node.attrib:
                prefetch_type = node.attrib['Type'].strip()
                setattr(self.conf, 'prefetch_type', prefetch_type)

            if 'Value' in node.attrib:
                prefetch_value = node.attrib['Value'].strip()
                setattr(self.conf, 'prefetch_value', prefetch_value)

    def _storage_pools(self, xml_root):
        text = xml_root.findtext('LUN/StoragePool')
        if not text:
            msg = _('Storage pool is not configured.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        pools = set(x.strip() for x in text.split(';') if x.strip())
        if not pools:
            msg = _('No valid storage pool configured.')
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        setattr(self.conf, 'storage_pools', list(pools))

    def _force_delete_volume(self, xml_root):
        force_delete_volume = False
        text = xml_root.findtext('LUN/ForceDeleteVolume')
        if text:
            if text.lower().strip() in ('true', 'false'):
                if text.lower().strip() == 'true':
                    force_delete_volume = True
            else:
                msg = _("ForceDeleteVolume configured error, "
                        "ForceDeleteVolume is %s.") % text
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        setattr(self.conf, 'force_delete_volume', force_delete_volume)

    def _iscsi_info(self, xml_root):
        iscsi_info = {}
        text = xml_root.findtext('iSCSI/DefaultTargetIP')
        if text:
            iscsi_info['default_target_ips'] = [
                ip.strip() for ip in text.split() if ip.strip()]

        initiators = {}
        nodes = xml_root.findall('iSCSI/Initiator')
        for node in nodes or []:
            if 'Name' in node.attrib:
                initiators[node.attrib['Name']] = node.attrib
            if 'HostName' in node.attrib:
                initiators[node.attrib['HostName']] = node.attrib

        if nodes and not initiators:
            msg = _("Name or HostName must be set one")
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        iscsi_info['initiators'] = initiators
        self._check_hostname_regex_config(iscsi_info)
        setattr(self.conf, 'iscsi_info', iscsi_info)

    def _fc_info(self, xml_root):
        fc_info = {}
        initiators = {}
        nodes = xml_root.findall('FC/Initiator')
        for node in nodes or []:
            if 'Name' in node.attrib:
                initiators[node.attrib['Name']] = node.attrib
            if 'HostName' in node.attrib:
                initiators[node.attrib['HostName']] = node.attrib

        if nodes and not initiators:
            msg = _("Name or HostName must be set one")
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        fc_info['initiators'] = initiators
        self._check_hostname_regex_config(fc_info)
        setattr(self.conf, 'fc_info', fc_info)

    def _check_hostname_regex_config(self, info):
        for item in info['initiators'].keys():
            ini = info['initiators'][item]
            if ini.get("HostName"):
                try:
                    if ini.get("HostName") == '*':
                        continue
                    re.compile(ini['HostName'])
                except Exception as err:
                    msg = _('Invalid initiator configuration. '
                            'Reason: %s.') % err
                    LOG.error(msg)
                    raise exception.InvalidInput(msg)

    def _convert_one_iscsi_info(self, ini_text):
        # get initiator configure attr list
        attr_list = re.split('[{;}]', ini_text)

        # get initiator configures
        ini = {}
        for attr in attr_list:
            if not attr:
                continue

            pair = attr.split(':', 1)
            if pair[0] == 'CHAPinfo':
                value = pair[1].replace('#', ';', 1)
            else:
                value = pair[1]
            ini[pair[0]] = value
        if 'Name' not in ini and 'HostName' not in ini:
            msg = _('Name or HostName must be specified for'
                    ' initiator.')
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        return ini

    def _parse_remote_initiator_info(self, dev, ini_type):
        ini_info = {'default_target_ips': []}

        if dev.get('iscsi_default_target_ip'):
            ini_info['default_target_ips'] = dev[
                'iscsi_default_target_ip'].split(';')

        initiators = {}
        if ini_type in dev:
            # Analyze initiators configure text, convert to:
            # [{'Name':'xxx'}, {'Name':'xxx','CHAPinfo':'mm-usr#mm-pwd'}]
            ini_list = re.split('\n', dev[ini_type])

            for text in ini_list:
                ini = self._convert_one_iscsi_info(text.strip())
                if 'Name' in ini:
                    initiators[ini['Name']] = ini
                if 'HostName' in ini:
                    initiators[ini['HostName']] = ini

            if ini_list and not initiators:
                msg = _("Name or HostName must be set one")
                LOG.error(msg)
                raise exception.InvalidInput(msg)

        ini_info['initiators'] = initiators
        self._check_hostname_regex_config(ini_info)
        return ini_info

    def _hypermetro_devices(self, xml_root):
        dev = self.conf.safe_get('hypermetro_device')
        config = {}

        if dev:
            config = {
                'san_address': dev['san_address'].split(';'),
                'san_user': dev['san_user'],
                'san_password': dev['san_password'],
                'vstore_name': dev.get('vstore_name'),
                'metro_domain': dev['metro_domain'],
                'storage_pools': dev['storage_pool'].split(';')[:1],
                'iscsi_info': self._parse_remote_initiator_info(
                    dev, 'iscsi_info'),
                'fc_info': self._parse_remote_initiator_info(
                    dev, 'fc_info'),
                'sync_speed': self.conf.hyper_sync_speed,
                'metro_sync_completed': dev['metro_sync_completed']
                if 'metro_sync_completed' in dev else "True",
                'in_band_or_not': dev['in_band_or_not'].lower() == 'true'
                if 'in_band_or_not' in dev else False,
                'storage_sn': dev.get('storage_sn')
            }

        setattr(self.conf, 'hypermetro', config)

    def _replication_devices(self, xml_root):
        replication_devs = self.conf.safe_get('replication_device')
        config = {}

        if replication_devs:
            dev = replication_devs[0]
            config = {
                'backend_id': dev['backend_id'],
                'san_address': dev['san_address'].split(';'),
                'san_user': dev['san_user'],
                'san_password': dev['san_password'],
                'vstore_name': dev.get('vstore_name'),
                'storage_pools': dev['storage_pool'].split(';')[:1],
                'iscsi_info': self._parse_remote_initiator_info(
                    dev, 'iscsi_info'),
                'fc_info': self._parse_remote_initiator_info(
                    dev, 'fc_info'),
                'sync_speed': self.conf.replica_sync_speed,
                'in_band_or_not': dev['in_band_or_not'].lower() == 'true'
                if 'in_band_or_not' in dev else False,
                'storage_sn': dev.get('storage_sn')
            }

        setattr(self.conf, 'replication', config)

    def _lun_copy_speed(self, xml_root):
        text = xml_root.findtext('LUN/LUNCopySpeed')
        if text and text.strip() not in constants.LUN_COPY_SPEED_TYPES:
            msg = (_("Invalid LUNCopySpeed '%(text)s', LUNCopySpeed must "
                     "be between %(low)s and %(high)s.")
                   % {"text": text, "low": constants.LUN_COPY_SPEED_LOW,
                      "high": constants.LUN_COPY_SPEED_HIGHEST})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if not text:
            speed = constants.LUN_COPY_SPEED_MEDIUM
        else:
            speed = text.strip()
        setattr(self.conf, 'lun_copy_speed', int(speed))

    def _lun_copy_mode(self, xml_root):
        clone_mode = constants.DEFAULT_CLONE_MODE
        text = xml_root.findtext('LUN/LUNCloneMode')
        if text:
            clone_mode = text.strip()
            if clone_mode not in constants.SUPPORT_CLONE_MODE:
                msg = _("%(array)s array requires %(valid)s lun type, "
                        "but %(conf)s is specified."
                        ) % {'array': self.conf.san_product,
                             'valid': constants.SUPPORT_CLONE_MODE,
                             'conf': clone_mode}
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        setattr(self.conf, 'clone_mode', clone_mode)

    def _hyper_pair_sync_speed(self, xml_root):
        text = xml_root.findtext('LUN/HyperSyncSpeed')
        if text and text.strip() not in constants.HYPER_SYNC_SPEED_TYPES:
            msg = (_("Invalid HyperSyncSpeed '%(text)s', HyperSyncSpeed must "
                     "be between %(low)s and %(high)s.")
                   % {"text": text, "low": constants.HYPER_SYNC_SPEED_LOW,
                      "high": constants.HYPER_SYNC_SPEED_HIGHEST})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if not text:
            speed = constants.HYPER_SYNC_SPEED_MEDIUM
        else:
            speed = text.strip()
        setattr(self.conf, 'hyper_sync_speed', int(speed))

    def _hyper_enforce_multipath(self, xml_root):
        enforce_multipath_for_hypermetro = True
        text = xml_root.findtext('LUN/HyperEnforceMultipath')
        if text:
            if text.lower().strip() in ('true', 'false'):
                if text.lower().strip() == 'false':
                    enforce_multipath_for_hypermetro = False
            else:
                msg = _("HyperEnforceMultipath configured error, "
                        "HyperEnforceMultipath is %s.") % text
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        setattr(self.conf, 'enforce_multipath_for_hypermetro',
                enforce_multipath_for_hypermetro)

    def _replication_pair_sync_speed(self, xml_root):
        text = xml_root.findtext('LUN/ReplicaSyncSpeed')
        if text and text.strip() not in constants.HYPER_SYNC_SPEED_TYPES:
            msg = (_("Invalid ReplicaSyncSpeed '%(text)s', ReplicaSyncSpeed "
                     "must be between %(low)s and %(high)s.")
                   % {"text": text, "low": constants.REPLICA_SYNC_SPEED_LOW,
                      "high": constants.REPLICA_SYNC_SPEED_HIGHEST})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if not text:
            speed = constants.REPLICA_SYNC_SPEED_MEDIUM
        else:
            speed = text.strip()
        setattr(self.conf, 'replica_sync_speed', int(speed))

    def _lun_copy_wait_interval(self, xml_root):
        text = xml_root.findtext('LUN/LUNcopyWaitInterval')

        if text and not text.isdigit():
            msg = (_("Invalid LUN_Copy_Wait_Interval '%s', "
                     "LUN_Copy_Wait_Interval must be a digit.")
                   % text)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        interval = text.strip() if text else constants.DEFAULT_WAIT_INTERVAL
        setattr(self.conf, 'lun_copy_wait_interval', int(interval))

    def _lun_timeout(self, xml_root):
        text = xml_root.findtext('LUN/Timeout')

        if text and not text.isdigit():
            msg = (_("Invalid LUN timeout '%s', LUN timeout must be a digit.")
                   % text)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        interval = text.strip() if text else constants.DEFAULT_WAIT_TIMEOUT
        setattr(self.conf, 'lun_timeout', int(interval))

    def _get_minimum_fc_initiator(self, xml_root):
        text = xml_root.findtext('FC/MinOnlineFCInitiator')
        minimum_fc_initiator = constants.DEFAULT_MINIMUM_FC_INITIATOR_ONLINE

        if text and not text.isdigit():
            msg = (_("Invalid FC MinOnlineFCInitiator '%s', "
                     "MinOnlineFCInitiator must be a digit.") % text)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if text and text.strip() and text.strip().isdigit():
            try:
                minimum_fc_initiator = int(text.strip())
            except Exception as err:
                msg = (_("Minimum FC initiator number %(num)s is set"
                         " too large, reason is %(err)s")
                       % {"num": text.strip(), "err": err})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        setattr(self.conf, 'min_fc_ini_online',
                minimum_fc_initiator)

    def _rollback_speed(self, xml_root):
        text = xml_root.findtext('LUN/SnapshotRollbackSpeed')
        if text and text.strip() not in constants.SNAPSHOT_ROLLBACK_SPEED_TYPES:
            msg = (_("Invalid SnapshotRollbackSpeed '%(text)s', "
                     "SnapshotRollbackSpeed must "
                     "be between %(low)s and %(high)s.")
                   % {"text": text,
                      "low": constants.SNAPSHOT_ROLLBACK_SPEED_LOW,
                      "high": constants.SNAPSHOT_ROLLBACK_SPEED_HIGHEST})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if not text:
            speed = constants.SNAPSHOT_ROLLBACK_SPEED_HIGH
        else:
            speed = text.strip()
        setattr(self.conf, 'rollback_speed', int(speed))

    def _get_local_in_band_or_not(self, xml_root):
        in_band_or_not = False
        text = xml_root.findtext('Storage/InBandOrNot')
        if text:
            if text.lower() in ('true', 'false'):
                in_band_or_not = text.lower() == 'true'
            else:
                msg = _("InBandOrNot configured error.")
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        setattr(self.conf, 'in_band_or_not', in_band_or_not)

    def _get_local_storage_sn(self, xml_root):
        text = xml_root.findtext('Storage/Storagesn')
        storagen_sn = text.strip() if text else None

        setattr(self.conf, 'storage_sn', storagen_sn)
