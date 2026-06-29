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
import six
from defusedxml import ElementTree as ET

from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder import utils
from cinder.volume.drivers.huawei import constants

LOG = logging.getLogger(__name__)


class HuaweiConf(object):
    def __init__(self, conf):
        self.conf = conf

    def _encode_authentication(self):
        need_encode = False
        tree = ET.parse(self.conf.cinder_huawei_conf_file)
        xml_root = tree.getroot()
        name_node = xml_root.find('Storage/UserName')
        pwd_node = xml_root.find('Storage/UserPassword')
        vstore_node = xml_root.find('Storage/vStoreName')
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
            utils.execute('chmod',
                          '600',
                          self.conf.cinder_huawei_conf_file,
                          run_as_root=True)
            tree.write(self.conf.cinder_huawei_conf_file, 'UTF-8')

    def update_config_value(self):
        self._encode_authentication()

        set_attr_funcs = (self._san_address,
                          self._san_user,
                          self._san_password,
                          self._vstore_name,
                          self._san_product,
                          self._san_protocol,
                          self._lun_type,
                          self._lun_ready_wait_interval,
                          self._lun_copy_wait_interval,
                          self._lun_timeout,
                          self._lun_write_type,
                          self._lun_prefetch,
                          self._storage_pools,
                          self._iscsi_default_target_ip,
                          self._iscsi_info,
                          self._fc_info,
                          self._ssl_cert_path,
                          self._ssl_cert_verify,
                          self._lun_copy_speed,
                          self._dedup_license,
                          self._compression_license,)

        tree = ET.parse(self.conf.cinder_huawei_conf_file)
        xml_root = tree.getroot()
        for f in set_attr_funcs:
            f(xml_root)

    @staticmethod
    def _get_ssl_verify(xml_root):
        value = False
        text = xml_root.findtext('Storage/SSLCertVerify')
        if text:
            if text.lower() in ('true', 'false'):
                value = text.lower() == 'true'
            else:
                msg = _("SSLCertVerify configured error.")
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        return value

    def _ssl_cert_path(self, xml_root):
        text = xml_root.findtext('Storage/SSLCertPath')
        ssl_value = self._get_ssl_verify(xml_root)
        if text and ssl_value:
            setattr(self.conf, 'ssl_cert_path', text)
        elif not text and ssl_value:
            msg = _("Cert path is necessary if SSLCertVerify is True.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        else:
            setattr(self.conf, 'ssl_cert_path', None)

    def _ssl_cert_verify(self, xml_root):
        value = self._get_ssl_verify(xml_root)
        setattr(self.conf, 'ssl_cert_verify', value)

    def _san_address(self, xml_root):
        text = xml_root.findtext('Storage/RestURL')
        if not text:
            msg = _("RestURL is not configured.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        addrs = text.split(';')
        addrs = list({x.strip() for x in addrs if x.strip()})
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

    def _vstore_name(self, xml_root):
        text = xml_root.findtext('Storage/vStoreName')
        if text:
            vstore_name = base64.b64decode(six.b(text[4:])).decode()
            setattr(self.conf, 'vstore_name', vstore_name)
        else:
            setattr(self.conf, 'vstore_name', None)

    def _set_extra_constants_by_product(self, product):
        extra_constants = {}
        if product in constants.DORADO_V6_AND_V6_PRODUCT:
            extra_constants['QOS_SPEC_KEYS'] = (
                'maxIOPS', 'maxBandWidth', 'IOType')
            extra_constants['QOS_IOTYPES'] = ('2',)
            extra_constants['SUPPORT_LUN_TYPES'] = ('Thin',)
            extra_constants['DEFAULT_LUN_TYPE'] = 'Thin'
        else:
            extra_constants['QOS_SPEC_KEYS'] = (
                'maxIOPS', 'minIOPS', 'minBandWidth',
                'maxBandWidth', 'latency', 'IOType')
            extra_constants['QOS_IOTYPES'] = ('0', '1', '2')
            extra_constants['SUPPORT_LUN_TYPES'] = ('Thick', 'Thin')
            extra_constants['DEFAULT_LUN_TYPE'] = 'Thick'

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
            msg = (_("Invalid SAN product '%(text)s', SAN product must be in "
                     "%(valid)s.")
                   % {'text': product, 'valid': constants.VALID_PRODUCT})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        self._set_extra_constants_by_product(product)
        setattr(self.conf, 'san_product', product)

    def _san_protocol(self, xml_root):
        text = xml_root.findtext('Storage/Protocol')
        if not text:
            msg = _("SAN protocol is not configured.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if text not in constants.VALID_PROTOCOL:
            msg = (_("Invalid SAN protocol '%(text)s', SAN protocol must be "
                     "in %(valid)s.")
                   % {'text': text, 'valid': constants.VALID_PROTOCOL})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        protocol = text.strip()
        setattr(self.conf, 'san_protocol', protocol)

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

    def _lun_ready_wait_interval(self, xml_root):
        text = xml_root.findtext('LUN/LUNReadyWaitInterval')

        if text and not text.isdigit():
            msg = (_("Invalid LUN_Ready_Wait_Interval '%s', "
                     "LUN_Ready_Wait_Interval must be a digit.")
                   % text)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        interval = text.strip() if text else constants.DEFAULT_WAIT_INTERVAL
        setattr(self.conf, 'lun_ready_wait_interval', int(interval))

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

    def _lun_write_type(self, xml_root):
        text = xml_root.findtext('LUN/WriteType')
        write_type = text.strip() if text else '1'

        if write_type not in constants.VALID_WRITE_TYPE:
            msg = (_("Invalid LUN WriteType '%(text)s', LUN WriteType must be "
                     "in %(valid)s.")
                   % {'text': write_type, 'valid': constants.VALID_WRITE_TYPE})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        setattr(self.conf, 'lun_write_type', write_type)

    def _lun_prefetch(self, xml_root):
        prefetch_type = '3'
        prefetch_value = '0'

        node = xml_root.find('LUN/Prefetch')
        if (node is not None
                and node.attrib['Type']
                and node.attrib['Value']):
            prefetch_type = node.attrib['Type'].strip()
            if prefetch_type not in ['0', '1', '2', '3']:
                msg = (_(
                    "Invalid prefetch type '%s' is configured. "
                    "PrefetchType must be in 0,1,2,3.") % prefetch_type)
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

            prefetch_value = node.attrib['Value'].strip()
            factor = {'1': 2}
            factor = int(factor.get(prefetch_type, '1'))
            prefetch_value = int(prefetch_value) * factor
            prefetch_value = six.text_type(prefetch_value)

        setattr(self.conf, 'lun_prefetch_type', prefetch_type)
        setattr(self.conf, 'lun_prefetch_value', prefetch_value)

    def _storage_pools(self, xml_root):
        nodes = xml_root.findall('LUN/StoragePool')
        if not nodes:
            msg = _('Storage pool is not configured.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        texts = [x.text for x in nodes]
        merged_text = ';'.join(texts)
        pools = set(x.strip() for x in merged_text.split(';') if x.strip())
        if not pools:
            msg = _('Invalid storage pool is configured.')
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        setattr(self.conf, 'storage_pools', list(pools))

    def _iscsi_default_target_ip(self, xml_root):
        text = xml_root.findtext('iSCSI/DefaultTargetIP')
        target_ip = text.split() if text else []
        setattr(self.conf, 'iscsi_default_target_ip', target_ip)

    def _iscsi_info(self, xml_root):
        nodes = xml_root.findall('iSCSI/Initiator')
        if nodes is None:
            setattr(self.conf, 'iscsi_info', [])
            return

        iscsi_info = []
        for node in nodes:
            props = {}
            for item in node.items():
                props[item[0].strip()] = item[1].strip()

            iscsi_info.append(props)

        setattr(self.conf, 'iscsi_info', iscsi_info)

    def _fc_info(self, xml_root):
        nodes = xml_root.findall('FC/Initiator')
        if nodes is None:
            setattr(self.conf, 'fc_info', [])
            return

        fc_info = []
        for node in nodes:
            props = {}
            for item in node.items():
                props[item[0].strip()] = item[1].strip()

            fc_info.append(props)

        setattr(self.conf, 'fc_info', fc_info)

    def _parse_rmt_iscsi_info(self, iscsi_info):
        if not (iscsi_info and iscsi_info.strip()):
            return []

        # Consider iscsi_info value:
        # ' {Name:xxx ;;TargetPortGroup: xxx};\n'
        # '{Name:\t\rxxx;CHAPinfo: mm-usr#mm-pwd} '

        # Step 1, ignore whitespace characters, convert to:
        # '{Name:xxx;;TargetPortGroup:xxx};{Name:xxx;CHAPinfo:mm-usr#mm-pwd}'
        iscsi_info = ''.join(iscsi_info.split())

        # Step 2, make initiators configure list, convert to:
        # ['Name:xxx;;TargetPortGroup:xxx', 'Name:xxx;CHAPinfo:mm-usr#mm-pwd']
        initiator_infos = iscsi_info[1:-1].split('};{')

        # Step 3, get initiator configure pairs, convert to:
        # [['Name:xxx', '', 'TargetPortGroup:xxx'],
        #  ['Name:xxx', 'CHAPinfo:mm-usr#mm-pwd']]
        initiator_infos = map(lambda x: x.split(';'), initiator_infos)

        # Step 4, remove invalid configure pairs, convert to:
        # [['Name:xxx', 'TargetPortGroup:xxx'],
        # ['Name:xxx', 'CHAPinfo:mm-usr#mm-pwd']]
        initiator_infos = map(lambda x: filter(lambda y: y, x),
                              initiator_infos)

        # Step 5, make initiators configure dict, convert to:
        # [{'TargetPortGroup': 'xxx', 'Name': 'xxx'},
        #  {'Name': 'xxx', 'CHAPinfo': 'mm-usr#mm-pwd'}]
        get_opts = lambda x: x.split(':', 1)
        initiator_infos = map(lambda x: dict(map(get_opts, x)),
                              initiator_infos)
        # Convert generator to list for py3 compatibility.
        initiator_infos = list(initiator_infos)

        # Step 6, replace CHAPinfo 'user#pwd' to 'user;pwd'
        key = 'CHAPinfo'
        for info in initiator_infos:
            if key in info:
                info[key] = info[key].replace('#', ';', 1)

        return initiator_infos

    def get_hypermetro_devices(self):
        devs = self.conf.safe_get('hypermetro_device')
        if not devs:
            return []

        devs_config = []
        for dev in devs:
            dev_config = {}
            dev_config['san_address'] = dev['san_address'].split(';')
            dev_config['san_user'] = dev['san_user']
            dev_config['san_password'] = dev['san_password']
            dev_config['vstore_name'] = dev.get('vstore_name')
            dev_config['metro_domain'] = dev['metro_domain']
            dev_config['storage_pools'] = dev['storage_pool'].split(';')
            dev_config['iscsi_info'] = self._parse_rmt_iscsi_info(
                dev.get('iscsi_info'))
            dev_config['fc_info'] = self._parse_rmt_iscsi_info(
                dev.get('fc_info'))
            dev_config['iscsi_default_target_ip'] = (
                dev['iscsi_default_target_ip'].split(';')
                if 'iscsi_default_target_ip' in dev
                else [])
            devs_config.append(dev_config)

        return devs_config

    def get_replication_devices(self):
        devs = self.conf.safe_get('replication_device')
        if not devs:
            return []

        devs_config = []
        for dev in devs:
            dev_config = {}
            dev_config['backend_id'] = dev['backend_id']
            dev_config['san_address'] = dev['san_address'].split(';')
            dev_config['san_user'] = dev['san_user']
            dev_config['san_password'] = dev['san_password']
            dev_config['vstore_name'] = dev.get('vstore_name')
            dev_config['storage_pools'] = dev['storage_pool'].split(';')
            dev_config['iscsi_info'] = self._parse_rmt_iscsi_info(
                dev.get('iscsi_info'))
            dev_config['fc_info'] = self._parse_rmt_iscsi_info(
                dev.get('fc_info'))
            dev_config['iscsi_default_target_ip'] = (
                dev['iscsi_default_target_ip'].split(';')
                if 'iscsi_default_target_ip' in dev
                else [])
            devs_config.append(dev_config)

        return devs_config

    def get_local_device(self):
        dev_config = {
            'backend_id': "default",
            'san_address': self.conf.san_address,
            'san_user': self.conf.san_user,
            'san_password': self.conf.san_password,
            'vstore_name': self.conf.vstore_name,
            'storage_pools': self.conf.storage_pools,
            'iscsi_info': self.conf.iscsi_info,
            'fc_info': self.conf.fc_info,
            'iscsi_default_target_ip': self.conf.iscsi_default_target_ip,
        }
        return dev_config

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

    def _set_license_configuration(self, text, license_type):
        license_list = []
        if text:
            license_list = list(set(x.strip() for x in text.split(';') if x.strip()))
        setattr(self.conf, license_type, license_list)

    def _dedup_license(self, xml_root):
        text = xml_root.findtext('LUN/DedupLicense')

        self._set_license_configuration(text, 'dedup_license')

    def _compression_license(self, xml_root):
        text = xml_root.findtext('LUN/CompressionLicense')
        self._set_license_configuration(text, 'compression_license')
