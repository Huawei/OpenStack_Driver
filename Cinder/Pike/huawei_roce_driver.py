# Copyright (c) 2025 Huawei Technologies Co., Ltd.
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
from oslo_log import log as logging
from oslo_utils import excutils

from cinder import coordination
from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei.huawei_driver import HuaweiBaseDriver
from cinder.volume.drivers.huawei import huawei_utils

LOG = logging.getLogger(__name__)


class HuaweiROCEDriver(HuaweiBaseDriver):
    """RoCE driver for Huawei storage arrays.

    Version history:
        25.2.0: support RoCE driver for OceanStor Dorado
    """

    def __init__(self, *args, **kwargs):
        super(HuaweiROCEDriver, self).__init__(*args, **kwargs)

    @staticmethod
    def _get_mapped_host(client, connector, lun_id):
        host_key = 'host'
        if connector is None or host_key not in connector:
            host_info = client.get_mapped_host_info(lun_id)
            if not host_info:
                LOG.warning('No host info in connector and lun %s is not mapped to any host.', lun_id)
                return None
            if len(host_info) > 1:
                LOG.warning("No host info in connector and lun %s is mapped to multiple hosts.", lun_id)
                return None
            host_id = host_info[0]['hostId']
        else:
            host_name = connector[host_key]
            host_id = client.get_host_id_by_name(host_name)
        return host_id

    @staticmethod
    def _delete_host(client, initiator_name, host_id):
        if client.is_roce_initiator_associated_to_host(
                initiator_name, host_id):
            client.remove_roce_initiator_from_host(initiator_name, host_id)
        client.remove_host(host_id)

    @staticmethod
    def _do_mapping(client, lun_info, host_id):
        """Map lun to the host."""
        lun_id = lun_info['ID']
        LOG.info('do_mapping, host_id: %(host_id)s, lun_id: %(lun_id)s.',
                 {'host_id': host_id, 'lun_id': lun_id})

        mapping_info = client.get_mapping(host_id, lun_id)
        if not mapping_info:
            client.create_mapping(host_id, lun_id)
        else:
            LOG.info("Mapping between host %s and lun %s already exists.", host_id, lun_id)

    @staticmethod
    def _get_initialize_response(client, target_ips, lun_info):
        mapping_info = {
            'portals': [(ip, constants.ROCE_TARGET_PORT, 'rdma') for ip in target_ips],
            'target_nqn': constants.ROCE_TARGET_NQN_PREFIX + client.device_id,
            'discard': True,
            'volume_nguid': lun_info.get('NGUID'),
            'vol_uuid': lun_info.get('NGUID'),
        }
        return mapping_info

    def get_volume_stats(self, refresh=False):
        """Get volume status."""
        data = HuaweiBaseDriver.get_volume_stats(self, refresh=False)
        backend_name = self.configuration.safe_get('volume_backend_name')
        data['volume_backend_name'] = backend_name or self.__class__.__name__
        data['storage_protocol'] = 'nvmeof'
        data['driver_version'] = self.VERSION
        data['vendor_name'] = 'Huawei'
        return data

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        """Map a volume to a host and return target RoCE information."""
        self._check_roce_params(volume, connector)

        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("Attach Volume, metadata is: %s.", metadata)
        # Attach local lun.
        local_info = self._do_initialize(volume, connector, True)
        LOG.info('initialize_connection_roce, return data is: %s.',
                 huawei_utils.mask_initiator_sensitive_info(local_info, sensitive_keys=['host_nqn', 'nqn']))
        return local_info

    def terminate_connection(self, volume, connector, **kwargs):
        """Delete map between a volume and a host."""
        host = "" if connector is None else connector.get('host', "")
        lock_mapping = 'huawei-mapping-%s' % host

        @coordination.synchronized(lock_mapping)
        def lock_host_when_terminate_connection():
            return self._terminate_connection_locked(volume, connector)

        return lock_host_when_terminate_connection()

    def _do_initialize(self, volume, connector, local):
        try:
            return self._initialize_connection(volume, connector, local)
        except Exception:
            with excutils.save_and_reraise_exception():
                self._terminate_connection(volume, connector)
            return {}

    def _initialize_connection(self, volume, connector, local=True):
        LOG.info('Initialize RoCE connection for volume %(id)s, '
                 'connector info %(conn)s. array is in %(location)s.',
                 {'id': volume.id,
                  'conn': huawei_utils.mask_initiator_sensitive_info(
                      connector, sensitive_keys=constants.SENSITIVE_INI_KEYS),
                  'location': 'local' if local else 'remote'})

        client = self.client if local else self.rmt_client
        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_RAISE, local)
        lun_info = client.get_lun_info(lun_id, lun_type)
        target_ips = client.get_roce_params(connector)
        host_nqn = connector.get("host_nqn") or connector.get("nqn")
        host_id = client.add_roce_host(connector.get('host'), host_nqn)

        try:
            client.ensure_roceini_added(host_nqn, host_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.remove_host_with_check(host_id)

        self._do_mapping(client, lun_info, host_id)

        mapping_info = self._get_initialize_response(client, target_ips, lun_info)
        conn = {
            'driver_volume_type': 'nvmeof',
            'data': mapping_info
        }
        LOG.info('Initialize RoCE connection successfully: %s.',
                 huawei_utils.mask_initiator_sensitive_info(conn, sensitive_keys=constants.SENSITIVE_INI_KEYS))
        return conn

    def _terminate_connection_locked(self, volume, connector):
        """Delete map between a volume and a host."""
        attachments = volume.volume_attachment
        if volume.multiattach and len(attachments.objects) > 1 and sum(
                1 for a in attachments.objects if a.connector == connector) > 1:
            LOG.info("Volume is multi-attach and attached to the same host"
                     " multiple times")
            return

        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("terminate_connection, metadata is: %s.", metadata)
        self._terminate_connection(volume, connector)
        LOG.info('terminate_connection success.')
        return

    def _terminate_connection(self, volume, connector, local=True):
        LOG.info('_terminate_connection, detach %(local)s volume.',
                 {'local': 'local' if local else 'remote'})

        client = self.client if local else self.rmt_client
        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_WARN, local)
        LOG.info('terminate_connection: LUN ID: %(lun_id)s, lun type: %(lun_type)s, connector: %('
                 'connector)s.', {'lun_id': lun_id,
                                  'lun_type': lun_type,
                                  'connector': huawei_utils.mask_initiator_sensitive_info(
                                      connector, sensitive_keys=constants.SENSITIVE_INI_KEYS)})

        # unmap the lun from host
        host_id = self._get_mapped_host(client, connector, lun_id)
        if not host_id or self.configuration.retain_storage_mapping:
            return

        initiator_name = connector.get('host_nqn') or connector.get('nqn')
        self._clear_mapping(client, initiator_name, host_id, lun_id)

    def _clear_mapping(self, client, initiator_name, host_id, lun_id):
        if not host_id:
            return
        client.delete_mapping(host_id, lun_id)
        mapped_luns = client.get_mapped_lun_info(host_id)
        if mapped_luns:
            LOG.info('Host %s has other lun mapped.', host_id)
            return
        self._delete_host(client, initiator_name, host_id)

    def _check_roce_params(self, volume, connector):
        if not volume or not connector:
            msg = _(
                '%(param)s is none.'
                % {'param': 'volume' if not volume else 'connector'})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not volume.id:
            msg = _(
                'volume param is error. volume is %(volume)s.'
                % {'volume': volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not (connector.get('host_nqn') or connector.get('nqn')) or not connector.get('host'):
            msg = _(
                'connector param is error. connector is %(connector)s.'
                % {'connector': huawei_utils.mask_initiator_sensitive_info(
                    connector, sensitive_keys=['host_nqn', 'nqn'])})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not self.is_dorado_v6:
            msg = _("Current storage doesn't support RoCE.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
