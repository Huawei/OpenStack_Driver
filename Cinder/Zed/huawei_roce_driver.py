# Copyright (c) 2024 Huawei Technologies Co., Ltd.
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

from cinder import coordination
from cinder import exception
from cinder.i18n import _
from cinder import interface

from cinder.volume import driver
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_base_driver
from cinder.volume.drivers.huawei import huawei_flow
from cinder.volume.drivers.huawei import huawei_utils


LOG = logging.getLogger(__name__)


@interface.volumedriver
class HuaweiROCEDriver(huawei_base_driver.HuaweiBaseDriver,
                       driver.VolumeDriver):
    def __init__(self, *args, **kwargs):
        super(HuaweiROCEDriver, self).__init__(*args, **kwargs)

    def get_volume_stats(self, refresh=False):
        if not self._stats or refresh:
            super(HuaweiROCEDriver, self).get_volume_stats()
            self._stats['storage_protocol'] = 'nvmeof'

        return self._stats

    def is_connector_valid(self, connector):
        if not connector:
            msg = _("Connector is none.")
            LOG.error(msg)
            raise exception.InvalidConnectorException(data=msg)

        if not connector.get('nqn') or not connector.get('host'):
            msg = _("Connector doesn't have host and nqn information.")
            LOG.error(msg)
            raise exception.InvalidConnectorException(data=msg)

        if not self.configuration.is_dorado_v6:
            msg = _("Only dorado v6 supports RoCE.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def is_hypermetro_valid(self, volume):
        if not self.hypermetro_rmt_cli:
            msg = _("Mapping hypermetro volume requires remote.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
        if not hypermetro:
            msg = _("Mapping hypermetro remote volume error.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return hypermetro

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        LOG.info('Initialize RoCE connection for volume %s, '
                 'connector info %s.', volume.id, huawei_utils.mask_initiator_sensitive_info(
                         connector, sensitive_keys=constants.SENSITIVE_INI_KEYS))

        self.is_connector_valid(connector)

        metadata = huawei_utils.get_volume_private_data(volume)
        local_mapping = huawei_flow.initialize_roce_connection(
            volume, constants.LUN_TYPE, connector, self.local_cli,
            self.configuration)
        if metadata.get('hypermetro'):
            hypermetro = self.is_hypermetro_valid(volume)

            remote_mapping = huawei_flow.initialize_remote_roce_connection(
                hypermetro['ID'], connector, self.hypermetro_rmt_cli,
                self.configuration)
            mapping_info = self._merge_roce_mapping(
                local_mapping, remote_mapping)
        else:
            mapping_info = local_mapping
        conn = {'driver_volume_type': 'nvmeof', 'data': mapping_info}

        LOG.info('Initialize roce connection successfully,'
                 'return data is: %s.',
                 huawei_utils.mask_initiator_sensitive_info(conn, sensitive_keys=['nqn']))
        return conn

    def terminate_connection(self, volume, connector, **kwargs):
        if connector is None or 'host' not in connector:
            host = ""
        else:
            host = connector.get('host', "")

        return self._terminate_connection_locked(host, volume, connector)

    @coordination.synchronized('huawei-mapping-{host}')
    def _terminate_connection_locked(self, host, volume, connector):
        LOG.info('Terminate roce connection for volume %s, '
                 'connector info %s.', volume.id,
                 huawei_utils.mask_initiator_sensitive_info(
                     connector, sensitive_keys=constants.SENSITIVE_INI_KEYS))
        if self._is_volume_multi_attach_to_same_host(volume, connector):
            return

        metadata = huawei_utils.get_volume_private_data(volume)
        if metadata.get('hypermetro'):
            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if hypermetro:
                huawei_flow.terminate_remote_roce_connection(
                    hypermetro['ID'], connector, self.hypermetro_rmt_cli)

        huawei_flow.terminate_roce_connection(
            volume, constants.LUN_TYPE, connector, self.local_cli)
        LOG.info('Terminate roce connection successfully.')

