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

from cinder import coordination
from cinder import exception
from cinder import interface

from cinder.volume import driver
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_base_driver
from cinder.volume.drivers.huawei import huawei_flow
from cinder.volume.drivers.huawei import huawei_utils


LOG = logging.getLogger(__name__)


@interface.volumedriver
class HuaweiTCPDriver(huawei_base_driver.HuaweiBaseDriver,
                       driver.VolumeDriver):
    def __init__(self, *args, **kwargs):
        super(HuaweiTCPDriver, self).__init__(*args, **kwargs)

    def get_volume_stats(self, refresh=False):
        if not self._stats or refresh:
            super(HuaweiTCPDriver, self).get_volume_stats()
            self._stats['storage_protocol'] = 'NVMe-TCP'

        return self._stats

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        LOG.info('Initialize nvme over tcp connection for volume %s, '
                 'connector info %s.', volume.id, huawei_utils.mask_initiator_sensitive_info(
                         connector, sensitive_keys=constants.SENSITIVE_INI_KEYS))

        self._is_connector_valid(connector)

        mapping_info = huawei_flow.initialize_tcp_connection(
            volume, constants.LUN_TYPE, connector, self.local_cli,
            self.configuration)
        conn = {'driver_volume_type': 'nvmeof', 'data': mapping_info}

        LOG.info('Initialize nvme over tcp connection successfully,'
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
        LOG.info('Terminate nvme over tcp connection for volume %s, '
                 'connector info %s.', volume.id,
                 huawei_utils.mask_initiator_sensitive_info(
                     connector, sensitive_keys=constants.SENSITIVE_INI_KEYS))
        if self._is_volume_multi_attach_to_same_host(volume, connector):
            return

        huawei_flow.terminate_tcp_connection(
            volume, constants.LUN_TYPE, connector, self.local_cli)
        LOG.info('Terminate nvme over tcp connection successfully.')

    def _is_connector_valid(self, connector):
        if not connector:
            msg = "Connector is none."
            LOG.error(msg)
            raise exception.InvalidConnectorException(data=msg)

        if not connector.get('nqn') or not connector.get('host'):
            msg = "Connector doesn't have host and nqn information."
            LOG.error(msg)
            raise exception.InvalidConnectorException(data=msg)
