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
from cinder.zonemanager import utils as zm_utils


LOG = logging.getLogger(__name__)


@interface.volumedriver
class HuaweiISCSIDriver(huawei_base_driver.HuaweiBaseDriver,
                        driver.ISCSIDriver):
    def __init__(self, *args, **kwargs):
        super(HuaweiISCSIDriver, self).__init__(*args, **kwargs)

    def get_volume_stats(self, refresh=False):
        if not self._stats or refresh:
            super(HuaweiISCSIDriver, self).get_volume_stats()
            self._stats['storage_protocol'] = 'iSCSI'

        return self._stats

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        LOG.info('Initialize iscsi connection for volume %(id)s, '
                 'connector info %(conn)s.',
                 {'id': volume.id, 'conn': connector})
        metadata = huawei_utils.get_volume_private_data(volume)
        if metadata.get('hypermetro'):
            if (not connector.get('multipath') and
                    self.configuration.enforce_multipath_for_hypermetro):
                msg = _("Mapping hypermetro volume must use multipath.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            elif (not connector.get('multipath') and
                  not self.configuration.enforce_multipath_for_hypermetro):
                LOG.warning("Mapping hypermetro volume not use multipath,"
                            " so just mapping the local lun.")
            if not self.hypermetro_rmt_cli:
                msg = _("Mapping hypermetro volume requires remote.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        local_mapping = huawei_flow.initialize_iscsi_connection(
            volume, constants.LUN_TYPE, connector, self.local_cli,
            self.configuration)
        if metadata.get('hypermetro') and connector.get('multipath'):
            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if not hypermetro:
                msg = _("Mapping hypermetro remote volume error.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            remote_mapping = huawei_flow.initialize_remote_iscsi_connection(
                hypermetro['ID'], connector, self.hypermetro_rmt_cli,
                self.configuration)

            same_host_lun_id = self._change_same_host_lun_id(
                local_mapping, remote_mapping)
            mapping_info = self._merge_iscsi_mapping(
                local_mapping, remote_mapping, same_host_lun_id)
        else:
            mapping_info = local_mapping

        mapping_info.pop('aval_host_lun_ids', None)
        conn = {'driver_volume_type': 'iscsi',
                'data': mapping_info}
        LOG.info('Initialize iscsi connection successfully: %s.', conn)
        return conn

    def terminate_connection(self, volume, connector, **kwargs):
        host = connector['host'] if 'host' in connector else ""

        return self._terminate_connection_locked(host, volume, connector)

    @coordination.synchronized('huawei-mapping-{host}')
    def _terminate_connection_locked(self, host, volume, connector):
        LOG.info('Terminate iscsi connection for volume %(id)s, '
                 'connector info %(conn)s.',
                 {'id': volume.id, 'conn': connector})
        if self._is_volume_multi_attach_to_same_host(volume, connector):
            return

        metadata = huawei_utils.get_volume_private_data(volume)
        if metadata.get('hypermetro'):
            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if hypermetro:
                huawei_flow.terminate_remote_iscsi_connection(
                    hypermetro['ID'], connector, self.hypermetro_rmt_cli,
                    self.configuration)

        huawei_flow.terminate_iscsi_connection(
            volume, constants.LUN_TYPE, connector, self.local_cli,
            self.configuration)
        LOG.info('Terminate iscsi connection successfully.')

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection_snapshot(self, snapshot, connector, **kwargs):
        LOG.info('Initialize iscsi connection for snapshot %(id)s, '
                 'connector info %(conn)s.',
                 {'id': snapshot.id, 'conn': connector})
        mapping_info = huawei_flow.initialize_iscsi_connection(
            snapshot, constants.SNAPSHOT_TYPE, connector, self.local_cli,
            self.configuration)

        mapping_info.pop('aval_host_lun_ids', None)
        conn = {'driver_volume_type': 'iscsi',
                'data': mapping_info}
        LOG.info('Initialize iscsi connection successfully: %s.', conn)
        return conn

    def terminate_connection_snapshot(self, snapshot, connector, **kwargs):
        host = connector['host'] if 'host' in connector else ""

        return self._terminate_connection_snapshot_locked(host, snapshot,
                                                          connector)

    @coordination.synchronized('huawei-mapping-{host}')
    def _terminate_connection_snapshot_locked(self, host, snapshot, connector):
        LOG.info('Terminate iscsi connection for snapshot %(id)s, '
                 'connector info %(conn)s.',
                 {'id': snapshot.id, 'conn': connector})
        huawei_flow.terminate_iscsi_connection(
            snapshot, constants.SNAPSHOT_TYPE, connector, self.local_cli,
            self.configuration)
        LOG.info('Terminate iscsi connection successfully.')


@interface.volumedriver
class HuaweiFCDriver(huawei_base_driver.HuaweiBaseDriver,
                     driver.FibreChannelDriver):
    def __init__(self, *args, **kwargs):
        super(HuaweiFCDriver, self).__init__(*args, **kwargs)
        self.fc_san = zm_utils.create_lookup_service()

    def get_volume_stats(self, refresh=False):
        if not self._stats or refresh:
            super(HuaweiFCDriver, self).get_volume_stats()
            self._stats['storage_protocol'] = 'FC'

        return self._stats

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        LOG.info('Initialize FC connection for volume %(id)s, '
                 'connector info %(conn)s.',
                 {'id': volume.id, 'conn': connector})

        metadata = huawei_utils.get_volume_private_data(volume)
        if metadata.get('hypermetro'):
            if (not connector.get('multipath') and
                    self.configuration.enforce_multipath_for_hypermetro):
                msg = _("Mapping hypermetro volume must use multipath.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            elif (not connector.get('multipath') and
                  not self.configuration.enforce_multipath_for_hypermetro):
                LOG.warning("Mapping hypermetro volume not use multipath,"
                            " so just mapping the local lun.")
            if not self.hypermetro_rmt_cli:
                msg = _("Mapping hypermetro volume requires remote.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        local_mapping = huawei_flow.initialize_fc_connection(
            volume, constants.LUN_TYPE, connector, self.fc_san, self.local_cli,
            self.configuration)
        if metadata.get('hypermetro') and connector.get('multipath'):
            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if not hypermetro:
                msg = _("Mapping hypermetro remote volume error.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            remote_mapping = huawei_flow.initialize_remote_fc_connection(
                hypermetro['ID'], connector, self.fc_san,
                self.hypermetro_rmt_cli, self.configuration)
            same_host_lun_id = self._change_same_host_lun_id(
                local_mapping, remote_mapping)
            mapping_info = self._merge_fc_mapping(
                local_mapping, remote_mapping, same_host_lun_id)
        else:
            mapping_info = local_mapping

        mapping_info.pop('aval_host_lun_ids', None)
        conn = {'driver_volume_type': 'fibre_channel',
                'data': mapping_info}
        LOG.info('Initialize FC connection successfully: %s.', conn)
        zm_utils.add_fc_zone(conn)
        return conn

    def terminate_connection(self, volume, connector, **kwargs):
        host = connector['host'] if 'host' in connector else ""

        return self._terminate_connection_locked(host, volume, connector)

    @coordination.synchronized('huawei-mapping-{host}')
    def _terminate_connection_locked(self, host, volume, connector):
        LOG.info('Terminate FC connection for volume %(id)s, '
                 'connector info %(conn)s.',
                 {'id': volume.id, 'conn': connector})
        if self._is_volume_multi_attach_to_same_host(volume, connector):
            return

        metadata = huawei_utils.get_volume_private_data(volume)
        if metadata.get('hypermetro'):
            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if hypermetro:
                rmt_ini_tgt_map = huawei_flow.terminate_remote_fc_connection(
                    hypermetro['ID'], connector, self.fc_san,
                    self.hypermetro_rmt_cli, self.configuration)

        loc_ini_tgt_map = huawei_flow.terminate_fc_connection(
            volume, constants.LUN_TYPE, connector, self.fc_san, self.local_cli,
            self.configuration)
        if metadata.get('hypermetro'):
            self._merge_ini_tgt_map(loc_ini_tgt_map, rmt_ini_tgt_map)

        conn = {'driver_volume_type': 'fibre_channel',
                'data': {'initiator_target_map': loc_ini_tgt_map},
                }
        LOG.info('Terminate FC connection successfully: %s.', conn)
        zm_utils.remove_fc_zone(conn)
        return conn

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection_snapshot(self, snapshot, connector, **kwargs):
        LOG.info('Initialize FC connection for snapshot %(id)s, '
                 'connector info %(conn)s.',
                 {'id': snapshot.id, 'conn': connector})
        mapping_info = huawei_flow.initialize_fc_connection(
            snapshot, constants.SNAPSHOT_TYPE, connector, self.fc_san,
            self.local_cli, self.configuration)

        mapping_info.pop('aval_host_lun_ids', None)
        conn = {'driver_volume_type': 'fibre_channel',
                'data': mapping_info}
        LOG.info('Initialize FC connection successfully: %s.', conn)
        zm_utils.add_fc_zone(conn)
        return conn

    def terminate_connection_snapshot(self, snapshot, connector, **kwargs):
        host = connector['host'] if 'host' in connector else ""

        return self._terminate_connection_snapshot_locked(host, snapshot,
                                                          connector)

    @coordination.synchronized('huawei-mapping-{host}')
    def _terminate_connection_snapshot_locked(self, host, snapshot, connector):
        LOG.info('Terminate FC connection for snapshot %(id)s, '
                 'connector info %(conn)s.',
                 {'id': snapshot.id, 'conn': connector})
        ini_tgt_map = huawei_flow.terminate_fc_connection(
            snapshot, constants.SNAPSHOT_TYPE, connector, self.fc_san,
            self.local_cli, self.configuration)

        conn = {'driver_volume_type': 'fibre_channel',
                'data': {'initiator_target_map': ini_tgt_map},
                }
        LOG.info('Terminate FC connection successfully: %s.', conn)
        zm_utils.remove_fc_zone(conn)
        return conn
