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

import re
import six
import uuid

from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder import objects
from cinder.objects import fields

from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_conf
from cinder.volume.drivers.huawei import huawei_flow
from cinder.volume.drivers.huawei import huawei_utils
from cinder.volume.drivers.huawei import hypermetro
from cinder.volume.drivers.huawei import replication
from cinder.volume.drivers.huawei import rest_client


LOG = logging.getLogger(__name__)

huawei_opts = [
    cfg.StrOpt('cinder_huawei_conf_file',
               default='/etc/cinder/cinder_huawei_conf.xml',
               help='The configuration file for Huawei driver.'),
    cfg.DictOpt('hypermetro_device',
                secret=True,
                help='To represent a hypermetro target device, which takes '
                     'standard dict config form: hypermetro_device = '
                     'key1:value1,key2:value2...'),
]

CONF = cfg.CONF
CONF.register_opts(huawei_opts)


class HuaweiBaseDriver(object):
    VERSION = "1.0.0"

    def __init__(self, *args, **kwargs):
        super(HuaweiBaseDriver, self).__init__(*args, **kwargs)

        if not self.configuration:
            msg = _('Configuration is not found.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        self.configuration.append_config_values(huawei_opts)

        self.active_backend_id = kwargs.get('active_backend_id')
        self.conf = huawei_conf.HuaweiConf(self.configuration)
        self.local_cli = None
        self.hypermetro_rmt_cli = None
        self.replication_rmt_cli = None
        self.support_capability = {}

    def do_setup(self, context):
        self.conf.update_config_value()

        self.local_cli = rest_client.RestClient(
            self.configuration.san_address,
            self.configuration.san_user,
            self.configuration.san_password,
            self.configuration.vstore_name,
            self.configuration.ssl_cert_verify,
            self.configuration.ssl_cert_path)
        self.local_cli.login()

        if self.configuration.hypermetro:
            self.hypermetro_rmt_cli = rest_client.RestClient(
                self.configuration.hypermetro['san_address'],
                self.configuration.hypermetro['san_user'],
                self.configuration.hypermetro['san_password'],
                self.configuration.hypermetro['vstore_name'],
            )
            self.hypermetro_rmt_cli.login()

        if self.configuration.replication:
            self.replication_rmt_cli = rest_client.RestClient(
                self.configuration.replication['san_address'],
                self.configuration.replication['san_user'],
                self.configuration.replication['san_password'],
                self.configuration.replication['vstore_name'],
            )
            self.replication_rmt_cli.login()

    def check_for_setup_error(self):
        def _check_storage_pools(client, config_pools):
            pools = client.get_all_pools()
            pool_names = [p['NAME'] for p in pools if
                          p.get('USAGETYPE', constants.BLOCK_POOL_TYPE) ==
                          constants.BLOCK_POOL_TYPE]

            for pool_name in config_pools:
                if pool_name not in pool_names:
                    msg = _('Storage pool %s does not exist.') % pool_name
                    LOG.error(msg)
                    raise exception.InvalidInput(reason=msg)

        _check_storage_pools(self.local_cli, self.configuration.storage_pools)
        if self.configuration.hypermetro:
            _check_storage_pools(
                self.hypermetro_rmt_cli,
                self.configuration.hypermetro['storage_pools'])
        if self.configuration.replication:
            _check_storage_pools(
                self.replication_rmt_cli,
                self.configuration.replication['storage_pools'])

        # If host is failed-over, switch the local and remote client.
        if (self.configuration.replication and self.active_backend_id ==
                self.configuration.replication['backend_id']):
            self._switch_replication_clients()

    def backup_use_temp_snapshot(self):
        return self.configuration.safe_get("backup_use_temp_snapshot")

    def create_export(self, context, volume, connector=None):
        pass

    def ensure_export(self, context, volume):
        pass

    def remove_export(self, context, volume):
        pass

    def create_export_snapshot(self, context, snapshot, connector):
        pass

    def remove_export_snapshot(self, context, snapshot):
        pass

    def _get_capacity(self, pool_info):
        """Get free capacity and total capacity of the pool."""
        free = pool_info.get('DATASPACE', pool_info['USERFREECAPACITY'])
        total = pool_info.get('USERTOTALCAPACITY')
        return (float(total) / constants.CAPACITY_UNIT,
                float(free) / constants.CAPACITY_UNIT)

    def _get_disk_type(self, pool_info):
        """Get disk type of the pool."""
        pool_disks = []
        for i, x in enumerate(constants.TIER_DISK_TYPES):
            if (pool_info.get('TIER%dCAPACITY' % i) and
                    pool_info.get('TIER%dCAPACITY' % i) != '0'):
                pool_disks.append(x)

        if len(pool_disks) > 1:
            pool_disks = ['mix']

        return pool_disks[0] if pool_disks else None

    def _get_smarttier(self, disk_type):
        return disk_type is not None and disk_type == 'mix'

    def _update_pool_stats(self):
        pools = []
        for pool_name in self.configuration.storage_pools:
            pool = {
                'pool_name': pool_name,
                'reserved_percentage':
                    self.configuration.reserved_percentage,
                'max_over_subscription_ratio':
                    self.configuration.max_over_subscription_ratio,
                'smartpartition':
                    self.support_capability['SmartPartition'],
                'smartcache': self.support_capability['SmartCache'],
                'QoS_support': self.support_capability['SmartQoS'],
                'thin_provisioning_support':
                    self.support_capability['SmartThin'],
                'thick_provisioning_support': True,
                'hypermetro': self.support_capability['HyperMetro'],
                'consistentcygroup_support': True,
                'consistent_group_snapshot_enabled':
                    self.support_capability['HyperSnap'],
                'location_info': self.local_cli.device_id,
                'replication_enabled':
                    self.support_capability['HyperReplication'],
                'replication_type': ['sync', 'async'],
                'multiattach': True,
                'dedup': [self.support_capability['SmartDedupe[\s\S]*LUN'],
                          False],
                'compression':
                    [self.support_capability['SmartCompression[\s\S]*LUN'],
                     False],
                'huawei_controller': True,
                'huawei_application_type': False,
            }

            if self.configuration.san_product == "Dorado":
                pool['huawei_application_type'] = True

            pool_info = self.local_cli.get_pool_by_name(pool_name)
            if pool_info:
                total_capacity, free_capacity = self._get_capacity(pool_info)
                disk_type = self._get_disk_type(pool_info)
                tier_support = self._get_smarttier(disk_type)

                pool['total_capacity_gb'] = total_capacity
                pool['free_capacity_gb'] = free_capacity
                pool['smarttier'] = (self.support_capability['SmartTier'] and
                                     tier_support)
                if disk_type:
                    pool['disk_type'] = disk_type

            pools.append(pool)

        return pools

    def _update_hypermetro_capability(self):
        if self.hypermetro_rmt_cli:
            feature_status = self.hypermetro_rmt_cli.get_feature_status()
            if (feature_status.get('HyperMetro') not in
                    constants.AVAILABLE_FEATURE_STATUS):
                    self.support_capability['HyperMetro'] = False
        else:
            self.support_capability['HyperMetro'] = False

    def _update_replication_capability(self):
        if self.replication_rmt_cli:
            feature_status = self.replication_rmt_cli.get_feature_status()
            if (feature_status.get('HyperReplication') not in
                    constants.AVAILABLE_FEATURE_STATUS):
                    self.support_capability['HyperReplication'] = False
        else:
            self.support_capability['HyperReplication'] = False

    def _update_support_capability(self):
        feature_status = self.local_cli.get_feature_status()

        for c in constants.CHECK_FEATURES:
            self.support_capability[c] = False
            for f in feature_status:
                if re.match(c, f):
                    self.support_capability[c] = (
                        feature_status[f] in
                        constants.AVAILABLE_FEATURE_STATUS)
                    break
            else:
                if constants.CHECK_FEATURES[c]:
                    self.support_capability[c] = self.local_cli.check_feature(
                        constants.CHECK_FEATURES[c])

        self._update_hypermetro_capability()
        self._update_replication_capability()

        LOG.debug('Update backend capabilities: %s.', self.support_capability)

    def _update_volume_stats(self):
        self._update_support_capability()
        pools = self._update_pool_stats()

        self._stats['pools'] = pools
        self._stats['volume_backend_name'] = (
            self.configuration.safe_get('volume_backend_name') or
            self.__class__.__name__)
        self._stats['driver_version'] = self.VERSION
        self._stats['vendor_name'] = 'Huawei'
        self._stats['replication_enabled'] = (
            self.support_capability['HyperReplication'])
        if self._stats['replication_enabled']:
            self._stats['replication_targets'] = (
                [self.configuration.replication['backend_id']])

    def get_volume_stats(self):
        """Get volume status and reload huawei config file."""
        self.conf.update_config_value()
        self._update_volume_stats()

    def create_volume(self, volume):
        (lun_id, lun_wwn, hypermetro_id, replication_id
         ) = huawei_flow.create_volume(
            volume, self.local_cli, self.hypermetro_rmt_cli,
            self.replication_rmt_cli, self.configuration,
            self.support_capability)

        model_update = huawei_utils.get_volume_model_update(
            volume, huawei_lun_id=lun_id, huawei_lun_wwn=lun_wwn,
            hypermetro_id=hypermetro_id, replication_id=replication_id,
            huawei_sn=self.local_cli.device_id
        )
        return model_update

    def delete_volume(self, volume):
        try:
            huawei_flow.delete_volume(
                volume, self.local_cli, self.hypermetro_rmt_cli,
                self.replication_rmt_cli, self.configuration)
        except Exception as exc:
            if huawei_utils.is_not_exist_exc(exc):
                return
            LOG.exception('Delete volume %s failed.', volume.id)
            raise

    def migrate_volume(self, ctxt, volume, host):
        try:
            huawei_flow.migrate_volume(volume, host, self.local_cli,
                                       self.support_capability)
        except Exception:
            LOG.exception('Migrate volume %s by backend failed.', volume.id)
            return False, {}

        return True, {}

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        new_name = huawei_utils.encode_name(volume.id)
        org_metadata = huawei_utils.get_volume_private_data(volume)
        new_metadata = huawei_utils.get_volume_private_data(new_volume)

        try:
            if org_metadata.get('huawei_sn') == new_metadata.get('huawei_sn'):
                self.local_cli.rename_lun(org_metadata['huawei_lun_id'],
                                          new_name[:-4] + '-org')
            self.local_cli.rename_lun(new_metadata['huawei_lun_id'],
                                      new_name, description=volume.name)
        except Exception:
            LOG.exception('Unable to rename lun %(id)s to %(name)s.',
                          {'id': new_metadata['huawei_lun_id'],
                           'name': new_name})
            name_id = new_volume.name_id
        else:
            LOG.info("Successfully rename lun %(id)s to %(name)s.",
                     {'id': new_metadata['huawei_lun_id'],
                      'name': new_name})
            name_id = None

        return {'_name_id': name_id,
                'provider_location': huawei_utils.to_string(**new_metadata),
                }

    def create_volume_from_snapshot(self, volume, snapshot):
        (lun_id, lun_wwn, hypermetro_id, replication_id
         ) = huawei_flow.create_volume_from_snapshot(
            volume, snapshot, self.local_cli, self.hypermetro_rmt_cli,
            self.replication_rmt_cli, self.configuration,
            self.support_capability)

        model_update = huawei_utils.get_volume_model_update(
            volume, huawei_lun_id=lun_id, huawei_lun_wwn=lun_wwn,
            hypermetro_id=hypermetro_id, replication_id=replication_id,
            huawei_sn=self.local_cli.device_id
        )
        return model_update

    def create_cloned_volume(self, volume, src_vref):
        (lun_id, lun_wwn, hypermetro_id, replication_id
         ) = huawei_flow.create_volume_from_volume(
            volume, src_vref, self.local_cli, self.hypermetro_rmt_cli,
            self.replication_rmt_cli, self.configuration,
            self.support_capability)

        model_update = huawei_utils.get_volume_model_update(
            volume, huawei_lun_id=lun_id, huawei_lun_wwn=lun_wwn,
            hypermetro_id=hypermetro_id, replication_id=replication_id,
            huawei_sn=self.local_cli.device_id
        )
        return model_update

    def extend_volume(self, volume, new_size):
        huawei_flow.extend_volume(
            volume, new_size, self.local_cli, self.hypermetro_rmt_cli,
            self.replication_rmt_cli, self.configuration)

    def create_snapshot(self, snapshot):
        snapshot_id, snapshot_wwn = huawei_flow.create_snapshot(
            snapshot, self.local_cli, self.support_capability)
        self.local_cli.activate_snapshot(snapshot_id)

        location = huawei_utils.to_string(
            huawei_snapshot_id=snapshot_id,
            huawei_snapshot_wwn=snapshot_wwn)
        return {'provider_location': location}

    def delete_snapshot(self, snapshot):
        try:
            huawei_flow.delete_snapshot(snapshot, self.local_cli)
        except Exception as exc:
            if huawei_utils.is_not_exist_exc(exc):
                return
            LOG.exception('Delete snapshot %s failed.', snapshot.id)
            raise

    def retype(self, ctxt, volume, new_type, diff, host):
        LOG.info('Start volume %(id)s retype. new_type: %(new_type)s, '
                 'diff: %(diff)s, host: %(host)s.',
                 {'id': volume.id, 'new_type': new_type,
                  'diff': diff, 'host': host})

        orig_lun_info = huawei_utils.get_lun_info(self.local_cli, volume)
        if not orig_lun_info:
            msg = _("Volume %s does not exist.") % volume.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        new_opts = huawei_utils.get_volume_type_params(new_type)

        if (volume.host != host['host'] or
                ('LUNType' in new_opts and
                 new_opts['LUNType'] != orig_lun_info['ALLOCTYPE'])):
            hypermetro_id, replication_id = huawei_flow.retype_by_migrate(
                volume, new_opts, host, self.local_cli,
                self.hypermetro_rmt_cli, self.replication_rmt_cli,
                self.configuration, self.support_capability)
        else:
            hypermetro_id, replication_id = huawei_flow.retype(
                volume, new_opts, self.local_cli, self.hypermetro_rmt_cli,
                self.replication_rmt_cli, self.configuration,
                self.support_capability)

        model_update = huawei_utils.get_volume_model_update(
            volume, hypermetro_id=hypermetro_id, replication_id=replication_id)

        return True, model_update

    def manage_existing_get_size(self, volume, existing_ref):
        lun_info = huawei_utils.get_external_lun_info(self.local_cli,
                                                      existing_ref)
        if not lun_info:
            msg = _("Lun %s to manage not exist.") % existing_ref
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        remainder = float(lun_info['CAPACITY']) % constants.CAPACITY_UNIT
        if remainder > 0:
            msg = _("LUN size must be times of 1GB.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        size = float(lun_info['CAPACITY']) / constants.CAPACITY_UNIT
        return int(size)

    def manage_existing(self, volume, existing_ref):
        (lun_id, lun_wwn, hypermetro_id, replication_id
         ) = huawei_flow.manage_existing(
            volume, existing_ref, self.local_cli,
            self.hypermetro_rmt_cli, self.replication_rmt_cli,
            self.configuration, self.support_capability)

        model_update = huawei_utils.get_volume_model_update(
            volume, huawei_lun_id=lun_id, huawei_lun_wwn=lun_wwn,
            hypermetro_id=hypermetro_id, replication_id=replication_id,
            huawei_sn=self.local_cli.device_id
        )
        return model_update

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        snapshot_info = huawei_utils.get_external_snapshot_info(
            self.local_cli, existing_ref)
        if not snapshot_info:
            msg = _("Snapshot %s not exist.") % existing_ref
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        remainder = float(snapshot_info['USERCAPACITY']
                          ) % constants.CAPACITY_UNIT
        if remainder > 0:
            msg = _("Snapshot size must be times of 1GB.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        size = float(snapshot_info['USERCAPACITY']) / constants.CAPACITY_UNIT
        return int(size)

    def manage_existing_snapshot(self, snapshot, existing_ref):
        snapshot_id, snapshot_wwn = huawei_flow.manage_existing_snapshot(
            snapshot, existing_ref, self.local_cli)

        location = huawei_utils.to_string(
            huawei_snapshot_id=snapshot_id,
            huawei_snapshot_wwn=snapshot_wwn)
        return {'provider_location': location}

    def create_group(self, context, group):
        huawei_flow.create_group(
            group, self.local_cli, self.hypermetro_rmt_cli,
            self.replication_rmt_cli, self.configuration,
            self.support_capability)
        return {'status': fields.GroupStatus.AVAILABLE}

    def create_group_from_src(self, context, group, volumes,
                              snapshots=None, source_vols=None):
        model_update = self.create_group(context, group)
        volumes_model_update = []
        delete_snapshots = False

        if not snapshots and source_vols:
            snapshots = []
            for src_vol in source_vols:
                vol_kwargs = {
                    'id': src_vol.id,
                    'provider_location': src_vol.provider_location,
                }
                snapshot_kwargs = {'id': six.text_type(uuid.uuid4()),
                                   'volume': objects.Volume(**vol_kwargs)}
                snapshot = objects.Snapshot(**snapshot_kwargs)
                snapshots.append(snapshot)

            snapshots_model_update = self._create_group_snapshot(snapshots)
            for i, model in enumerate(snapshots_model_update):
                snapshot = snapshots[i]
                snapshot.provider_location = model['provider_location']

            delete_snapshots = True

        if snapshots:
            try:
                for i, vol in enumerate(volumes):
                    snapshot = snapshots[i]
                    vol_model_update = self.create_volume_from_snapshot(
                        vol, snapshot)
                    vol_model_update.update({'id': vol.id})
                    volumes_model_update.append(vol_model_update)
            finally:
                if delete_snapshots:
                    self._delete_group_snapshot(snapshots)

        return model_update, volumes_model_update

    def delete_group(self, context, group, volumes):
        opts = huawei_utils.get_group_type_params(group)

        hypermetro_group = any(opt for opt in opts if opt.get('hypermetro'))
        if hypermetro_group:
            hypermetro_mgr = hypermetro.HuaweiHyperMetro(
                self.local_cli, self.hypermetro_rmt_cli,
                self.configuration.hypermetro)
            hypermetro_mgr.delete_consistencygroup(group.id, volumes)

        replication_group = any(opt for opt in opts
                                if opt.get('replication_enabled'))
        if replication_group:
            replication_mgr = replication.ReplicationManager(
                self.local_cli, self.replication_rmt_cli,
                self.configuration.replication)
            replication_mgr.delete_group(group.id, volumes)

        model_update = {'status': fields.GroupStatus.DELETED}

        volumes_model_update = []
        for volume in volumes:
            update = {'id': volume.id}
            try:
                self.delete_volume(volume)
                update['status'] = 'deleted'
            except Exception:
                update['status'] = 'error_deleting'
            finally:
                volumes_model_update.append(update)

        return model_update, volumes_model_update

    def update_group(self, context, group,
                     add_volumes=None, remove_volumes=None):
        opts = huawei_utils.get_group_type_params(group)

        hypermetro_group = any(opt for opt in opts if opt.get('hypermetro'))
        if hypermetro_group:
            hypermetro_mgr = hypermetro.HuaweiHyperMetro(
                self.local_cli, self.hypermetro_rmt_cli,
                self.configuration.hypermetro)
            hypermetro_mgr.update_consistencygroup(
                group.id, add_volumes, remove_volumes)

        replication_group = any(opt for opt in opts
                                if opt.get('replication_enabled'))
        if replication_group:
            replication_mgr = replication.ReplicationManager(
                self.local_cli, self.replication_rmt_cli,
                self.configuration.replication)
            replication_mgr.update_group(
                group.id, add_volumes, remove_volumes)

        model_update = {'status': fields.GroupStatus.AVAILABLE}

        return model_update, None, None

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        try:
            snapshots_model_update = self._create_group_snapshot(snapshots)
        except Exception:
            LOG.exception("Failed to create snapshots for group %s.",
                          group_snapshot.id)
            raise

        model_update = {'status': fields.GroupSnapshotStatus.AVAILABLE}
        return model_update, snapshots_model_update

    def _create_group_snapshot(self, snapshots):
        snapshots_model_update = []
        created_snapshots = []

        for snapshot in snapshots:
            try:
                snapshot_id, snapshot_wwn = huawei_flow.create_snapshot(
                    snapshot, self.local_cli, self.support_capability)
            except Exception:
                LOG.exception("Failed to create snapshot %s of group.",
                              snapshot.id)
                for snap_id in created_snapshots:
                    self.local_cli.delete_snapshot(snap_id)
                raise

            location = huawei_utils.to_string(
                huawei_snapshot_id=snapshot_id,
                huawei_snapshot_wwn=snapshot_wwn)
            snap_model_update = {
                'id': snapshot.id,
                'status': fields.SnapshotStatus.AVAILABLE,
                'provider_location': location,
            }
            snapshots_model_update.append(snap_model_update)
            created_snapshots.append(snapshot_id)

        try:
            self.local_cli.activate_snapshot(created_snapshots)
        except Exception:
            LOG.exception("Failed to activate group snapshots %s.",
                          created_snapshots)
            for snap_id in created_snapshots:
                self.local_cli.delete_snapshot(snap_id)
            raise

        return snapshots_model_update

    def delete_group_snapshot(self, context, group_snapshot, snapshots):
        try:
            snapshots_model_update = self._delete_group_snapshot(snapshots)
        except Exception:
            LOG.exception("Failed to delete snapshots for group %s.",
                          group_snapshot.id)
            raise

        model_update = {'status': fields.GroupSnapshotStatus.DELETED}
        return model_update, snapshots_model_update

    def _delete_group_snapshot(self, snapshots):
        snapshots_model_update = []
        for snapshot in snapshots:
            try:
                self.delete_snapshot(snapshot)
                snapshot_model = {'id': snapshot.id,
                                  'status': fields.SnapshotStatus.DELETED}
                snapshots_model_update.append(snapshot_model)
            except Exception:
                LOG.exception("Failed to delete snapshot %s of group.",
                              snapshot.id)
                raise

        return snapshots_model_update

    def failover_host(self, context, volumes, secondary_id=None, groups=None):
        if secondary_id == 'default':
            if not self.active_backend_id:
                return None, [], []

            volumes_update = huawei_flow.failback(
                volumes, self.local_cli, self.replication_rmt_cli,
                self.configuration)
            secondary_id = ''
        elif secondary_id in (
                None, self.configuration.replication['backend_id']):
            if (self.active_backend_id ==
                    self.configuration.replication['backend_id']):
                # Already failover, return success
                return self.active_backend_id, [], []

            volumes_update = huawei_flow.failover(
                volumes, self.local_cli, self.replication_rmt_cli,
                self.configuration)
            secondary_id = self.configuration.replication['backend_id']
        else:
            msg = "Invalid secondary id %s." % secondary_id
            raise exception.InvalidReplicationTarget(reason=msg)

        self.active_backend_id = secondary_id
        self._switch_replication_clients()

        return secondary_id, volumes_update, []

    def _switch_replication_clients(self):
        self.local_cli, self.replication_rmt_cli = (
            self.replication_rmt_cli, self.local_cli)
        (self.configuration.iscsi_info,
         self.configuration.replication['iscsi_info']) = (
            self.configuration.replication['iscsi_info'],
            self.configuration.iscsi_info
        )

    def _change_same_host_lun_id(self, local_mapping, remote_mapping):
        loc_aval_host_lun_ids = local_mapping.get('aval_host_lun_ids', [])
        rmt_aval_host_lun_ids = remote_mapping.get('aval_host_lun_ids', [])

        if local_mapping['hostlun_id'] == remote_mapping['hostlun_id']:
            return local_mapping['hostlun_id']

        for i in xrange(1, 512):
            if i in loc_aval_host_lun_ids and i in rmt_aval_host_lun_ids:
                same_host_lun_id = i
                break
        else:
            same_host_lun_id = None

        if not same_host_lun_id:
            msg = _("Can't find common host lun id for hypermetro volume.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self.local_cli.change_hostlun_id(
            local_mapping['mappingview_id'], local_mapping['lun_id'],
            same_host_lun_id)
        self.hypermetro_rmt_cli.change_hostlun_id(
            remote_mapping['mappingview_id'], remote_mapping['lun_id'],
            same_host_lun_id)
        return same_host_lun_id

    def _merge_iscsi_mapping(self, local_mapping, remote_mapping,
                             same_host_lun_id):
        local_mapping['target_iqns'].extend(remote_mapping['target_iqns'])
        local_mapping['target_portals'].extend(
            remote_mapping['target_portals'])
        local_mapping['target_luns'] = [same_host_lun_id] * len(
            local_mapping['target_portals'])
        return local_mapping

    def _merge_fc_mapping(self, local_mapping, remote_mapping,
                          same_host_lun_id):
        self._merge_ini_tgt_map(local_mapping['initiator_target_map'],
                                remote_mapping['initiator_target_map'])
        local_mapping['target_lun'] = same_host_lun_id
        local_mapping['target_wwn'] += remote_mapping['target_wwn']

        return local_mapping

    def _merge_ini_tgt_map(self, loc, rmt):
        for k in rmt:
            loc[k] = loc.get(k, []) + rmt[k]
