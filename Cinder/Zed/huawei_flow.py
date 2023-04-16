# Copyright (c) 2017 Huawei Technologies Co., Ltd.
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

import ipaddress
import json
import six
import uuid

from oslo_log import log as logging
from oslo_utils import strutils

import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task
from taskflow.types import failure

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_utils
from cinder.volume.drivers.huawei import hypermetro
from cinder.volume.drivers.huawei import replication
from cinder.volume.drivers.huawei import smartx
from cinder.volume import volume_utils

LOG = logging.getLogger(__name__)


class LunOptsCheckTask(task.Task):
    default_provides = 'opts'

    def __init__(self, client, feature_support, configuration, new_opts=None,
                 *args, **kwargs):
        super(LunOptsCheckTask, self).__init__(*args, **kwargs)
        self.client = client
        self.feature_support = feature_support
        self.configuration = configuration
        self.new_opts = new_opts

    def execute(self, volume):
        if self.new_opts:
            opts = self.new_opts
        else:
            is_dorado_v6 = self.configuration.is_dorado_v6
            opts = huawei_utils.get_volume_params(volume, is_dorado_v6)

        huawei_utils.check_volume_type_valid(opts)

        feature_pairs = (
            ('qos', 'SmartQoS'),
            ('smartcache', 'SmartCache'),
            ('smartpartition', 'SmartPartition'),
            ('hypermetro', 'HyperMetro'),
            ('replication_enabled', 'HyperReplication'),
            ('policy', 'SmartTier'),
            ('dedup', 'SmartDedupe[\s\S]*LUN'),
            ('compression', 'SmartCompression[\s\S]*LUN'),
        )

        for feature in feature_pairs:
            if opts.get(feature[0]) and not self.feature_support[feature[1]]:
                msg = _("Huawei storage doesn't support %s.") % feature[1]
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if opts.get('smartcache'):
            smartcache = smartx.SmartCache(self.client)
            smartcache.check_cache_valid(opts['cachename'])

        if opts.get('smartpartition'):
            smartpartition = smartx.SmartPartition(self.client)
            smartpartition.check_partition_valid(opts['partitionname'])

        return opts


class CreateLunTask(task.Task):
    default_provides = ('lun_id', 'lun_info')

    def __init__(self, client, configuration, feature_support,
                 *args, **kwargs):
        super(CreateLunTask, self).__init__(*args, **kwargs)
        self.client = client
        self.configuration = configuration
        self.feature_support = feature_support

    def _get_lun_application_name(self, opts, lun_params):
        if opts.get('applicationname') is not None:
            workload_type_id = self.client.get_workload_type_id(
                opts['applicationname'])
            if workload_type_id:
                lun_params['WORKLOADTYPEID'] = workload_type_id
            else:
                msg = _("The workload type %s is not exist. Please create it "
                        "on the array") % opts['applicationname']
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        return lun_params

    def execute(self, volume, opts, src_size=None):
        pool_name = volume_utils.extract_host(volume.host, level='pool')
        pool_id = self.client.get_pool_id(pool_name)
        if not pool_id:
            msg = _("Pool %s doesn't exist in storage.") % pool_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        lun_params = {
            'NAME': huawei_utils.encode_name(volume.id),
            'PARENTID': pool_id,
            'DESCRIPTION': volume.name,
            'ALLOCTYPE': opts.get('LUNType', self.configuration.lun_type),
            'CAPACITY': int(int(src_size) * constants.CAPACITY_UNIT if src_size
                            else int(volume.size) * constants.CAPACITY_UNIT),
        }

        if opts.get('controllername'):
            controller = self.client.get_controller_id(opts['controllername'])
            if controller:
                lun_params['OWNINGCONTROLLER'] = controller
        if hasattr(self.configuration, 'write_type'):
            lun_params['WRITEPOLICY'] = self.configuration.write_type
        if hasattr(self.configuration, 'prefetch_type'):
            lun_params['PREFETCHPOLICY'] = self.configuration.prefetch_type
        if hasattr(self.configuration, 'prefetch_value'):
            lun_params['PREFETCHVALUE'] = self.configuration.prefetch_value
        if opts.get('policy'):
            lun_params['DATATRANSFERPOLICY'] = opts['policy']

        if opts.get('dedup') is not None:
            lun_params['ENABLESMARTDEDUP'] = opts['dedup']
        elif not self.feature_support['SmartDedupe[\s\S]*LUN']:
            lun_params['ENABLESMARTDEDUP'] = False

        if opts.get('compression') is not None:
            lun_params['ENABLECOMPRESSION'] = opts['compression']
        elif not self.feature_support['SmartCompression[\s\S]*LUN']:
            lun_params['ENABLECOMPRESSION'] = False

        lun_params = self._get_lun_application_name(opts, lun_params)

        lun = self.client.create_lun(lun_params)
        return lun['ID'], lun

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_lun(result[0])


class WaitLunOnlineTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(WaitLunOnlineTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, lun_id):
        huawei_utils.wait_lun_online(self.client, lun_id)


class AddQoSTask(task.Task):
    default_provides = 'qos_id'

    def __init__(self, client, configuration, *args, **kwargs):
        super(AddQoSTask, self).__init__(*args, **kwargs)
        self.smartqos = smartx.SmartQos(client, configuration.is_dorado_v6)

    def execute(self, lun_id, opts):
        if opts.get('qos'):
            qos_id = self.smartqos.add(opts['qos'], lun_id)
            return qos_id

    def revert(self, result, lun_id, **kwargs):
        if isinstance(result, failure.Failure):
            return
        if result:
            self.smartqos.remove(result, lun_id)


class AddCacheTask(task.Task):
    default_provides = 'cache_id'

    def __init__(self, client, *args, **kwargs):
        super(AddCacheTask, self).__init__(*args, **kwargs)
        self.smartcache = smartx.SmartCache(client)

    def execute(self, lun_id, opts):
        if opts.get('smartcache'):
            cache_id = self.smartcache.add(opts['cachename'], lun_id)
            return cache_id

    def revert(self, result, lun_id, **kwargs):
        if isinstance(result, failure.Failure):
            return
        if result:
            self.smartcache.remove(result, lun_id)


class AddPartitionTask(task.Task):
    default_provides = 'partition_id'

    def __init__(self, client, *args, **kwargs):
        super(AddPartitionTask, self).__init__(*args, **kwargs)
        self.smartpartition = smartx.SmartPartition(client)

    def execute(self, lun_id, opts):
        if opts.get('smartpartition'):
            partition_id = self.smartpartition.add(
                opts['partitionname'], lun_id)
            return partition_id

    def revert(self, result, lun_id, **kwargs):
        if isinstance(result, failure.Failure):
            return
        if result:
            self.smartpartition.remove(result, lun_id)


class CreateHyperMetroTask(task.Task):
    default_provides = 'hypermetro_id'

    def __init__(self, local_cli, remote_cli, config, is_sync=True,
                 *args, **kwargs):
        super(CreateHyperMetroTask, self).__init__(*args, **kwargs)
        self.hypermetro = hypermetro.HuaweiHyperMetro(
            local_cli, remote_cli, config)
        self.loc_client = local_cli
        self.rmt_client = remote_cli
        self.sync = is_sync

    def execute(self, volume, lun_id, lun_info, opts):
        metadata = huawei_utils.get_volume_private_data(volume)
        hypermetro_id = None

        if not opts.get('hypermetro'):
            return hypermetro_id

        if metadata.get('hypermetro'):
            hypermetro = huawei_utils.get_hypermetro(self.loc_client, volume)
            hypermetro_id = hypermetro.get('ID') if hypermetro else None

        if not hypermetro_id:
            lun_keys = ('CAPACITY', 'ALLOCTYPE', 'PREFETCHPOLICY',
                        'PREFETCHVALUE', 'WRITEPOLICY', 'DATATRANSFERPOLICY')
            lun_params = {k: lun_info[k] for k in lun_keys if k in lun_info}
            lun_params['NAME'] = huawei_utils.encode_name(volume.id)
            lun_params['DESCRIPTION'] = volume.name
            if (lun_info.get("WORKLOADTYPENAME") and
                    lun_info.get("WORKLOADTYPEID")):
                workload_type_name = self.loc_client.get_workload_type_name(
                    lun_info['WORKLOADTYPEID'])
                rmt_workload_type_id = self.rmt_client.get_workload_type_id(
                    workload_type_name)
                if rmt_workload_type_id:
                    lun_params['WORKLOADTYPEID'] = rmt_workload_type_id
                else:
                    msg = _("The workload type %s is not exist. Please create "
                            "it on the array") % workload_type_name
                    LOG.error(msg)
                    raise exception.InvalidInput(reason=msg)

            hypermetro_id = self.hypermetro.create_hypermetro(
                lun_id, lun_params, self.sync)

        return hypermetro_id

    def revert(self, result, volume, **kwargs):
        if isinstance(result, failure.Failure):
            return
        if result:
            self.hypermetro.delete_hypermetro(volume)


class AddHyperMetroGroupTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(AddHyperMetroGroupTask, self).__init__(*args, **kwargs)
        self.hypermetro = hypermetro.HuaweiHyperMetro(
            local_cli, remote_cli, config)

    def execute(self, volume, hypermetro_id):
        if volume.group_id and hypermetro_id:
            self.hypermetro.add_hypermetro_to_group(
                volume.group_id, hypermetro_id)


class CreateReplicationTask(task.Task):
    default_provides = 'replication_id'

    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(CreateReplicationTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)
        self.loc_client = local_cli
        self.rmt_client = remote_cli

    def execute(self, volume, lun_id, lun_info, opts):
        data = huawei_utils.get_replication_data(volume)
        pair_id = data.get('pair_id')

        if opts.get('replication_enabled') and not pair_id:
            lun_keys = ('CAPACITY', 'ALLOCTYPE', 'PREFETCHPOLICY',
                        'PREFETCHVALUE', 'WRITEPOLICY', 'DATATRANSFERPOLICY')
            lun_params = {k: lun_info[k] for k in lun_keys if k in lun_info}
            lun_params['NAME'] = huawei_utils.encode_name(volume.id)
            lun_params['DESCRIPTION'] = volume.name
            if (lun_info.get("WORKLOADTYPENAME") and
                    lun_info.get("WORKLOADTYPEID")):
                workload_type_name = self.loc_client.get_workload_type_name(
                    lun_info['WORKLOADTYPEID'])
                rmt_workload_type_id = self.rmt_client.get_workload_type_id(
                    workload_type_name)
                if rmt_workload_type_id:
                    lun_params['WORKLOADTYPEID'] = rmt_workload_type_id
                else:
                    msg = _("The workload type %s is not exist. Please create "
                            "it on the array") % workload_type_name
                    LOG.error(msg)
                    raise exception.InvalidInput(reason=msg)

            pair_id = self.replication.create_replica(
                lun_id, lun_params, opts['replication_type'])
        elif not opts.get('replication_enabled') and pair_id:
            pair_id = None

        return pair_id

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        if result:
            self.replication.delete_replica(result)


class AddReplicationGroupTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(AddReplicationGroupTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)

    def execute(self, volume, replication_id):
        if volume.group_id and replication_id:
            self.replication.add_replication_to_group(
                volume.group_id, replication_id)


class CheckLunExistTask(task.Task):
    default_provides = ('lun_info', 'lun_id')

    def __init__(self, client, *args, **kwargs):
        super(CheckLunExistTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, volume):
        lun_info = huawei_utils.get_lun_info(self.client, volume)
        if not lun_info:
            msg = _("Volume %s does not exist.") % volume.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return lun_info, lun_info['ID']


class CheckLunIsInUse(task.Task):
    def __init__(self, *args, **kwargs):
        super(CheckLunIsInUse, self).__init__(*args, **kwargs)

    def execute(self, opts, volume, lun_info):
        """
        opts: come from LunOptsCheckTask
        lun_info: come from CheckLunExistTask
        """
        add_hypermetro = False
        delete_hypermetro = False
        metadata = huawei_utils.get_volume_private_data(volume)
        in_use_lun = lun_info.get('EXPOSEDTOINITIATOR') == 'true'
        if opts.get('hypermetro'):
            if not metadata.get('hypermetro'):
                add_hypermetro = True
        else:
            if not metadata.get('hypermetro'):
                delete_hypermetro = True

        if (add_hypermetro or delete_hypermetro) and in_use_lun:
            msg = _("Cann't add hypermetro to the volume in use.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)


class GetLunIDTask(task.Task):
    default_provides = 'lun_id'

    def __init__(self, client, *args, **kwargs):
        super(GetLunIDTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, volume):
        lun_info = huawei_utils.get_lun_info(self.client, volume)
        if not lun_info:
            LOG.error("Volume %s does not exist.", volume.id)
            return None

        return lun_info['ID']


class CheckLunMappedTask(task.Task):
    def __init__(self, client, configuration, *args, **kwargs):
        super(CheckLunMappedTask, self).__init__(*args, **kwargs)
        self.client = client
        self.configuration = configuration

    def execute(self, lun_info):
        if lun_info.get('EXPOSEDTOINITIATOR') == 'true':
            msg = _("LUN %s has been mapped to host. Now force to "
                    "delete it") % lun_info['ID']
            LOG.warning(msg)
            huawei_utils.remove_lun_from_lungroup(
                self.client, lun_info["ID"],
                self.configuration.force_delete_volume)


class DeleteHyperMetroTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(DeleteHyperMetroTask, self).__init__(*args, **kwargs)
        self.hypermetro = hypermetro.HuaweiHyperMetro(
            local_cli, remote_cli, config)

    def execute(self, volume, opts=None):
        metadata = huawei_utils.get_volume_private_data(volume)

        if ((not opts or not opts.get('hypermetro'))
                and metadata.get('hypermetro')):
            self.hypermetro.delete_hypermetro(volume)


class DeleteReplicationTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(DeleteReplicationTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)

    def execute(self, volume, opts=None):
        data = huawei_utils.get_replication_data(volume)
        pair_id = data.get('pair_id')
        if (not opts or not opts.get('replication_enabled')) and pair_id:
            self.replication.delete_replica(pair_id)


class DeleteQoSTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteQoSTask, self).__init__(*args, **kwargs)
        self.smartqos = smartx.SmartQos(client)

    def execute(self, lun_info):
        qos_id = lun_info.get('IOCLASSID')
        if qos_id:
            self.smartqos.remove(qos_id, lun_info['ID'])


class DeleteCacheTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteCacheTask, self).__init__(*args, **kwargs)
        self.smartcache = smartx.SmartCache(client)

    def execute(self, lun_info):
        cache_id = lun_info.get('SMARTCACHEPARTITIONID')
        if cache_id:
            self.smartcache.remove(cache_id, lun_info['ID'])


class DeletePartitionTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeletePartitionTask, self).__init__(*args, **kwargs)
        self.smartpartition = smartx.SmartPartition(client)

    def execute(self, lun_info):
        partition_id = lun_info.get('CACHEPARTITIONID')
        if partition_id:
            self.smartpartition.remove(partition_id, lun_info['ID'])


class DeleteLunTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteLunTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, lun_id):
        self.client.delete_lun(lun_id)


class CreateMigratedLunTask(task.Task):
    default_provides = ('tgt_lun_id', 'tgt_lun_info')

    def __init__(self, client, host, feature_support, *args, **kwargs):
        super(CreateMigratedLunTask, self).__init__(*args, **kwargs)
        self.client = client
        self.host = host
        self.feature_support = feature_support

    def execute(self, lun_info, opts=None):
        if not self.feature_support['SmartMigration']:
            msg = _("Huawei storage doesn't support SmartMigration.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_device = self.host['capabilities']['location_info']
        if target_device != self.client.device_id:
            msg = _("Migrate target %(tgt)s is not the same storage as "
                    "%(org)s.") % {'tgt': target_device,
                                   'org': self.client.device_id}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        pool_name = self.host['capabilities']['pool_name']
        pool_id = self.client.get_pool_id(pool_name)
        if not pool_id:
            msg = _("Pool %s doesn't exist in storage.") % pool_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if opts:
            new_lun_type = opts.get('LUNType')
            tier_policy = opts.get('policy')
        else:
            new_lun_type = None
            tier_policy = None

        lun_keys = ('DESCRIPTION', 'ALLOCTYPE', 'CAPACITY', 'WRITEPOLICY',
                    'PREFETCHPOLICY', 'PREFETCHVALUE', 'DATATRANSFERPOLICY',
                    'OWNINGCONTROLLER')
        lun_params = {k: lun_info[k] for k in lun_keys if k in lun_info}
        lun_params['NAME'] = lun_info['NAME'][:-4] + '-mig'
        lun_params['PARENTID'] = pool_id
        if new_lun_type:
            lun_params['ALLOCTYPE'] = new_lun_type
        if tier_policy:
            lun_params['DATATRANSFERPOLICY'] = tier_policy
        if lun_info.get("WORKLOADTYPENAME") and lun_info.get(
                "WORKLOADTYPEID"):
            lun_params["WORKLOADTYPEID"] = lun_info["WORKLOADTYPEID"]

        lun = self.client.create_lun(lun_params)
        return lun['ID'], lun

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_lun(result[0])


class CreateMigrateTask(task.Task):
    default_provides = 'migration_id'

    def __init__(self, client, *args, **kwargs):
        super(CreateMigrateTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, src_lun_id, tgt_lun_id):
        migration = self.client.create_lun_migration(src_lun_id, tgt_lun_id)
        return migration['ID']

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_lun_migration(result)


class WaitMigrateDoneTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(WaitMigrateDoneTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, migration_id, tgt_lun_id):
        def _migrate_done():
            migration = self.client.get_lun_migration(migration_id)
            if (migration['RUNNINGSTATUS'] in
                    constants.MIGRATION_STATUS_IN_PROCESS):
                return False
            elif (migration['RUNNINGSTATUS'] in
                    constants.MIGRATION_STATUS_COMPLETE):
                return True
            else:
                msg = _("Migration %s error.") % migration_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        huawei_utils.wait_for_condition(_migrate_done,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_TIMEOUT)
        self.client.delete_lun_migration(migration_id)
        self.client.delete_lun(tgt_lun_id)


class CheckSnapshotExistTask(task.Task):
    default_provides = ('snapshot_info', 'snapshot_id')

    def __init__(self, client, *args, **kwargs):
        super(CheckSnapshotExistTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot):
        snapshot_info = huawei_utils.get_snapshot_info(self.client, snapshot)
        if not snapshot_info:
            msg = _("Snapshot %s does not exist.") % snapshot.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return snapshot_info, snapshot_info['ID']


class GetSnapshotIDTask(task.Task):
    default_provides = 'snapshot_id'

    def __init__(self, client, *args, **kwargs):
        super(GetSnapshotIDTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot):
        snapshot_info = huawei_utils.get_snapshot_info(self.client, snapshot)
        if not snapshot_info:
            LOG.error("Snapshot %s does not exist.", snapshot.id)
            return None

        return snapshot_info['ID']


class CreateLunCopyTask(task.Task):
    default_provides = 'luncopy_id'

    def __init__(self, client, feature_support, configuration,
                 *args, **kwargs):
        super(CreateLunCopyTask, self).__init__(*args, **kwargs)
        self.client = client
        self.feature_support = feature_support
        self.configuration = configuration

    def execute(self, volume, snapshot_id, lun_id):
        if not self.feature_support['HyperCopy']:
            msg = _("Huawei storage doesn't support HyperCopy.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        copy_name = huawei_utils.encode_name(volume.id)
        metadata = huawei_utils.get_volume_private_data(volume)
        copyspeed = metadata.get('copyspeed')
        if not copyspeed:
            copyspeed = self.configuration.lun_copy_speed
        elif copyspeed not in constants.LUN_COPY_SPEED_TYPES:
            msg = (_("LUN copy speed is: %(speed)s. It should be between "
                     "%(low)s and %(high)s.")
                   % {"speed": copyspeed,
                      "low": constants.LUN_COPY_SPEED_LOW,
                      "high": constants.LUN_COPY_SPEED_HIGH})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        luncopy_id = self.client.create_luncopy(
            copy_name, snapshot_id, lun_id, copyspeed)
        return luncopy_id

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_luncopy(result)


class WaitLunCopyDoneTask(task.Task):
    def __init__(self, client, configuration, *args, **kwargs):
        super(WaitLunCopyDoneTask, self).__init__(*args, **kwargs)
        self.client = client
        self.configuration = configuration

    def execute(self, luncopy_id):
        self.client.start_luncopy(luncopy_id)

        def _luncopy_done():
            luncopy = self.client.get_luncopy_info(luncopy_id)
            if luncopy['HEALTHSTATUS'] != constants.STATUS_HEALTH:
                msg = _("Luncopy %s is abnormal.") % luncopy_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            return (luncopy['RUNNINGSTATUS'] in
                    constants.LUNCOPY_STATUS_COMPLETE)
        huawei_utils.wait_for_condition(
            _luncopy_done, self.configuration.lun_copy_wait_interval,
            self.configuration.lun_timeout)

        self.client.delete_luncopy(luncopy_id)


class CreateClonePairTask(task.Task):
    default_provides = 'clone_pair_id'

    def __init__(self, client, feature_support, configuration,
                 *args, **kwargs):
        super(CreateClonePairTask, self).__init__(*args, **kwargs)
        self.client = client
        self.feature_support = feature_support
        self.configuration = configuration

    def execute(self, source_id, target_id):
        clone_speed = self.configuration.lun_copy_speed
        clone_pair_id = self.client.create_clone_pair(
            source_id, target_id, clone_speed)
        return clone_pair_id

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_clone_pair(result)


class WaitClonePairDoneTask(task.Task):
    def __init__(self, client, configuration, *args, **kwargs):
        super(WaitClonePairDoneTask, self).__init__(*args, **kwargs)
        self.client = client
        self.configuration = configuration

    def execute(self, clone_pair_id):
        def _clone_pair_done():
            clone_pair_info = self.client.get_clone_pair_info(clone_pair_id)
            if clone_pair_info['copyStatus'] != constants.CLONE_STATUS_HEALTH:
                msg = _("ClonePair %s is abnormal.") % clone_pair_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            return (clone_pair_info['syncStatus'] in
                    constants.CLONE_STATUS_COMPLETE)

        self.client.sync_clone_pair(clone_pair_id)
        huawei_utils.wait_for_condition(
            _clone_pair_done, self.configuration.lun_copy_wait_interval,
            self.configuration.lun_timeout)
        self.client.delete_clone_pair(clone_pair_id)


class CreateLunCloneTask(task.Task):
    default_provides = 'lun_id', 'lun_info'

    def __init__(self, client, *args, **kwargs):
        super(CreateLunCloneTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, volume, src_id):
        name = huawei_utils.encode_name(volume.id)
        lun_info = self.client.create_lunclone(src_id, name)
        lun_id = lun_info["ID"]
        expected_size = int(volume.size) * constants.CAPACITY_UNIT
        try:
            if int(lun_info['CAPACITY']) < expected_size:
                self.client.extend_lun(lun_id, expected_size)
            self.client.split_lunclone(lun_id)
        except Exception:
            LOG.exception('Split clone lun %s error.', lun_id)
            self.client.delete_lun(lun_id)
            raise

        lun_info = self.client.get_lun_info_by_id(lun_id)
        return lun_info['ID'], lun_info


class LunClonePreCheckTask(task.Task):
    def __init__(self, *args, **kwargs):
        super(LunClonePreCheckTask, self).__init__(*args, **kwargs)

    @staticmethod
    def execute(volume, src_volume):
        if volume.volume_type_id != src_volume.volume_type_id:
            msg = _("Volume type must be the same as source "
                    "for fast clone.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)


class CreateSnapshotTask(task.Task):
    default_provides = 'snapshot_id'

    def __init__(self, client, feature_support, *args, **kwargs):
        super(CreateSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client
        self.feature_support = feature_support

    def execute(self, snapshot):
        if not self.feature_support['HyperSnap']:
            msg = _("Huawei storage doesn't support snapshot.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        lun_info = huawei_utils.get_lun_info(self.client, snapshot.volume)
        if not lun_info:
            msg = _("Source volume %s to create snapshot does not exist."
                    ) % snapshot.volume.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        name = huawei_utils.encode_name(snapshot.id)
        snapshot_info = self.client.create_snapshot(
            lun_info['ID'], name, snapshot.id)
        return snapshot_info['ID']

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_snapshot(result)


class CreateTempSnapshotTask(task.Task):
    default_provides = 'snapshot_id'

    def __init__(self, client, feature_support, *args, **kwargs):
        super(CreateTempSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client
        self.feature_support = feature_support

    def execute(self, src_id):
        if not self.feature_support['HyperSnap']:
            msg = _("Huawei storage doesn't support snapshot.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        snap_id = six.text_type(uuid.uuid4())
        name = huawei_utils.encode_name(snap_id)
        snapshot_info = self.client.create_snapshot(src_id, name, snap_id)
        return snapshot_info['ID']

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_snapshot(result)


class ActiveSnapshotTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(ActiveSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot_id):
        self.client.activate_snapshot(snapshot_id)

    def revert(self, snapshot_id):
        self.client.stop_snapshot(snapshot_id)


class WaitSnapshotReadyTask(task.Task):
    default_provides = 'snapshot_wwn'

    def __init__(self, client, *args, **kwargs):
        super(WaitSnapshotReadyTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot_id):
        def _snapshot_ready():
            self.snapshot = self.client.get_snapshot_info_by_id(snapshot_id)
            if self.snapshot['HEALTHSTATUS'] != constants.STATUS_HEALTH:
                msg = _("Snapshot %s is fault.") % snapshot_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            return not (self.snapshot['RUNNINGSTATUS'] ==
                        constants.SNAPSHOT_INITIALIZING)

        huawei_utils.wait_for_condition(_snapshot_ready,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_TIMEOUT)
        return self.snapshot['WWN']


class DeleteSnapshotTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot_info):
        if snapshot_info['RUNNINGSTATUS'] == constants.SNAPSHOT_ACTIVATED:
            self.client.stop_snapshot(snapshot_info['ID'])
        self.client.delete_snapshot(snapshot_info['ID'])


class DeleteTempSnapshotTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteTempSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot_id):
        self.client.stop_snapshot(snapshot_id)
        self.client.delete_snapshot(snapshot_id)


class RevertToSnapshotTask(task.Task):
    def __init__(self, client, rollback_speed, *args, **kwargs):
        super(RevertToSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client
        self.rollback_speed = rollback_speed

    def execute(self, snapshot_info, snapshot_id):
        running_status = snapshot_info.get("RUNNINGSTATUS")
        health_status = snapshot_info.get("HEALTHSTATUS")

        if running_status not in (
                constants.SNAPSHOT_RUNNING_STATUS_ACTIVATED,
                constants.SNAPSHOT_RUNNING_STATUS_ROLLINGBACK):
            err_msg = (_("The running status %(status)s of snapshot %(name)s.")
                       % {"status": running_status, "name": snapshot_id})
            LOG.error(err_msg)
            raise exception.InvalidSnapshot(reason=err_msg)

        if health_status not in (constants.SNAPSHOT_HEALTH_STATUS_NORMAL,):
            err_msg = (_("The health status %(status)s of snapshot %(name)s.")
                       % {"status": running_status, "name": snapshot_id})
            LOG.error(err_msg)
            raise exception.InvalidSnapshot(reason=err_msg)

        if constants.SNAPSHOT_RUNNING_STATUS_ACTIVATED == snapshot_info.get(
                'RUNNINGSTATUS'):
            self.client.rollback_snapshot(snapshot_id, self.rollback_speed)

    def revert(self, result, snapshot_id, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.cancel_rollback_snapshot(snapshot_id)


class WaitSnapshotRollbackDoneTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(WaitSnapshotRollbackDoneTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot_id):
        def _snapshot_rollback_finish():
            snapshot_info = self.client.get_snapshot_info_by_id(snapshot_id)

            if snapshot_info.get('HEALTHSTATUS') not in (
                    constants.SNAPSHOT_HEALTH_STATUS_NORMAL,):
                msg = _("The snapshot %s is abnormal.") % snapshot_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if (snapshot_info.get('ROLLBACKRATE') ==
                    constants.SNAPSHOT_ROLLBACK_PROGRESS_FINISH or
                    snapshot_info.get('ROLLBACKENDTIME') != '-1'):
                LOG.info("Snapshot %s rollback successful.", snapshot_id)
                return True
            return False

        huawei_utils.wait_for_condition(_snapshot_rollback_finish,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_TIMEOUT)


class ExtendVolumeTask(task.Task):
    default_provides = 'lun_info'

    def __init__(self, client, *args, **kwargs):
        super(ExtendVolumeTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, lun_id, new_size):
        lun_info = self.client.get_lun_info_by_id(lun_id)
        if int(lun_info['CAPACITY']) < new_size:
            self.client.extend_lun(lun_id, new_size)
            LOG.info('Extend LUN %(id)s to size %(new_size)s.',
                     {'id': lun_id,
                      'new_size': new_size})
            lun_info = self.client.get_lun_info_by_id(lun_id)
        return lun_info


class ExtendHyperMetroTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(ExtendHyperMetroTask, self).__init__(*args, **kwargs)
        self.hypermetro = hypermetro.HuaweiHyperMetro(
            local_cli, remote_cli, config)
        self.local_cli = local_cli

    def execute(self, volume, new_size):
        metadata = huawei_utils.get_volume_private_data(volume)
        if not metadata.get('hypermetro'):
            return

        hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
        if not hypermetro:
            msg = _('Volume %s is not in hypermetro pair') % volume.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self.hypermetro.extend_hypermetro(hypermetro['ID'], new_size)


class ExtendReplicationTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(ExtendReplicationTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)

    def execute(self, volume, new_size):
        data = huawei_utils.get_replication_data(volume)
        pair_id = data.get('pair_id')
        if pair_id:
            self.replication.extend_replica(pair_id, new_size)


class UpdateLunTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(UpdateLunTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, lun_info, opts):
        data = {}
        compression_check = lun_info.get('ENABLECOMPRESSION') == 'true'
        if not opts['compression'] and compression_check:
            data["ENABLECOMPRESSION"] = 'false'

        dedup_check = lun_info.get('ENABLESMARTDEDUP') == 'true'
        if not opts['dedup'] and dedup_check:
            data["ENABLESMARTDEDUP"] = 'false'

        if (opts.get('policy') and
                opts['policy'] != lun_info.get('DATATRANSFERPOLICY')):
            data["DATATRANSFERPOLICY"] = opts['policy']

        if data:
            self.client.update_lun(lun_info['ID'], data)


class UpdateQoSTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(UpdateQoSTask, self).__init__(*args, **kwargs)
        self.client = client
        self.smartqos = smartx.SmartQos(client)

    def execute(self, lun_info, opts):
        qos_id = lun_info.get('IOCLASSID')
        if opts.get('qos'):
            if qos_id:
                self.smartqos.update(qos_id, opts['qos'], lun_info['ID'])
            else:
                self.smartqos.add(opts['qos'], lun_info['ID'])
        elif qos_id:
            self.smartqos.remove(qos_id, lun_info['ID'])


class UpdateCacheTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(UpdateCacheTask, self).__init__(*args, **kwargs)
        self.smartcache = smartx.SmartCache(client)

    def execute(self, lun_info, opts):
        cache_id = lun_info.get('SMARTCACHEPARTITIONID')
        if opts.get('smartcache'):
            if cache_id:
                self.smartcache.update(
                    cache_id, opts['cachename'], lun_info['ID'])
            else:
                self.smartcache.add(opts['cachename'], lun_info['ID'])
        elif cache_id:
            self.smartcache.remove(cache_id, lun_info['ID'])


class UpdatePartitionTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(UpdatePartitionTask, self).__init__(*args, **kwargs)
        self.smartpartition = smartx.SmartPartition(client)

    def execute(self, lun_info, opts):
        partition_id = lun_info.get('CACHEPARTITIONID')
        if opts.get('smartpartition'):
            if partition_id:
                self.smartpartition.update(
                    partition_id, opts['partitionname'], lun_info['ID'])
            else:
                self.smartpartition.add(opts['partitionname'], lun_info['ID'])
        elif partition_id:
            self.smartpartition.remove(partition_id, lun_info['ID'])


class ManageVolumePreCheckTask(task.Task):
    default_provides = ('lun_info', 'lun_id')

    def __init__(self, client, volume, existing_ref, configuration,
                 *args, **kwargs):
        super(ManageVolumePreCheckTask, self).__init__(*args, **kwargs)
        self.client = client
        self.volume = volume
        self.existing_ref = existing_ref
        self.configuration = configuration

    def _get_external_lun(self):
        lun_info = huawei_utils.get_external_lun_info(
            self.client, self.existing_ref)
        if not lun_info:
            msg = _('External lun %s not exist.') % self.existing_ref
            LOG.error(msg)
            raise exception.ManageExistingInvalidReference(
                existing_ref=self.existing_ref, reason=msg)

        return lun_info

    def _check_lun_abnormal(self, lun_info, *args):
        return lun_info['HEALTHSTATUS'] != constants.STATUS_HEALTH

    def _check_pool_inconsistency(self, lun_info, *args):
        pool = volume_utils.extract_host(self.volume.host, 'pool')
        return pool != lun_info['PARENTNAME']

    def _check_lun_in_use(self, lun_info, *args):
        return (lun_info.get('ISADD2LUNGROUP') == 'true' or
                lun_info.get('EXPOSEDTOINITIATOR') == 'true')

    def _check_lun_in_hypermetro(self, lun_info, *args):
        rss = {}
        if 'HASRSSOBJECT' in lun_info:
            rss = json.loads(lun_info['HASRSSOBJECT'])
        return rss.get('HyperMetro') == 'TRUE'

    def _check_lun_in_replication(self, lun_info, *args):
        rss = {}
        if 'HASRSSOBJECT' in lun_info:
            rss = json.loads(lun_info['HASRSSOBJECT'])
        return rss.get('RemoteReplication') == 'TRUE'

    def _check_lun_in_splitmirror(self, lun_info, *args):
        rss = {}
        if 'HASRSSOBJECT' in lun_info:
            rss = json.loads(lun_info['HASRSSOBJECT'])
        return rss.get('SplitMirror') == 'TRUE'

    def _check_lun_in_hypermirror(self, lun_info, *args):
        rss = {}
        if 'HASRSSOBJECT' in lun_info:
            rss = json.loads(lun_info['HASRSSOBJECT'])
        return rss.get('LUNMirror') == 'TRUE'

    def _check_lun_in_luncopy(self, lun_info, *args):
        rss = {}
        if 'HASRSSOBJECT' in lun_info:
            rss = json.loads(lun_info['HASRSSOBJECT'])
        return rss.get('LunCopy') == 'TRUE'

    def _check_lun_in_migration(self, lun_info, *args):
        rss = {}
        if 'HASRSSOBJECT' in lun_info:
            rss = json.loads(lun_info['HASRSSOBJECT'])
        return rss.get('LunMigration') == 'TRUE'

    def _check_lun_not_common(self, lun_info, *args):
        return (lun_info.get('MIRRORTYPE') != '0' or
                lun_info.get('SUBTYPE') != '0')

    def _check_lun_consistency(self, lun_info, opts):
        return ('LUNType' in opts and
                opts['LUNType'] != lun_info['ALLOCTYPE'])

    def _check_lun_dedup_consistency(self, lun_info, opts):
        dedup_flag = False
        if opts.get('dedup') is not None:
            dedup_enabled = lun_info['ENABLESMARTDEDUP'] == 'true'
            if opts['dedup'] != dedup_enabled:
                dedup_flag = True
        return dedup_flag

    def _check_lun_compresison_consistency(self, lun_info, opts):
        compression_flag = False
        if opts.get('compression') is not None:
            compression_enabled = lun_info['ENABLECOMPRESSION'] == 'true'
            if opts['compression'] != compression_enabled:
                compression_flag = True
        return compression_flag

    def execute(self, opts):
        lun_info = self._get_external_lun()

        for i in dir(self):
            if callable(getattr(self, i)) and i.startswith('_check_'):
                func = getattr(self, i)
                if func(lun_info, opts):
                    msg = _("Volume managing pre check %s failed."
                            ) % func.__name__
                    LOG.error(msg)
                    raise exception.ManageExistingInvalidReference(
                        existing_ref=self.existing_ref, reason=msg)

        return lun_info, lun_info['ID']


class ManageLunTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(ManageLunTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, volume, lun_info):
        new_name = huawei_utils.encode_name(volume.id)
        self.client.rename_lun(lun_info['ID'], new_name, volume.name)

    def revert(self, result, lun_info, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.rename_lun(lun_info['ID'], lun_info['NAME'],
                               lun_info['DESCRIPTION'])


class ManageSnapshotPreCheckTask(task.Task):
    default_provides = 'snapshot_info'

    def __init__(self, client, snapshot, existing_ref, *args, **kwargs):
        super(ManageSnapshotPreCheckTask, self).__init__(*args, **kwargs)
        self.client = client
        self.snapshot = snapshot
        self.existing_ref = existing_ref

    def _get_external_snapshot(self):
        snapshot_info = huawei_utils.get_external_snapshot_info(
            self.client, self.existing_ref)
        if not snapshot_info:
            msg = _('External snapshot %s not exist.') % self.existing_ref
            LOG.error(msg)
            raise exception.ManageExistingInvalidReference(
                existing_ref=self.existing_ref, reason=msg)

        return snapshot_info

    def _check_snapshot_abnormal(self, snapshot_info):
        return snapshot_info['HEALTHSTATUS'] != constants.STATUS_HEALTH

    def _check_snapshot_in_use(self, snapshot_info):
        return snapshot_info.get('EXPOSEDTOINITIATOR') == 'true'

    def _check_parent_volume_inconsistency(self, snapshot_info):
        parent_info = huawei_utils.get_lun_info(
            self.client, self.snapshot.volume)
        return (not parent_info or
                snapshot_info.get('PARENTID') != parent_info['ID'])

    def execute(self):
        snapshot_info = self._get_external_snapshot()
        for i in dir(self):
            if callable(getattr(self, i)) and i.startswith('_check_'):
                func = getattr(self, i)
                if func(snapshot_info):
                    msg = _("Snapshot managing pre check %s failed."
                            ) % func.__name__
                    LOG.error(msg)
                    raise exception.ManageExistingInvalidReference(
                        existing_ref=self.existing_ref, reason=msg)

        return snapshot_info


class ManageSnapshotTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(ManageSnapshotTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, snapshot, snapshot_info):
        new_name = huawei_utils.encode_name(snapshot.id)
        data = {'NAME': new_name}
        self.client.update_snapshot(snapshot_info['ID'], data)

        if (snapshot_info.get('RUNNINGSTATUS') ==
                constants.SNAPSHOT_UNACTIVATED):
            self.client.activate_snapshot(snapshot_info['ID'])


class GroupOptsCheckTask(task.Task):
    default_provides = 'opts'

    def __init__(self, *args, **kwargs):
        super(GroupOptsCheckTask, self).__init__(*args, **kwargs)

    def execute(self, opts):
        for opt in opts:
            huawei_utils.check_volume_type_valid(opt)
        return opts


class CreateHyperMetroGroupTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, feature_support,
                 *args, **kwargs):
        super(CreateHyperMetroGroupTask, self).__init__(*args, **kwargs)
        self.hypermetro = hypermetro.HuaweiHyperMetro(
            local_cli, remote_cli, config)
        self.feature_support = feature_support

    def execute(self, group, opts):
        if any(opt for opt in opts if opt['hypermetro']):
            if not self.feature_support['HyperMetro']:
                msg = _("Huawei storage doesn't support HyperMetro.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            self.hypermetro.create_consistencygroup(group.id)

    def revert(self, result, group, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.hypermetro.delete_consistencygroup(group.id, [])


class CreateReplicationGroupTask(task.Task):
    def __init__(self, local_cli, remote_cli, config, feature_support,
                 *args, **kwargs):
        super(CreateReplicationGroupTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)
        self.feature_support = feature_support

    def execute(self, group, opts):
        create_group = False
        replication_type = set()
        for opt in opts:
            if opt['replication_enabled']:
                create_group = True
                replication_type.add(opt['replication_type'])

        if create_group:
            if not self.feature_support['HyperReplication']:
                msg = _("Huawei storage doesn't support HyperReplication.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            if len(replication_type) != 1:
                msg = _("Multiple replication types exist in group.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            self.replication.create_group(group.id, replication_type.pop())

    def revert(self, result, group, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.replication.delete_group(group.id, [])


class GetISCSIConnectionTask(task.Task):
    default_provides = ('target_ips', 'target_iqns', 'target_eths',
                        'config_info')

    def __init__(self, client, iscsi_info, *args, **kwargs):
        super(GetISCSIConnectionTask, self).__init__(*args, **kwargs)
        self.client = client
        self.iscsi_info = iscsi_info

    def _get_config_target_ips(self, ini):
        if ini and ini.get('TargetIP'):
            target_ips = [ip.strip() for ip in ini['TargetIP'].split()
                          if ip.strip()]
        else:
            target_ips = self.iscsi_info['default_target_ips']
        return target_ips

    def _get_port_ip(self, port_id):
        iqn_info = port_id.split(',', 1)[0]
        return iqn_info.split(':', 5)[5]

    def _get_port_iqn(self, port_id):
        iqn_info = port_id.split(',', 1)[0]
        return iqn_info.split('+')[1]

    def execute(self, connector):
        ip_iqn_map = {}
        target_ports = self.client.get_iscsi_tgt_ports()
        for port in target_ports:
            ip = self._get_port_ip(port['ID'])
            normalized_ip = ipaddress.ip_address(six.text_type(ip)).exploded
            ip_iqn_map[normalized_ip] = (port['ID'], port['ETHPORTID'])

        config_info = huawei_utils.find_config_info(self.iscsi_info,
                                                    connector=connector)

        config_ips = self._get_config_target_ips(config_info)
        LOG.info('Configured iscsi ips %s.', config_ips)

        target_ips = []
        target_iqns = []
        target_eths = []

        for ip in config_ips:
            ip_addr = ipaddress.ip_address(six.text_type(ip))
            normalized_ip = ip_addr.exploded
            if normalized_ip in ip_iqn_map:
                iqn = self._get_port_iqn(ip_iqn_map[normalized_ip][0])
                target_iqns.append(iqn)
                target_eths.append(ip_iqn_map[normalized_ip][1])

        for iqn in target_iqns:
            ip = iqn.split(':', 5)[5]
            if ipaddress.ip_address(six.text_type(ip)).version == 6:
                ip = '[' + ip + ']'
            target_ips.append(ip)

        if not target_ips or not target_iqns or not target_eths:
            msg = _('Get iSCSI target ip&iqn&eth error.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.info('Get iscsi target_ips: %s, target_iqns: %s, target_eths: %s.',
                 target_ips, target_iqns, target_eths)

        return target_ips, target_iqns, target_eths, config_info


class CreateHostTask(task.Task):
    default_provides = 'host_id'

    def __init__(self, client, iscsi_info, configuration, *args, **kwargs):
        super(CreateHostTask, self).__init__(*args, **kwargs)
        self.client = client
        self.iscsi_info = iscsi_info
        self.configuration = configuration

    def _get_new_alua_info(self, config):
        info = {'accessMode': '0'}
        if config.get('ACCESSMODE') and config.get('HYPERMETROPATHOPTIMIZED'):
            info.update({
                'accessMode': config['ACCESSMODE'],
                'hyperMetroPathOptimized': config['HYPERMETROPATHOPTIMIZED']
            })

        return info

    def execute(self, connector):
        orig_host_name = connector['host']
        host_id = huawei_utils.get_host_id(self.client, orig_host_name)
        info = {}
        if self.configuration.is_dorado_v6:
            config_info = huawei_utils.find_config_info(
                self.iscsi_info, connector=connector)
            info = self._get_new_alua_info(config_info)
        if host_id:
            self.client.update_host(host_id, info)
        if not host_id:
            host_name = huawei_utils.encode_host_name(orig_host_name)
            host_id = self.client.create_host(host_name, orig_host_name, info)
        return host_id


class AddISCSIInitiatorTask(task.Task):
    default_provides = 'chap_info'

    def __init__(self, client, iscsi_info, configuration, *args, **kwargs):
        super(AddISCSIInitiatorTask, self).__init__(*args, **kwargs)
        self.client = client
        self.iscsi_info = iscsi_info
        self.configuration = configuration

    def _get_chap_info(self, config):
        chap_config = config.get('CHAPinfo')
        if not chap_config:
            return {}

        chap_name, chap_password = chap_config.split(';')
        return {'CHAPNAME': chap_name,
                'CHAPPASSWORD': chap_password}

    def _get_alua_info(self, config):
        alua_info = {'MULTIPATHTYPE': '0'}
        if config.get('ACCESSMODE') and self.configuration.is_dorado_v6:
            return alua_info

        if config.get('ALUA'):
            alua_info['MULTIPATHTYPE'] = config['ALUA']

        if alua_info['MULTIPATHTYPE'] == '1':
            for k in ('FAILOVERMODE', 'SPECIALMODETYPE', 'PATHTYPE'):
                if config.get(k):
                    alua_info[k] = config[k]

        return alua_info

    def execute(self, connector, host_id, config_info):
        initiator = connector['initiator']
        self.client.add_iscsi_initiator(initiator)

        alua_info = self._get_alua_info(config_info)
        self.client.associate_iscsi_initiator_to_host(
            initiator, host_id, alua_info)

        chap_info = self._get_chap_info(config_info)
        ini_info = self.client.get_iscsi_initiator(initiator)
        if (ini_info['USECHAP'] == 'true' and not chap_info) or (
                ini_info['USECHAP'] == 'false' and chap_info):
            self.client.update_iscsi_initiator_chap(initiator, chap_info)

        return chap_info


class CreateHostGroupTask(task.Task):
    default_provides = 'hostgroup_id'

    def __init__(self, client, *args, **kwargs):
        super(CreateHostGroupTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_id):
        hostgroup_name = constants.HOSTGROUP_PREFIX + host_id
        hostgroup_id = self.client.create_hostgroup(hostgroup_name)
        self.client.associate_host_to_hostgroup(hostgroup_id, host_id)
        return hostgroup_id


class CreateLunGroupTask(task.Task):
    default_provides = 'lungroup_id'

    def __init__(self, client, configuration, *args, **kwargs):
        super(CreateLunGroupTask, self).__init__(*args, **kwargs)
        self.client = client
        self.configuration = configuration

    def execute(self, host_id, lun_id, lun_type):
        lungroup_name = constants.LUNGROUP_PREFIX + host_id
        lungroup_id = self.client.create_lungroup(lungroup_name)
        mapping_view = self.client.get_mappingview_by_lungroup_id(lungroup_id)
        is_associated_host = True if mapping_view else False
        self.client.associate_lun_to_lungroup(
            lungroup_id, lun_id, lun_type,
            self.configuration.is_dorado_v6, is_associated_host)
        return lungroup_id

    def revert(self, result, lun_id, lun_type, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.remove_lun_from_lungroup(result, lun_id, lun_type)


class CreateMappingViewTask(task.Task):
    default_provides = ('mappingview_id', 'hostlun_id', 'aval_host_lun_ids')

    def __init__(self, client, *args, **kwargs):
        super(CreateMappingViewTask, self).__init__(*args, **kwargs)
        self.client = client

    def _get_hostlun_id(self, func, host_id, lun_id):
        hostlun_id = func(host_id, lun_id)
        if hostlun_id is None:
            import time
            time.sleep(3)
            hostlun_id = func(host_id, lun_id)

        if hostlun_id is None:
            msg = _("Can not get hostlun id. Maybe the storage is busy, "
                    "Please try it later")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return hostlun_id

    def execute(self, lun_id, lun_type, host_id, hostgroup_id, lungroup_id,
                lun_info, portgroup_id=None):
        mappingview_name = constants.MAPPING_VIEW_PREFIX + host_id
        mappingview_id = self.client.create_mappingview(mappingview_name)
        self.client.associate_hostgroup_to_mappingview(
            mappingview_id, hostgroup_id)
        self.client.associate_lungroup_to_mappingview(
            mappingview_id, lungroup_id)
        if portgroup_id:
            self.client.associate_portgroup_to_mappingview(
                mappingview_id, portgroup_id)

        if lun_type == constants.LUN_TYPE:
            hostlun_id = self._get_hostlun_id(
                self.client.get_lun_host_lun_id, host_id, lun_info)
        else:
            hostlun_id = self._get_hostlun_id(
                self.client.get_snapshot_host_lun_id, host_id, lun_id)

        mappingview_info = self.client.get_mappingview_by_id(mappingview_id)
        aval_host_lun_ids = json.loads(
            mappingview_info['AVAILABLEHOSTLUNIDLIST'])
        return mappingview_id, hostlun_id, aval_host_lun_ids


class GetISCSIPropertiesTask(task.Task):
    default_provides = 'mapping_info'

    def execute(self, connector, hostlun_id, target_iqns, target_ips,
                chap_info, mappingview_id, aval_host_lun_ids, lun_id,
                lun_info):
        hostlun_id = int(hostlun_id)
        mapping_info = {
            'target_discovered': False,
            'hostlun_id': hostlun_id,
            'mappingview_id': mappingview_id,
            'aval_host_lun_ids': aval_host_lun_ids,
            'lun_id': lun_id,
        }

        if connector.get('multipath'):
            mapping_info.update({
                'target_iqns': target_iqns,
                'target_portals': ['%s:3260' % ip for ip in target_ips],
                'target_luns': [hostlun_id] * len(target_ips),
            })
        else:
            mapping_info.update({
                'target_iqn': target_iqns[0],
                'target_portal': '%s:3260' % target_ips[0],
                'target_lun': hostlun_id,
            })

        if chap_info:
            mapping_info['auth_method'] = 'CHAP'
            mapping_info['auth_username'] = chap_info['CHAPNAME']
            mapping_info['auth_password'] = chap_info['CHAPPASSWORD']

        if lun_info.get('ALLOCTYPE') == constants.THIN_LUNTYPE:
            mapping_info['discard'] = True

        return mapping_info


class GetHyperMetroRemoteLunTask(task.Task):
    default_provides = ('lun_id', 'lun_info')

    def __init__(self, client, hypermetro_id, *args, **kwargs):
        super(GetHyperMetroRemoteLunTask, self).__init__(*args, **kwargs)
        self.client = client
        self.hypermetro_id = hypermetro_id

    def execute(self):
        hypermetro_info = self.client.get_hypermetro_by_id(self.hypermetro_id)
        remote_lun_id = hypermetro_info['LOCALOBJID']
        remote_lun_info = self.client.get_lun_info_by_id(remote_lun_id)
        return remote_lun_id, remote_lun_info


class GetLunMappingTask(task.Task):
    default_provides = ('mappingview_id', 'lungroup_id', 'hostgroup_id',
                        'portgroup_id', 'host_id')

    def __init__(self, client, *args, **kwargs):
        super(GetLunMappingTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, connector, lun_id):
        if connector is None or 'host' not in connector:
            mappingview_id, lungroup_id, hostgroup_id, portgroup_id, host_id = (
                huawei_utils.get_mapping_info(self.client, lun_id))
            return (mappingview_id, lungroup_id, hostgroup_id, portgroup_id,
                    host_id)
        host_name = connector['host']
        host_id = huawei_utils.get_host_id(self.client, host_name)
        if not host_id:
            LOG.warning('Host %s not exist, return success for '
                        'connection termination.', host_name)
            return None, None, None, None, None

        mappingview_name = constants.MAPPING_VIEW_PREFIX + host_id
        mappingview = self.client.get_mappingview_by_name(mappingview_name)
        if not mappingview:
            LOG.warning('Mappingview %s not exist, return success for '
                        'connection termination.', mappingview_name)
            return None, None, None, None, host_id

        lungroup_id = self.client.get_lungroup_in_mappingview(
            mappingview['ID'])
        portgroup_id = self.client.get_portgroup_in_mappingview(
            mappingview['ID'])
        hostgroup_id = self.client.get_hostgroup_in_mappingview(
            mappingview['ID'])

        return (mappingview['ID'], lungroup_id, hostgroup_id, portgroup_id,
                host_id)


class ClearLunMappingTask(task.Task):
    default_provides = 'ini_tgt_map'

    def __init__(self, client, configuration, fc_san=None, *args, **kwargs):
        super(ClearLunMappingTask, self).__init__(*args, **kwargs)
        self.client = client
        self.fc_san = fc_san
        self.configuration = configuration

    def _get_obj_count_of_lungroup(self, lungroup_id):
        lun_count = self.client.get_lun_count_of_lungroup(lungroup_id)
        snap_count = self.client.get_snapshot_count_of_lungroup(lungroup_id)
        return lun_count + snap_count

    def _delete_portgroup(self, mappingview_id, portgroup_id):
        self.client.remove_portgroup_from_mappingview(
            mappingview_id, portgroup_id)

        eth_ports = self.client.get_eth_ports_in_portgroup(portgroup_id)
        fc_ports = self.client.get_fc_ports_in_portgroup(portgroup_id)
        for p in [p['ID'] for p in eth_ports] + [p['ID'] for p in fc_ports]:
            self.client.remove_port_from_portgroup(portgroup_id, p)
        self.client.delete_portgroup(portgroup_id)

    def _delete_lungroup(self, mappingview_id, lungroup_id):
        self.client.remove_lungroup_from_mappingview(
            mappingview_id, lungroup_id)
        self.client.delete_lungroup(lungroup_id)

    def _delete_hostgroup(self, mappingview_id, hostgroup_id, host_id):
        self.client.remove_hostgroup_from_mappingview(
            mappingview_id, hostgroup_id)
        self.client.remove_host_from_hostgroup(hostgroup_id, host_id)
        self.client.delete_hostgroup(hostgroup_id)

    def _delete_host(self, host_id):
        iscsi_initiators = self.client.get_host_iscsi_initiators(host_id)
        for ini in iscsi_initiators:
            self.client.remove_iscsi_initiator_from_host(ini)

        fc_initiators = self.client.get_host_fc_initiators(host_id)
        for ini in fc_initiators:
            self.client.remove_fc_initiator_from_host(ini)

        self.client.delete_host(host_id)

    def _get_ini_tgt_map(self, connector, host_id):
        ini_tgt_map = {}
        portgroup = self.client.get_portgroup_by_name(
            constants.PORTGROUP_PREFIX + host_id)
        if portgroup:
            ports = self.client.get_fc_ports_in_portgroup(portgroup['ID'])
            port_wwns = [p['WWN'] for p in ports]
            wwns = map(lambda x: x.lower(), connector['wwpns'])
            for wwn in wwns:
                ini_tgt_map[wwn] = port_wwns

        return ini_tgt_map

    def execute(self, connector, lun_id, lun_type, host_id, mappingview_id,
                lungroup_id, hostgroup_id, portgroup_id):
        obj_count = 0
        if lun_id and lungroup_id:
            self.client.remove_lun_from_lungroup(lungroup_id, lun_id, lun_type)
            obj_count = self._get_obj_count_of_lungroup(lungroup_id)

        # If lungroup still has member objects, don't clear mapping relation.
        if obj_count > 0:
            LOG.info('Lungroup %(lg)s still has %(count)s members.',
                     {'lg': lungroup_id, 'count': obj_count})
            return {}
        if self.configuration.retain_storage_mapping:
            return {}

        ini_tgt_map = {}
        if self.fc_san and host_id:
            ini_tgt_map = self._get_ini_tgt_map(connector, host_id)

        if mappingview_id and portgroup_id:
            self._delete_portgroup(mappingview_id, portgroup_id)
        if mappingview_id and not self.fc_san:
            self.client.update_iscsi_initiator_chap(
                connector.get('initiator'), chap_info=None)
        if mappingview_id and lungroup_id:
            self._delete_lungroup(mappingview_id, lungroup_id)
        if mappingview_id and hostgroup_id:
            self._delete_hostgroup(mappingview_id, hostgroup_id, host_id)
        if mappingview_id:
            self.client.delete_mapping_view(mappingview_id)
        if host_id and not self.client.is_host_associate_inband_lun(host_id):
            self._delete_host(host_id)

        return ini_tgt_map


class GetFCConnectionTask(task.Task):
    default_provides = ('ini_tgt_map', 'tgt_port_wwns')

    def __init__(self, client, fc_san, configuration, *args, **kwargs):
        super(GetFCConnectionTask, self).__init__(*args, **kwargs)
        self.client = client
        self.fc_san = fc_san
        self.configuration = configuration

    def _get_fc_ports(self, wwns):
        contr_map = {}
        slot_map = {}
        port_map = {}

        fc_ports = self.client.get_fc_ports()
        for port in fc_ports:
            if port['RUNNINGSTATUS'] == constants.FC_PORT_CONNECTED:
                contr = port['PARENTID'].split('.')[0]
                slot = port['PARENTID']
                port_wwn = port['WWN']

                if contr not in contr_map:
                    contr_map[contr] = [slot]
                elif slot not in contr_map[contr]:
                    contr_map[contr].append(slot)

                if slot not in slot_map:
                    slot_map[slot] = [port_wwn]
                elif port_wwn not in slot_map[slot]:
                    slot_map[slot].append(port_wwn)

                port_map[port_wwn] = {
                    'id': port['ID'],
                    'runspeed': int(port['RUNSPEED']),
                    'slot': slot,
                }

        fabrics = self._get_fabric(wwns, list(port_map.keys()))
        if not fabrics:
            msg = _("No valid fabric connection..")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return contr_map, slot_map, port_map, fabrics

    def _get_fabric(self, ini_port_wwns, tgt_port_wwns):
        ini_tgt_map = self.fc_san.get_device_mapping_from_network(
            ini_port_wwns, tgt_port_wwns)

        def _filter_not_connected_fabric(fabric_name, fabric):
            ini_port_wwn_list = fabric.get('initiator_port_wwn_list')
            tgt_port_wwn_list = fabric.get('target_port_wwn_list')

            if not ini_port_wwn_list or not tgt_port_wwn_list:
                LOG.warning("Fabric %(fabric_name)s doesn't really "
                            "connect host and array: %(fabric)s.",
                            {'fabric_name': fabric_name,
                             'fabric': fabric})
                return None

            return set(ini_port_wwn_list), set(tgt_port_wwn_list)

        valid_fabrics = []
        for fabric in ini_tgt_map:
            pair = _filter_not_connected_fabric(fabric, ini_tgt_map[fabric])
            if pair:
                valid_fabrics.append(pair)

        LOG.info("Got fabric: %s.", valid_fabrics)
        return valid_fabrics

    def _count_port_weight(self, port):
        port_bandwidth = port['runspeed']
        portgroup_ids = self.client.get_portgroup_by_port_id(port['id'], 212)
        weight = 1.0 / port_bandwidth if port_bandwidth > 0 else 1.0

        return len(portgroup_ids), weight

    def _select_port_per_fabric(self, port_map, candid_ports, used_slots):
        used_slot_pairs = []
        other_slot_pairs = []
        for p in candid_ports:
            weight = self._count_port_weight(port_map[p])

            if port_map[p]['slot'] in used_slots:
                used_slot_pairs.append((weight, p))
            else:
                other_slot_pairs.append((weight, p))

        new_port = None
        if other_slot_pairs:
            sorted_pairs = sorted(other_slot_pairs, key=lambda a: a[0])
            new_port = sorted_pairs[0][1]
        if not new_port and used_slot_pairs:
            sorted_pairs = sorted(used_slot_pairs, key=lambda a: a[0])
            new_port = sorted_pairs[0][1]

        return new_port

    def _select_ports_per_contr(self, fabrics, slots, slot_map, port_map):
        contr_ports = set()
        for slot in slots:
            contr_ports.update(slot_map[slot])

        if len(fabrics) == 1:
            select_fabrics = fabrics * 2
        else:
            select_fabrics = fabrics

        used_slots = set()
        selected_ports = set()
        for fabric in select_fabrics:
            new_port = self._select_port_per_fabric(
                port_map, fabric[1] & contr_ports, used_slots)
            if new_port:
                selected_ports.add(new_port)
                used_slots.add(port_map[new_port]['slot'])

        return selected_ports

    def _get_ports_in_use(self, host_id):
        portgroup = self.client.get_portgroup_by_name(
            constants.PORTGROUP_PREFIX + host_id)
        if not portgroup:
            return []
        ports = self.client.get_fc_ports_in_portgroup(portgroup['ID'])
        return [p['WWN'] for p in ports]

    def _get_fc_zone(self, wwns, host_id):
        selected_ports = set()
        ini_tgt_map = {}

        used_ports = self._get_ports_in_use(host_id)
        if not used_ports:
            contr_map, slot_map, port_map, fabrics = self._get_fc_ports(wwns)
            for contr in contr_map:
                ports = self._select_ports_per_contr(
                    fabrics, contr_map[contr], slot_map, port_map)
                selected_ports.update(ports)

            for fabric in fabrics:
                for ini in fabric[0]:
                    ini_tgt_map[ini] = list(selected_ports & fabric[1])

        return ini_tgt_map, list(selected_ports) + used_ports

    def _get_divided_wwns(self, wwns, host_id):
        invalid_wwns, effective_wwns = [], []
        for wwn in wwns:
            wwn_info = self.client.get_fc_init_info(wwn)
            if not wwn_info:
                LOG.info("%s is not found in device, ignore it.", wwn)
                continue

            if wwn_info.get('RUNNINGSTATUS') == constants.FC_INIT_ONLINE:
                if wwn_info.get('ISFREE') == 'true':
                    effective_wwns.append(wwn)
                    continue

                if wwn_info.get('PARENTTYPE') == constants.PARENT_TYPE_HOST \
                        and wwn_info.get('PARENTID') == host_id:
                    effective_wwns.append(wwn)
                    continue

            invalid_wwns.append(wwn)

        return invalid_wwns, effective_wwns

    def _get_fc_link(self, wwns, host_id):
        invalid_wwns, effective_wwns = self._get_divided_wwns(wwns, host_id)

        if invalid_wwns:
            if (self.configuration.min_fc_ini_online ==
                    constants.DEFAULT_MINIMUM_FC_INITIATOR_ONLINE):
                msg = _("There are invalid initiators %s. If you want to "
                        "continue to attach volume to host, configure "
                        "MinFCIniOnline in the XML file.") % invalid_wwns
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if len(effective_wwns) < self.configuration.min_fc_ini_online:
            msg = (("The number of online fc initiator %(wwns)s less than"
                    " the set number: %(set)s.")
                   % {"wwns": effective_wwns,
                      "set": self.configuration.min_fc_ini_online})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        ini_tgt_map = {}
        tgt_port_wwns = set()

        for ini in effective_wwns:
            tgts = self.client.get_fc_target_wwpns(ini)
            ini_tgt_map[ini] = tgts
            tgt_port_wwns.update(tgts)

        return ini_tgt_map, list(tgt_port_wwns)

    def execute(self, connector, host_id):
        wwns = map(lambda x: x.lower(), connector['wwpns'])

        if self.fc_san:
            ini_tgt_map, tgt_port_wwns = self._get_fc_zone(wwns, host_id)
        else:
            ini_tgt_map, tgt_port_wwns = self._get_fc_link(wwns, host_id)

        if not tgt_port_wwns:
            msg = _('No fc connection for wwns %s.') % wwns
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return ini_tgt_map, tgt_port_wwns


class AddFCInitiatorTask(task.Task):
    def __init__(self, client, fc_info, configuration, *args, **kwargs):
        super(AddFCInitiatorTask, self).__init__(*args, **kwargs)
        self.client = client
        self.fc_info = fc_info
        self.configuration = configuration

    def _get_alua_info(self, config):
        alua_info = {'MULTIPATHTYPE': '0'}
        if config.get('ACCESSMODE') and self.configuration.is_dorado_v6:
            return alua_info

        if config.get('ALUA'):
            alua_info['MULTIPATHTYPE'] = config['ALUA']

        if alua_info['MULTIPATHTYPE'] == '1':
            for k in ('FAILOVERMODE', 'SPECIALMODETYPE', 'PATHTYPE'):
                if config.get(k):
                    alua_info[k] = config[k]

        return alua_info

    def execute(self, host_id, ini_tgt_map, connector):
        for ini in ini_tgt_map:
            self.client.add_fc_initiator(ini)

            config_info = huawei_utils.find_config_info(self.fc_info, connector,
                                                        initiator=ini)
            alua_info = self._get_alua_info(config_info)
            self.client.associate_fc_initiator_to_host(host_id, ini, alua_info)


class CreateFCPortGroupTask(task.Task):
    default_provides = 'portgroup_id'

    def __init__(self, client, fc_san, *args, **kwargs):
        super(CreateFCPortGroupTask, self).__init__(*args, **kwargs)
        self.client = client
        self.fc_san = fc_san

    def _get_fc_ports(self):
        port_map = {}
        fc_ports = self.client.get_fc_ports()
        for port in fc_ports:
            port_map[port['WWN']] = port['ID']
        return port_map

    def _get_ports_to_add(self, ini_tgt_map):
        ports = set()
        for tgts in six.itervalues(ini_tgt_map):
            ports |= set(tgts)
        return ports

    def execute(self, host_id, ini_tgt_map):
        if not self.fc_san:
            return None

        portgroup_name = constants.PORTGROUP_PREFIX + host_id
        portgroup_id = self.client.create_portgroup(portgroup_name)
        port_map = self._get_fc_ports()
        ports = self._get_ports_to_add(ini_tgt_map)
        for port in ports:
            self.client.add_port_to_portgroup(portgroup_id, port_map[port])
        return portgroup_id

    def revert(self, result, ini_tgt_map, **kwargs):
        if isinstance(result, failure.Failure):
            return
        if result:
            port_map = self._get_fc_ports()
            ports = self._get_ports_to_add(ini_tgt_map)
            for port in ports:
                self.client.remove_port_from_portgroup(result, port_map[port])


class GetFCPropertiesTask(task.Task):
    default_provides = 'mapping_info'

    def execute(self, ini_tgt_map, tgt_port_wwns, hostlun_id, mappingview_id,
                aval_host_lun_ids, lun_id, lun_info):
        hostlun_id = int(hostlun_id)
        mapping_info = {
            'hostlun_id': hostlun_id,
            'mappingview_id': mappingview_id,
            'aval_host_lun_ids': aval_host_lun_ids,
            'target_discovered': True,
            'target_wwn': tgt_port_wwns,
            'target_lun': hostlun_id,
            'initiator_target_map': ini_tgt_map,
            'lun_id': lun_id,
        }

        if lun_info.get('ALLOCTYPE') == constants.THIN_LUNTYPE:
            mapping_info['discard'] = True

        return mapping_info


class ClassifyVolumeTask(task.Task):
    default_provides = ('normal_volumes', 'replication_volumes')

    def execute(self, volumes):
        normal_volumes = []
        replication_volumes = []

        for v in volumes:
            data = huawei_utils.to_dict(v.replication_driver_data)
            if 'pair_id' in data:
                replication_volumes.append(v)
            else:
                normal_volumes.append(v)

        return normal_volumes, replication_volumes


class FailoverVolumeTask(task.Task):
    default_provides = 'volumes_update'

    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(FailoverVolumeTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)

    def _failover_normal_volumes(self, volumes):
        volumes_update = []
        for v in volumes:
            volume_update = {'volume_id': v.id,
                             'updates': {'status': 'error'}}
            volumes_update.append(volume_update)

        return volumes_update

    def execute(self, replication_volumes, normal_volumes):
        volumes_update = self.replication.failover(replication_volumes)
        volumes_update += self._failover_normal_volumes(normal_volumes)
        return volumes_update


class FailbackVolumeTask(task.Task):
    default_provides = 'volumes_update'

    def __init__(self, local_cli, remote_cli, config, *args, **kwargs):
        super(FailbackVolumeTask, self).__init__(*args, **kwargs)
        self.replication = replication.ReplicationManager(
            local_cli, remote_cli, config)

    def _failback_normal_volumes(self, volumes):
        volumes_update = []
        for v in volumes:
            volume_update = {'volume_id': v.id,
                             'updates': {'status': 'available'}}
            volumes_update.append(volume_update)

        return volumes_update

    def execute(self, replication_volumes, normal_volumes):
        volumes_update = self.replication.failback(replication_volumes)
        volumes_update += self._failback_normal_volumes(normal_volumes)
        return volumes_update


def create_volume(volume, local_cli, hypermetro_rmt_cli, replication_rmt_cli,
                  configuration, feature_support):
    store_spec = {'volume': volume}

    work_flow = linear_flow.Flow('create_volume')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration),
        CreateLunTask(local_cli, configuration, feature_support),
        WaitLunOnlineTask(local_cli),
        AddQoSTask(local_cli, configuration),
        AddCacheTask(local_cli),
        AddPartitionTask(local_cli),
        CreateHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration,
            is_sync=False),
        AddHyperMetroGroupTask(
            local_cli, hypermetro_rmt_cli, configuration),
        CreateReplicationTask(
            local_cli, replication_rmt_cli, configuration),
        AddReplicationGroupTask(
            local_cli, replication_rmt_cli, configuration),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    lun_id = engine.storage.fetch('lun_id')
    lun_info = engine.storage.fetch('lun_info')
    hypermetro_id = engine.storage.fetch('hypermetro_id')
    replication_id = engine.storage.fetch('replication_id')
    return lun_id, lun_info['WWN'], hypermetro_id, replication_id


def delete_volume(volume, local_cli, hypermetro_rmt_cli, replication_rmt_cli,
                  configuration):
    store_spec = {'volume': volume}
    work_flow = linear_flow.Flow('delete_volume')
    work_flow.add(
        CheckLunExistTask(local_cli),
        CheckLunMappedTask(local_cli,
                           configuration),
        DeleteReplicationTask(local_cli, replication_rmt_cli,
                              configuration),
        DeleteHyperMetroTask(local_cli, hypermetro_rmt_cli,
                             configuration),
        DeletePartitionTask(local_cli),
        DeleteCacheTask(local_cli),
        DeleteQoSTask(local_cli),
        DeleteLunTask(local_cli),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def migrate_volume(volume, host, local_cli, feature_support, configuration):
    store_spec = {'volume': volume}

    work_flow = linear_flow.Flow('migrate_volume')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration),
        CheckLunExistTask(local_cli),
        CreateMigratedLunTask(local_cli, host, feature_support),
        WaitLunOnlineTask(local_cli, rebind={'lun_id': 'tgt_lun_id'}),
        CreateMigrateTask(local_cli, rebind={'src_lun_id': 'lun_id'}),
        WaitMigrateDoneTask(local_cli),
        AddCacheTask(local_cli),
        AddPartitionTask(local_cli),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def create_volume_from_snapshot(
        volume, snapshot, local_cli, hypermetro_rmt_cli, replication_rmt_cli,
        configuration, feature_support):
    store_spec = {'volume': volume}
    metadata = huawei_utils.get_volume_metadata(volume)
    work_flow = linear_flow.Flow('create_volume_from_snapshot')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration),
        CheckSnapshotExistTask(local_cli, inject={'snapshot': snapshot}))

    if (strutils.bool_from_string(metadata.get('fastclone', False)) or
            (metadata.get('fastclone') is None and
             configuration.clone_mode == "fastclone")):
        work_flow.add(
            LunClonePreCheckTask(inject={'src_volume': snapshot}),
            CreateLunCloneTask(local_cli,
                               rebind={'src_id': 'snapshot_id'}),
        )
    elif configuration.is_dorado_v6:
        work_flow.add(
            CreateLunTask(local_cli, configuration, feature_support,
                          inject={"src_size": snapshot.volume_size}),
            WaitLunOnlineTask(local_cli),
            CreateClonePairTask(local_cli, feature_support, configuration,
                                rebind={'source_id': 'snapshot_id',
                                        'target_id': 'lun_id'}),
            WaitClonePairDoneTask(local_cli, configuration),)
    else:
        work_flow.add(
            CreateLunTask(local_cli, configuration, feature_support),
            WaitLunOnlineTask(local_cli),
            CreateLunCopyTask(local_cli, feature_support, configuration),
            WaitLunCopyDoneTask(local_cli, configuration),)

    work_flow.add(
        ExtendVolumeTask(local_cli, inject={
            "new_size": int(volume.size) * constants.CAPACITY_UNIT}),
        AddQoSTask(local_cli, configuration),
        AddCacheTask(local_cli),
        AddPartitionTask(local_cli),
        CreateHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration),
        AddHyperMetroGroupTask(
            local_cli, hypermetro_rmt_cli, configuration),
        CreateReplicationTask(
            local_cli, replication_rmt_cli, configuration),
        AddReplicationGroupTask(
            local_cli, replication_rmt_cli, configuration),)

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    lun_id = engine.storage.fetch('lun_id')
    lun_info = engine.storage.fetch('lun_info')
    hypermetro_id = engine.storage.fetch('hypermetro_id')
    replication_id = engine.storage.fetch('replication_id')
    return lun_id, lun_info['WWN'], hypermetro_id, replication_id


def create_volume_from_volume(
        volume, src_volume, local_cli, hypermetro_rmt_cli, replication_rmt_cli,
        configuration, feature_support):
    store_spec = {'volume': volume}
    metadata = huawei_utils.get_volume_metadata(volume)
    work_flow = linear_flow.Flow('create_volume_from_volume')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration),
        CheckLunExistTask(local_cli, provides=('src_lun_info', 'src_id'),
                          inject={'volume': src_volume}),
    )

    if (strutils.bool_from_string(metadata.get('fastclone', False)) or
            (metadata.get('fastclone') is None and
             configuration.clone_mode == "fastclone")):
        work_flow.add(
            LunClonePreCheckTask(inject={'src_volume': src_volume}),
            CreateLunCloneTask(local_cli),
        )
    elif configuration.is_dorado_v6:
        work_flow.add(
            CreateLunTask(local_cli, configuration, feature_support,
                          inject={"src_size": src_volume.size}),
            WaitLunOnlineTask(local_cli),
            CreateClonePairTask(local_cli, feature_support, configuration,
                                rebind={'source_id': 'src_id',
                                        'target_id': 'lun_id'}),
            WaitClonePairDoneTask(local_cli, configuration),)
    else:
        work_flow.add(
            CreateTempSnapshotTask(local_cli, feature_support),
            WaitSnapshotReadyTask(local_cli),
            ActiveSnapshotTask(local_cli),
            CreateLunTask(local_cli, configuration, feature_support),
            WaitLunOnlineTask(local_cli),
            CreateLunCopyTask(local_cli, feature_support, configuration),
            WaitLunCopyDoneTask(local_cli, configuration),
            DeleteTempSnapshotTask(local_cli),
        )

    work_flow.add(
        ExtendVolumeTask(local_cli, inject={
            "new_size": int(volume.size) * constants.CAPACITY_UNIT}),
        AddQoSTask(local_cli, configuration),
        AddCacheTask(local_cli),
        AddPartitionTask(local_cli),
        CreateHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration),
        AddHyperMetroGroupTask(
            local_cli, hypermetro_rmt_cli, configuration),
        CreateReplicationTask(
            local_cli, replication_rmt_cli, configuration),
        AddReplicationGroupTask(
            local_cli, replication_rmt_cli, configuration),)

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    lun_id = engine.storage.fetch('lun_id')
    lun_info = engine.storage.fetch('lun_info')
    hypermetro_id = engine.storage.fetch('hypermetro_id')
    replication_id = engine.storage.fetch('replication_id')
    return lun_id, lun_info['WWN'], hypermetro_id, replication_id


def create_snapshot(snapshot, local_cli, feature_support):
    store_spec = {'snapshot': snapshot}

    work_flow = linear_flow.Flow('create_snapshot')
    work_flow.add(
        CreateSnapshotTask(local_cli, feature_support),
        WaitSnapshotReadyTask(local_cli),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    snapshot_id = engine.storage.fetch('snapshot_id')
    snapshot_wwn = engine.storage.fetch('snapshot_wwn')

    return snapshot_id, snapshot_wwn


def delete_snapshot(snapshot, local_cli):
    store_spec = {'snapshot': snapshot}
    work_flow = linear_flow.Flow('delete_snapshot')
    work_flow.add(
        CheckSnapshotExistTask(local_cli),
        DeleteSnapshotTask(local_cli),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def extend_volume(volume, new_size, local_cli, hypermetro_rmt_cli,
                  replication_rmt_cli, configuration):
    store_spec = {'volume': volume,
                  'new_size': int(new_size) * constants.CAPACITY_UNIT}
    work_flow = linear_flow.Flow('extend_volume')
    work_flow.add(
        CheckLunExistTask(local_cli),
        ExtendHyperMetroTask(local_cli, hypermetro_rmt_cli, configuration),
        ExtendReplicationTask(local_cli, replication_rmt_cli, configuration),
        ExtendVolumeTask(local_cli)
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def retype(volume, new_opts, local_cli, hypermetro_rmt_cli,
           replication_rmt_cli, configuration, feature_support):
    store_spec = {'volume': volume}

    work_flow = linear_flow.Flow('retype_volume')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration, new_opts),
        CheckLunExistTask(local_cli),
        CheckLunIsInUse(),
        UpdateLunTask(local_cli),
        UpdateQoSTask(local_cli),
        UpdateCacheTask(local_cli),
        UpdatePartitionTask(local_cli),
        DeleteHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration),
        DeleteReplicationTask(
            local_cli, replication_rmt_cli, configuration),
        CreateHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration),
        CreateReplicationTask(
            local_cli, replication_rmt_cli, configuration),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    hypermetro_id = engine.storage.fetch('hypermetro_id')
    replication_id = engine.storage.fetch('replication_id')
    return hypermetro_id, replication_id


def retype_by_migrate(volume, new_opts, host, local_cli, hypermetro_rmt_cli,
                      replication_rmt_cli, configuration, feature_support):
    store_spec = {'volume': volume}

    work_flow = linear_flow.Flow('retype_volume_by_migrate')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration, new_opts),
        CheckLunExistTask(local_cli),
        CheckLunIsInUse(),
        CreateMigratedLunTask(local_cli, host, feature_support),
        WaitLunOnlineTask(local_cli, rebind={'lun_id': 'tgt_lun_id'}),
        CreateMigrateTask(local_cli, rebind={'src_lun_id': 'lun_id'}),
        WaitMigrateDoneTask(local_cli),
        UpdateQoSTask(local_cli),
        AddCacheTask(local_cli),
        AddPartitionTask(local_cli),
        CreateHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration,
            rebind={'lun_info': 'tgt_lun_info'}),
        CreateReplicationTask(
            local_cli, replication_rmt_cli, configuration,
            rebind={'lun_info': 'tgt_lun_info'}),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    hypermetro_id = engine.storage.fetch('hypermetro_id')
    replication_id = engine.storage.fetch('replication_id')
    return hypermetro_id, replication_id


def manage_existing(volume, existing_ref, local_cli, hypermetro_rmt_cli,
                    replication_rmt_cli, configuration, feature_support):
    store_spec = {'volume': volume}

    work_flow = linear_flow.Flow('manage_volume')
    work_flow.add(
        LunOptsCheckTask(local_cli, feature_support, configuration),
        ManageVolumePreCheckTask(
            local_cli, volume, existing_ref, configuration),
        ManageLunTask(local_cli),
        UpdateQoSTask(local_cli),
        UpdateLunTask(local_cli),
        UpdateCacheTask(local_cli),
        UpdatePartitionTask(local_cli),
        DeleteHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration),
        DeleteReplicationTask(
            local_cli, replication_rmt_cli, configuration),
        CreateHyperMetroTask(
            local_cli, hypermetro_rmt_cli, configuration),
        CreateReplicationTask(
            local_cli, replication_rmt_cli, configuration),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    lun_info = engine.storage.fetch('lun_info')
    hypermetro_id = engine.storage.fetch('hypermetro_id')
    replication_id = engine.storage.fetch('replication_id')
    return lun_info['ID'], lun_info['WWN'], hypermetro_id, replication_id


def manage_existing_snapshot(snapshot, existing_ref, local_cli):
    store_spec = {'snapshot': snapshot}

    work_flow = linear_flow.Flow('manage_snapshot')
    work_flow.add(
        ManageSnapshotPreCheckTask(local_cli, snapshot, existing_ref),
        ManageSnapshotTask(local_cli),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    snapshot_info = engine.storage.fetch('snapshot_info')
    return snapshot_info['ID'], snapshot_info['WWN']


def create_group(group, local_cli, hypermetro_rmt_cli, replication_rmt_cli,
                 configuration, feature_support):
    opts = huawei_utils.get_group_type_params(group, configuration.is_dorado_v6)
    store_spec = {'group': group,
                  'opts': opts}

    work_flow = linear_flow.Flow('create_group')
    work_flow.add(
        GroupOptsCheckTask(),
        CreateHyperMetroGroupTask(
            local_cli, hypermetro_rmt_cli, configuration,
            feature_support),
        CreateReplicationGroupTask(
            local_cli, replication_rmt_cli, configuration,
            feature_support),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def initialize_iscsi_connection(lun, lun_type, connector, client,
                                configuration):
    store_spec = {'connector': connector,
                  'lun': lun,
                  'lun_type': lun_type}
    work_flow = linear_flow.Flow('initialize_iscsi_connection')

    if lun_type == constants.LUN_TYPE:
        work_flow.add(CheckLunExistTask(client, rebind={'volume': 'lun'}))
    else:
        work_flow.add(
            CheckSnapshotExistTask(
                client, provides=('lun_info', 'lun_id'),
                rebind={'snapshot': 'lun'}))

    work_flow.add(
        CreateHostTask(client, configuration.iscsi_info, configuration),
        GetISCSIConnectionTask(client, configuration.iscsi_info),
        AddISCSIInitiatorTask(client, configuration.iscsi_info, configuration),
        CreateHostGroupTask(client),
        CreateLunGroupTask(client, configuration),
        CreateMappingViewTask(client),
        GetISCSIPropertiesTask(),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()
    return engine.storage.fetch('mapping_info')


def initialize_remote_iscsi_connection(hypermetro_id, connector,
                                       client, configuration):
    store_spec = {'connector': connector,
                  'lun_type': constants.LUN_TYPE}
    work_flow = linear_flow.Flow('initialize_remote_iscsi_connection')

    work_flow.add(
        GetHyperMetroRemoteLunTask(client, hypermetro_id),
        CreateHostTask(client, configuration.hypermetro['iscsi_info'],
                       configuration),
        GetISCSIConnectionTask(client, configuration.hypermetro['iscsi_info']),
        AddISCSIInitiatorTask(client, configuration.hypermetro['iscsi_info'],
                              configuration),
        CreateHostGroupTask(client),
        CreateLunGroupTask(client, configuration),
        CreateMappingViewTask(client),
        GetISCSIPropertiesTask(client),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()
    return engine.storage.fetch('mapping_info')


def terminate_iscsi_connection(lun, lun_type, connector, client,
                               configuration):
    store_spec = {'connector': connector,
                  'lun': lun,
                  'lun_type': lun_type}
    work_flow = linear_flow.Flow('terminate_iscsi_connection')

    if lun_type == constants.LUN_TYPE:
        work_flow.add(
            GetLunIDTask(client, rebind={'volume': 'lun'}),
        )
    else:
        work_flow.add(
            GetSnapshotIDTask(
                client, provides='lun_id', rebind={'snapshot': 'lun'}),
        )

    work_flow.add(
        GetLunMappingTask(client),
        ClearLunMappingTask(client, configuration),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def terminate_remote_iscsi_connection(hypermetro_id, connector, client,
                                      configuration):
    store_spec = {'connector': connector}
    work_flow = linear_flow.Flow('terminate_remote_iscsi_connection')

    work_flow.add(
        GetHyperMetroRemoteLunTask(client, hypermetro_id),
        GetLunMappingTask(client),
        ClearLunMappingTask(client, configuration,
                            inject={'lun_type': constants.LUN_TYPE}),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()


def initialize_fc_connection(lun, lun_type, connector, fc_san, client,
                             configuration):
    store_spec = {'connector': connector,
                  'lun': lun,
                  'lun_type': lun_type}
    work_flow = linear_flow.Flow('initialize_fc_connection')

    if lun_type == constants.LUN_TYPE:
        work_flow.add(CheckLunExistTask(client, rebind={'volume': 'lun'}))
    else:
        work_flow.add(
            CheckSnapshotExistTask(
                client, provides=('lun_info', 'lun_id'),
                rebind={'snapshot': 'lun'}))

    work_flow.add(
        CreateHostTask(client, configuration.fc_info, configuration),
        GetFCConnectionTask(client, fc_san, configuration),
        AddFCInitiatorTask(client, configuration.fc_info, configuration),
        CreateHostGroupTask(client),
        CreateLunGroupTask(client, configuration),
        CreateFCPortGroupTask(client, fc_san),
        CreateMappingViewTask(client),
        GetFCPropertiesTask(),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()
    return engine.storage.fetch('mapping_info')


def initialize_remote_fc_connection(hypermetro_id, connector, fc_san, client,
                                    configuration):
    store_spec = {'connector': connector,
                  'lun_type': constants.LUN_TYPE}
    work_flow = linear_flow.Flow('initialize_remote_fc_connection')

    work_flow.add(
        GetHyperMetroRemoteLunTask(client, hypermetro_id),
        CreateHostTask(client, configuration.hypermetro['fc_info'],
                       configuration),
        GetFCConnectionTask(client, fc_san, configuration),
        AddFCInitiatorTask(client, configuration.hypermetro['fc_info'],
                           configuration),
        CreateHostGroupTask(client),
        CreateLunGroupTask(client, configuration),
        CreateFCPortGroupTask(client, fc_san),
        CreateMappingViewTask(client),
        GetFCPropertiesTask(),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()
    return engine.storage.fetch('mapping_info')


def terminate_fc_connection(lun, lun_type, connector, fc_san, client,
                            configuration):
    store_spec = {'connector': connector,
                  'lun': lun,
                  'lun_type': lun_type}
    work_flow = linear_flow.Flow('terminate_fc_connection')

    if lun_type == constants.LUN_TYPE:
        work_flow.add(
            GetLunIDTask(client, rebind={'volume': 'lun'}),
        )
    else:
        work_flow.add(
            GetSnapshotIDTask(
                client, provides='lun_id', rebind={'snapshot': 'lun'}),
        )

    work_flow.add(
        GetLunMappingTask(client),
        ClearLunMappingTask(client, configuration, fc_san),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    return engine.storage.fetch('ini_tgt_map')


def terminate_remote_fc_connection(hypermetro_id, connector, fc_san, client,
                                   configuration):
    store_spec = {'connector': connector}
    work_flow = linear_flow.Flow('terminate_remote_fc_connection')

    work_flow.add(
        GetHyperMetroRemoteLunTask(client, hypermetro_id),
        GetLunMappingTask(client),
        ClearLunMappingTask(client, configuration, fc_san,
                            inject={'lun_type': constants.LUN_TYPE}),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    return engine.storage.fetch('ini_tgt_map')


def failover(volumes, local_cli, replication_rmt_cli, configuration):
    store_spec = {'volumes': volumes}
    work_flow = linear_flow.Flow('failover')
    work_flow.add(
        ClassifyVolumeTask(),
        FailoverVolumeTask(local_cli, replication_rmt_cli,
                           configuration),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    volumes_update = engine.storage.fetch('volumes_update')
    return volumes_update


def failback(volumes, local_cli, replication_rmt_cli, configuration):
    store_spec = {'volumes': volumes}
    work_flow = linear_flow.Flow('failback')
    work_flow.add(
        ClassifyVolumeTask(),
        FailbackVolumeTask(local_cli, replication_rmt_cli,
                           configuration),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()

    volumes_update = engine.storage.fetch('volumes_update')
    return volumes_update


def revert_to_snapshot(snapshot, local_cli, rollback_speed):
    store_spec = {'snapshot': snapshot}
    work_flow = linear_flow.Flow('revert_to_snapshot')
    work_flow.add(
        CheckSnapshotExistTask(local_cli),
        RevertToSnapshotTask(local_cli, rollback_speed),
        WaitSnapshotRollbackDoneTask(local_cli),
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()
