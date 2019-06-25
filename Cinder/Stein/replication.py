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
#

import six

from oslo_log import log as logging
import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task
from taskflow.types import failure

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_utils

LOG = logging.getLogger(__name__)


class BaseReplicationOp(object):
    def __init__(self, loc_client, rmt_client):
        self.loc_client = loc_client
        self.rmt_client = rmt_client

    def _wait_until_status(self, rep_id, expect_statuses):
        def _status_check():
            info = self.get_info(rep_id)
            if info['HEALTHSTATUS'] != constants.REPLICA_HEALTH_STATUS_NORMAL:
                msg = _('Replication status %s is abnormal.'
                        ) % info['HEALTHSTATUS']
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if info['RUNNINGSTATUS'] in expect_statuses:
                return True

            return False

        huawei_utils.wait_for_condition(_status_check,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_TIMEOUT)

    def _wait_until_role(self, rep_id, is_primary):
        def _role_check():
            info = self.get_info(rep_id)
            if info['HEALTHSTATUS'] != constants.REPLICA_HEALTH_STATUS_NORMAL:
                msg = _('Replication status %s is abnormal.'
                        ) % info['HEALTHSTATUS']
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if info['ISPRIMARY'] == is_primary:
                return True

            return False

        huawei_utils.wait_for_condition(_role_check,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_TIMEOUT)

    def create(self, params):
        return self._create(params)

    def delete(self, rep_id):
        self._delete(rep_id)

    def sync(self, rep_id, client=None):
        if not client:
            client = self.loc_client
        self._sync(rep_id, client)

    def split(self, rep_id, rep_info=None):
        expect_status = (constants.REPLICA_RUNNING_STATUS_SPLIT,
                         constants.REPLICA_RUNNING_STATUS_INTERRUPTED)
        info = rep_info or self.get_info(rep_id)
        if (info.get('ISEMPTY') == 'true' or
                info['RUNNINGSTATUS'] in expect_status):
            return

        self._split(rep_id)
        self._wait_until_status(rep_id, expect_status)

    def switch(self, rep_id):
        self._switch(rep_id)

    def unprotect_secondary(self, rep_id):
        self._unprotect_secondary(rep_id)

    def protect_secondary(self, rep_id):
        self._protect_secondary(rep_id)

    def failover(self, rep_id):
        """Failover replication.

        Steps:
            1. Split replication.
            2. Set secondary access readable & writable.
            3. Try to switch replication roles.
        """
        self.split(rep_id)
        self.unprotect_secondary(rep_id)
        try:
            self.switch(rep_id)
            self._wait_until_role(rep_id, 'true')
            self.protect_secondary(rep_id)
            self.sync(rep_id, self.rmt_client)
        except Exception:
            LOG.warning('Switch replication %s roles failed, but secondary '
                        'is readable&writable now.', rep_id)

    def failback(self, rep_id):
        """Failback replication.

        Steps:
        1. Switch the role of replication if needed.
        2. Sync original secondary data back to original primary.
        3. Recover original primary&secondary replication relationship.
        """
        info = self.get_info(rep_id)
        self.split(rep_id, info)
        self.unprotect_secondary(rep_id)

        # If remote lun is primary, means the previous failover
        # didn't switch the replication roles, so we need to switch
        # again to make the original secondary lun primary.
        if info['ISPRIMARY'] == 'true':
            self.switch(rep_id)
            self._wait_until_role(rep_id, 'false')
            self.protect_secondary(rep_id)
            self.sync(rep_id)
            self._wait_until_status(
                rep_id, (constants.REPLICA_RUNNING_STATUS_NORMAL,))

        self.failover(rep_id)


class ReplicationPairOp(BaseReplicationOp):
    def get_info(self, rep_id):
        return self.rmt_client.get_replication_pair_by_id(rep_id)

    def _create(self, params):
        return self.loc_client.create_replication_pair(params)

    def _delete(self, rep_id):
        return self.loc_client.delete_replication_pair(rep_id)

    def _sync(self, rep_id, client):
        client.sync_replication_pair(rep_id)

    def _split(self, rep_id):
        self.loc_client.split_replication_pair(rep_id)

    def _switch(self, rep_id):
        self.loc_client.switch_replication_pair(rep_id)

    def _unprotect_secondary(self, rep_id):
        self.rmt_client.set_replication_pair_second_access(
            rep_id, constants.REPLICA_SECOND_RW)

    def _protect_secondary(self, rep_id):
        self.rmt_client.set_replication_pair_second_access(
            rep_id, constants.REPLICA_SECOND_RO)


class ReplicationGroupOp(BaseReplicationOp):
    def get_info(self, rep_id):
        return self.rmt_client.get_replication_group_by_id(rep_id)

    def _create(self, params):
        return self.loc_client.create_replication_group(params)

    def _delete(self, rep_id):
        return self.loc_client.delete_replication_group(rep_id)

    def _sync(self, rep_id, client):
        client.sync_replication_group(rep_id)

    def _split(self, rep_id):
        self.loc_client.split_replication_group(rep_id)

    def _switch(self, rep_id):
        self.loc_client.switch_replication_group(rep_id)

    def _unprotect_secondary(self, rep_id):
        self.rmt_client.set_replication_group_second_access(
            rep_id, constants.REPLICA_SECOND_RW)

    def _protect_secondary(self, rep_id):
        self.rmt_client.set_replication_group_second_access(
            rep_id, constants.REPLICA_SECOND_RO)

    def add_pair_to_group(self, group_id, pair_id):
        return self.loc_client.add_pair_to_replication_group(
            group_id, pair_id)

    def remove_pair_from_group(self, group_id, pair_id):
        return self.loc_client.remove_pair_from_replication_group(
            group_id, pair_id)


class _CheckCreateConditionTask(task.Task):
    default_provides = set(('rmt_dev_id',))

    def __init__(self, loc_client, rmt_client, *args, **kwargs):
        super(_CheckCreateConditionTask, self).__init__(*args, **kwargs)
        self.loc_client = loc_client
        self.rmt_client = rmt_client

    def execute(self):
        rmt_array = self.rmt_client.get_array_info()
        rmt_dev = self.loc_client.get_remote_device_by_wwn(rmt_array['wwn'])
        if not rmt_dev:
            msg = _("Remote device %s doesn't exist.") % rmt_array['wwn']
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return {'rmt_dev_id': rmt_dev['ID']}


class _CreateRemoteLunTask(task.Task):
    default_provides = set(('remote_lun_id',))

    def __init__(self, client, *args, **kwargs):
        super(_CreateRemoteLunTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, lun_params, rmt_pool):
        pool_id = self.client.get_pool_id(rmt_pool)
        if not pool_id:
            msg = _('Remote pool %s for replication not exist.') % rmt_pool
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        lun_params['PARENTID'] = pool_id
        remote_lun = self.client.create_lun(lun_params)
        huawei_utils.wait_lun_online(self.client, remote_lun['ID'])
        return {'remote_lun_id': remote_lun['ID']}

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_lun(result['remote_lun_id'])


class _CreatePairTask(task.Task):
    default_provides = set(('pair_id',))

    def __init__(self, op, configs, *args, **kwargs):
        super(_CreatePairTask, self).__init__(*args, **kwargs)
        self.op = op
        self.configs = configs

    def execute(self, local_lun_id, remote_lun_id, rmt_dev_id, replica_model):
        params = {
            "LOCALRESID": local_lun_id,
            "REMOTEDEVICEID": rmt_dev_id,
            "REMOTERESID": remote_lun_id,
            "REPLICATIONMODEL": replica_model,
            "RECOVERYPOLICY": '1',
            "SPEED": self.configs['sync_speed'],
        }

        if replica_model == constants.REPLICA_ASYNC_MODEL:
            params['SYNCHRONIZETYPE'] = '2'
            params['TIMINGVAL'] = constants.REPLICA_PERIOD

        pair_info = self.op.create(params)
        self.op.sync(pair_info['ID'])
        return {'pair_id': pair_info['ID']}


class ReplicationManager(object):
    def __init__(self, local_client, rmt_client, configs):
        self.loc_client = local_client
        self.rmt_client = rmt_client
        self.pair_op = ReplicationPairOp(self.loc_client, self.rmt_client)
        self.group_op = ReplicationGroupOp(self.loc_client, self.rmt_client)
        self.configs = configs

    def create_replica(self, local_lun_id, lun_params, replica_model):
        """Create remote LUN and replication pair.

        Purpose:
            1. create remote lun
            2. create replication pair
            3. sync replication pair
        """
        LOG.info(('Create replication, local lun: %(local_lun_id)s, '
                  'replication model: %(model)s.'),
                 {'local_lun_id': local_lun_id, 'model': replica_model})

        store_spec = {'local_lun_id': local_lun_id,
                      'lun_params': lun_params,
                      'replica_model': replica_model,
                      'rmt_pool': self.configs['storage_pools'][0],
                      }

        work_flow = linear_flow.Flow('create_replication')
        work_flow.add(
            _CheckCreateConditionTask(self.loc_client, self.rmt_client),
            _CreateRemoteLunTask(self.rmt_client),
            _CreatePairTask(self.pair_op, self.configs),
        )

        engine = taskflow.engines.load(work_flow, store=store_spec)
        engine.run()
        return engine.storage.fetch('pair_id')

    def delete_replica(self, pair_id):
        LOG.info('Delete replication pair %s.', pair_id)
        try:
            pair_info = self.pair_op.get_info(pair_id)
        except exception.VolumeBackendAPIException as exc:
            if huawei_utils.is_not_exist_exc(exc):
                return
            raise

        self.pair_op.split(pair_id)
        self.pair_op.delete(pair_id)
        self.rmt_client.delete_lun(pair_info['LOCALRESID'])

    def extend_replica(self, pair_id, new_size):
        LOG.info('Extend replication pair %s', pair_id)
        pair_info = self.pair_op.get_info(pair_id)

        cg_info = None
        cg_id = None
        if pair_info['ISINCG'] == 'true':
            cg_id = pair_info['CGID']
            cg_info = self.group_op.get_info(cg_id)

        if cg_info:
            self.group_op.split(cg_id, cg_info)
        else:
            self.pair_op.split(pair_id, pair_info)

        try:
            self.rmt_client.extend_lun(pair_info['LOCALRESID'], new_size)
            self.loc_client.extend_lun(pair_info['REMOTERESID'], new_size)
        finally:
            if cg_info:
                self.group_op.sync(cg_id)
            else:
                self.pair_op.sync(pair_id)

    def _pre_fail_check(self, volumes, statuc_check_func):
        normal_volumes = []
        pair_ids = []
        group_ids = set()
        volume_pair_infos = {}

        for v in volumes:
            drv_data = huawei_utils.to_dict(v.replication_driver_data)
            pair_id = drv_data.get('pair_id')
            if not pair_id:
                normal_volumes.append(v.id)
                continue

            pair_info = self.pair_op.get_info(pair_id)
            volume_pair_infos[v.id] = pair_info

            cg_id = pair_info.get('CGID')
            if cg_id:
                if cg_id not in group_ids:
                    group_ids.add(cg_id)
            else:
                pair_ids.append(pair_id)

        for pair_info in six.itervalues(volume_pair_infos):
            if not statuc_check_func(pair_info):
                msg = _('Replication pair %(id)s is not at the status '
                        'failover/failback available, RUNNINGSTATUS: %(run)s, '
                        'SECRESDATASTATUS: %(sec)s.'
                        ) % {'id': pair_info['ID'],
                             'run': pair_info['RUNNINGSTATUS'],
                             'sec': pair_info['SECRESDATASTATUS']}
                LOG.error(msg)
                raise exception.InvalidReplicationTarget(reason=msg)

        return normal_volumes, list(group_ids), pair_ids, volume_pair_infos

    def _fail_op(self, volumes, status_check_func, fail_group_func,
                 fail_pair_func):
        (normal_volumes, group_ids, pair_ids, volume_pair_infos
         ) = self._pre_fail_check(volumes, status_check_func)

        for group in group_ids:
            fail_group_func(group)

        for pair in pair_ids:
            fail_pair_func(pair)

        volumes_update = []
        for v in volumes:
            if v.id in normal_volumes:
                LOG.warning("Volume %s doesn't have replication.", v.id)
                continue

            rmt_lun_id = volume_pair_infos[v.id]['LOCALRESID']
            rmt_lun_info = self.rmt_client.get_lun_info_by_id(rmt_lun_id)
            location = huawei_utils.to_string(
                huawei_lun_id=rmt_lun_id,
                huawei_lun_wwn=rmt_lun_info['WWN'],
                huawei_sn=self.rmt_client.device_id,
            )

            volume_update = {'volume_id': v.id}
            volume_update['updates'] = {
                'provider_location': location,
            }

            volumes_update.append(volume_update)

        return volumes_update

    def failback(self, volumes):
        """Failback volumes to primary storage."""
        def _status_check_func(pair_info):
            return pair_info['RUNNINGSTATUS'] in (
                constants.REPLICA_RUNNING_STATUS_NORMAL,
                constants.REPLICA_RUNNING_STATUS_SPLIT,
                constants.REPLICA_RUNNING_STATUS_INTERRUPTED)

        return self._fail_op(volumes, _status_check_func,
                             self.group_op.failback, self.pair_op.failback)

    def failover(self, volumes):
        """Failover volumes to secondary storage."""
        def _status_check_func(pair_info):
            return pair_info['RUNNINGSTATUS'] in (
                constants.REPLICA_RUNNING_STATUS_NORMAL,
                constants.REPLICA_RUNNING_STATUS_SPLIT,
                constants.REPLICA_RUNNING_STATUS_INTERRUPTED
            ) and pair_info['SECRESDATASTATUS'] in (
                constants.REPLICA_SECRES_DATA_SYNC,
                constants.REPLICA_SECRES_DATA_COMPLETE
            )

        return self._fail_op(volumes, _status_check_func,
                             self.group_op.failover, self.pair_op.failover)

    def create_group(self, group_id, replica_model):
        LOG.info("Create replication group %s.", group_id)
        group_name = huawei_utils.encode_name(group_id)
        params = {'NAME': group_name,
                  'DESCRIPTION': group_id,
                  'RECOVERYPOLICY': '1',
                  'REPLICATIONMODEL': replica_model,
                  'SPEED': self.configs['sync_speed']}

        if replica_model == constants.REPLICA_ASYNC_MODEL:
            params['SYNCHRONIZETYPE'] = '2'
            params['TIMINGVAL'] = constants.REPLICA_CG_PERIOD

        group = self.group_op.create(params)
        return group['ID']

    def _add_volumes_to_group(self, group_id, volumes):
        for volume in volumes:
            drv_data = huawei_utils.to_dict(volume.replication_driver_data)
            pair_id = drv_data.get('pair_id')
            if not pair_id:
                LOG.warning("Volume %s doesn't have replication.", volume.id)
                continue

            self.pair_op.split(pair_id)
            self.group_op.add_pair_to_group(group_id, pair_id)

    def _remove_volumes_from_group(self, group_id, volumes):
        for volume in volumes:
            drv_data = huawei_utils.to_dict(volume.replication_driver_data)
            pair_id = drv_data.get('pair_id')
            if not pair_id:
                LOG.warning("Volume %s doesn't have replication.", volume.id)
                continue

            self.group_op.remove_pair_from_group(group_id, pair_id)
            self.pair_op.sync(pair_id)

    def delete_group(self, group_id, volumes):
        LOG.info("Delete replication group %s.", group_id)
        group_info = huawei_utils.get_replication_group(
            self.loc_client, group_id)
        if not group_info:
            LOG.warning('Replication group %s to delete not exist.',
                        group_id)
            return

        self.group_op.split(group_info['ID'], group_info)
        self._remove_volumes_from_group(group_info['ID'], volumes)
        self.group_op.delete(group_info['ID'])

    def update_group(self, group_id, add_volumes, remove_volumes):
        LOG.info("Update replication group %s.", group_id)
        group_info = huawei_utils.get_replication_group(
            self.loc_client, group_id)
        if not group_info:
            LOG.warning('Replication group %s to update not exist.',
                        group_id)
            return

        self.group_op.split(group_info['ID'], group_info)
        self._add_volumes_to_group(group_info['ID'], add_volumes)
        self._remove_volumes_from_group(group_info['ID'], remove_volumes)
        self.group_op.sync(group_info['ID'])

    def add_replication_to_group(self, group_id, pair_id):
        group_info = huawei_utils.get_replication_group(
            self.loc_client, group_id)
        if not group_info:
            msg = _('Replication group %s not exist.') % group_id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self.group_op.split(group_info['ID'], group_info)
        self.pair_op.split(pair_id)
        self.group_op.add_pair_to_group(group_info['ID'], pair_id)
        self.group_op.sync(group_info['ID'])
