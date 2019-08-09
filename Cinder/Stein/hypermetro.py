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
from oslo_utils import strutils

import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task
from taskflow.types import failure

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_utils


LOG = logging.getLogger(__name__)


class _CheckCreateConditionTask(task.Task):
    default_provides = set(('domain_id', 'remote_pool_id'))

    def __init__(self, client, hypermetro_configs, *args, **kwargs):
        super(_CheckCreateConditionTask, self).__init__(*args, **kwargs)
        self.client = client
        self.hypermetro_configs = hypermetro_configs

    def execute(self):
        domain_name = self.hypermetro_configs['metro_domain']
        domain_id = self.client.get_hypermetro_domain_id(domain_name)
        if not domain_id:
            msg = _("Hypermetro domain %s doesn't exist.") % domain_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Get the remote pool info.
        hypermetro_pool = self.hypermetro_configs['storage_pools'][0]
        pool_id = self.client.get_pool_id(hypermetro_pool)
        if not pool_id:
            msg = _("Remote pool %s does not exist.") % hypermetro_pool
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return {'domain_id': domain_id,
                'remote_pool_id': pool_id}


class _CreateRemoteLunTask(task.Task):
    default_provides = set(('remote_lun_id',))

    def __init__(self, client, *args, **kwargs):
        super(_CreateRemoteLunTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, lun_params, remote_pool_id):
        # Create remote hypermetro lun.
        lun_params['PARENTID'] = remote_pool_id
        remote_lun = self.client.create_lun(lun_params)
        huawei_utils.wait_lun_online(self.client, remote_lun['ID'])
        return {'remote_lun_id': remote_lun['ID']}

    def revert(self, result, **kwargs):
        if isinstance(result, failure.Failure):
            return
        self.client.delete_lun(result['remote_lun_id'])


class _CreateHyperMetroTask(task.Task):
    default_provides = set(('hypermetro_id',))

    def __init__(self, client, hypermetro_configs, is_sync, *args, **kwargs):
        super(_CreateHyperMetroTask, self).__init__(*args, **kwargs)
        self.client = client
        self.hypermetro_configs = hypermetro_configs
        self.sync = is_sync

    def _is_sync_completed(self, metro_id):
        metro_info = self.client.get_hypermetro_by_id(metro_id)
        if ((metro_info['HEALTHSTATUS'] != constants.METRO_HEALTH_NORMAL) or
                metro_info['RUNNINGSTATUS'] not in (
                constants.METRO_RUNNING_NORMAL,
                constants.METRO_RUNNING_SYNC,
                constants.RUNNING_TO_BE_SYNC)):
            msg = _("HyperMetro pair %(id)s is not in a available status, "
                    "RunningStatus is: %(run)s, HealthStatus is: %(health)s"
                    ) % {"id": metro_id,
                         "run": metro_info.get('RUNNINGSTATUS'),
                         "health": metro_info.get("HEALTHSTATUS")}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        if metro_info.get('RUNNINGSTATUS') == constants.METRO_RUNNING_NORMAL:
            return True
        return False

    def execute(self, domain_id, local_lun_id, remote_lun_id):
        hypermetro_param = {"DOMAINID": domain_id,
                            "HCRESOURCETYPE": '1',
                            "ISFIRSTSYNC": False,
                            "LOCALOBJID": local_lun_id,
                            "REMOTEOBJID": remote_lun_id,
                            "SPEED": self.hypermetro_configs['sync_speed']}
        if self.sync:
            hypermetro_param.update({"ISFIRSTSYNC": True})

        hypermetro_pair = self.client.create_hypermetro(
            hypermetro_param)
        if self.sync:
            self.client.sync_hypermetro(hypermetro_pair['ID'])
            if strutils.bool_from_string(
                    self.hypermetro_configs['metro_sync_completed']):
                huawei_utils.wait_for_condition(
                    lambda: self._is_sync_completed(hypermetro_pair['ID']),
                    constants.DEFAULT_WAIT_INTERVAL,
                    constants.DEFAULT_WAIT_INTERVAL * 10)

        return {'hypermetro_id': hypermetro_pair['ID']}


class HuaweiHyperMetro(object):
    def __init__(self, local_cli, remote_cli, configs):
        self.local_cli = local_cli
        self.remote_cli = remote_cli
        self.configs = configs

    def create_hypermetro(self, local_lun_id, lun_params, is_sync):
        LOG.info('To create hypermetro for local lun %s', local_lun_id)

        store_spec = {'local_lun_id': local_lun_id,
                      'lun_params': lun_params}
        work_flow = linear_flow.Flow('create_hypermetro')
        work_flow.add(_CheckCreateConditionTask(self.remote_cli, self.configs),
                      _CreateRemoteLunTask(self.remote_cli),
                      _CreateHyperMetroTask(self.local_cli, self.configs,
                                            is_sync))

        engine = taskflow.engines.load(work_flow, store=store_spec)
        engine.run()

        return engine.storage.fetch('hypermetro_id')

    def delete_hypermetro(self, volume):
        lun_name = huawei_utils.encode_name(volume.id)
        hypermetro = self.local_cli.get_hypermetro_by_lun_name(lun_name)

        if hypermetro:
            if (hypermetro['RUNNINGSTATUS'] in (
                    constants.METRO_RUNNING_NORMAL,
                    constants.METRO_RUNNING_SYNC)):
                self.local_cli.stop_hypermetro(hypermetro['ID'])

            self.local_cli.delete_hypermetro(hypermetro['ID'])
            self.remote_cli.delete_lun(hypermetro['REMOTEOBJID'])
        else:
            remote_lun_info = self.remote_cli.get_lun_info_by_name(lun_name)
            if remote_lun_info:
                self.remote_cli.delete_lun(remote_lun_info['ID'])

    def extend_hypermetro(self, hypermetro_id, new_size):
        LOG.info('Extend hypermetro pair %s', hypermetro_id)
        metro_info = self.remote_cli.get_hypermetro_by_id(hypermetro_id)
        metrogroup = None
        if metro_info['ISINCG'] == 'true':
            cg_id = metro_info['CGID']
            metrogroup = huawei_utils.get_hypermetro_group(
                self.local_cli, cg_id)

        if metrogroup:
            self._stop_consistencygroup_if_need(metrogroup)
        elif ((metro_info['HEALTHSTATUS'] == constants.METRO_HEALTH_NORMAL)
              and metro_info['RUNNINGSTATUS'] in (
                  constants.METRO_RUNNING_NORMAL,
                  constants.METRO_RUNNING_SYNC)):
            self.local_cli.stop_hypermetro(hypermetro_id)

        try:
            self.remote_cli.extend_lun(metro_info['LOCALOBJID'], new_size)
            self.local_cli.extend_lun(metro_info['REMOTEOBJID'], new_size)
        finally:
            if metrogroup:
                self.local_cli.sync_metrogroup(metrogroup['ID'])
            else:
                self.local_cli.sync_hypermetro(hypermetro_id)

    def sync_hypermetro(self, hypermetro_id):
        self.local_cli.sync_hypermetro(hypermetro_id)

    def create_consistencygroup(self, group_id):
        LOG.info("Create hypermetro consistency group %s.", group_id)

        domain_name = self.configs['metro_domain']
        domain_id = self.local_cli.get_hypermetro_domain_id(domain_name)
        if not domain_id:
            msg = _("Hypermetro domain %s doesn't exist.") % domain_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        params = {"NAME": huawei_utils.encode_name(group_id),
                  "DESCRIPTION": group_id,
                  "RECOVERYPOLICY": "1",
                  "SPEED": self.configs['sync_speed'],
                  "DOMAINID": domain_id}
        self.local_cli.create_metrogroup(params)

    def delete_consistencygroup(self, group_id, volumes):
        LOG.info("Delete hypermetro consistency group %s.", group_id)

        metrogroup = huawei_utils.get_hypermetro_group(self.local_cli,
                                                       group_id)
        if not metrogroup:
            LOG.warning('Hypermetro group %s to delete not exist.',
                        group_id)
            return

        self._stop_consistencygroup_if_need(metrogroup)
        self._remove_volume_from_metrogroup(volumes, metrogroup['ID'])
        self.local_cli.delete_metrogroup(metrogroup['ID'])

    def _check_metro_in_group(self, metrogroup_id, metro_id):
        metro_info = self.local_cli.get_hypermetro_by_id(metro_id)
        return (metro_info and metro_info.get('ISINCG') == 'true' and
                metro_info.get('CGID') == metrogroup_id)

    def _ensure_hypermetro_in_group(self, metrogroup_id, metro_ids):
        for metro_id in metro_ids:
            huawei_utils.wait_for_condition(
                lambda: self._check_metro_in_group(metrogroup_id, metro_id),
                constants.DEFAULT_WAIT_INTERVAL,
                constants.DEFAULT_WAIT_INTERVAL * 10)

    def _ensure_hypermetro_not_in_group(self, metrogroup_id, metro_ids):
        for metro_id in metro_ids:
            huawei_utils.wait_for_condition(
                lambda: not self._check_metro_in_group(metrogroup_id,
                                                       metro_id),
                constants.DEFAULT_WAIT_INTERVAL,
                constants.DEFAULT_WAIT_INTERVAL * 10)
            self.local_cli.sync_hypermetro(metro_id)

    def _add_volume_to_metrogroup(self, volumes, metrogroup_id):
        metro_ids = []
        for volume in volumes:
            metadata = huawei_utils.get_volume_private_data(volume)
            if not metadata.get('hypermetro'):
                LOG.warning("Volume %s doesn't have hypermetro.", volume.id)
                continue

            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if not hypermetro:
                LOG.warning("Volume %s doesn't have hypermetro on the array.",
                            volume.id)
                continue

            metro_id = hypermetro['ID']
            self._stop_hypermetro_if_need(metro_id)
            self.local_cli.add_metro_to_metrogroup(metrogroup_id, metro_id)
            metro_ids.append(metro_id)

        self._ensure_hypermetro_in_group(metrogroup_id, metro_ids)

    def _remove_volume_from_metrogroup(self, volumes, metrogroup_id):
        metro_ids = []
        for volume in volumes:
            metadata = huawei_utils.get_volume_private_data(volume)
            if not metadata.get('hypermetro'):
                LOG.warning("Volume %s doesn't have hypermetro.", volume.id)
                continue

            hypermetro = huawei_utils.get_hypermetro(self.local_cli, volume)
            if not hypermetro:
                LOG.warning("Volume %s doesn't have hypermetro on the array.",
                            volume.id)
                continue

            metro_id = hypermetro['ID']
            self.local_cli.remove_metro_from_metrogroup(
                metrogroup_id, metro_id)
            metro_ids.append(metro_id)

        self._ensure_hypermetro_not_in_group(metrogroup_id, metro_ids)

    def update_consistencygroup(self, group_id, add_volumes, remove_volumes):
        LOG.info("Update hypermetro consistency group %s.", group_id)

        metrogroup = huawei_utils.get_hypermetro_group(
            self.local_cli, group_id)
        if not metrogroup:
            msg = _('Hypermetro group %s to update not exist.') % group_id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._stop_consistencygroup_if_need(metrogroup)
        self._add_volume_to_metrogroup(add_volumes, metrogroup['ID'])
        self._remove_volume_from_metrogroup(remove_volumes, metrogroup['ID'])
        self.local_cli.sync_metrogroup(metrogroup['ID'])

    def _stop_consistencygroup_if_need(self, metrogroup):
        if (metrogroup['HEALTHSTATUS'] == constants.METRO_HEALTH_NORMAL and
                metrogroup['RUNNINGSTATUS'] in
                (constants.METRO_RUNNING_NORMAL,
                 constants.METRO_RUNNING_SYNC)):
            self.local_cli.stop_metrogroup(metrogroup['ID'])

    def _stop_hypermetro_if_need(self, metro_id):
        metro_info = self.local_cli.get_hypermetro_by_id(metro_id)
        if metro_info:
            if ((metro_info['HEALTHSTATUS'] == constants.METRO_HEALTH_NORMAL
                 ) and metro_info['RUNNINGSTATUS'] in (
                    constants.METRO_RUNNING_NORMAL,
                    constants.METRO_RUNNING_SYNC)):
                self.local_cli.stop_hypermetro(metro_id)

    def add_hypermetro_to_group(self, group_id, metro_id):
        metrogroup = huawei_utils.get_hypermetro_group(
            self.local_cli, group_id)
        if not metrogroup:
            msg = _('Hypermetro group %s to not exist.') % group_id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._stop_consistencygroup_if_need(metrogroup)
        self._stop_hypermetro_if_need(metro_id)
        self.local_cli.add_metro_to_metrogroup(metrogroup['ID'], metro_id)
        self.local_cli.sync_metrogroup(metrogroup['ID'])
