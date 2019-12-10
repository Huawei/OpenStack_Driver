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
from oslo_utils import excutils

from cinder import exception
from cinder.i18n import _
from cinder.i18n import _LI
from cinder.i18n import _LW
from cinder import utils
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_utils

LOG = logging.getLogger(__name__)


class HuaweiHyperMetro(object):

    def __init__(self, client, rmt_client, configuration):
        self.client = client
        self.rmt_client = rmt_client
        self.configuration = configuration

    def create_hypermetro(self, local_lun_id, lun_params, is_sync=False):
        """Create hypermetro."""

        try:
            # Check remote metro domain is valid.
            domain_id = self._valid_rmt_metro_domain()

            # Get the remote pool info.
            config_pool = self.rmt_client.storage_pools[0]
            remote_pool = self.rmt_client.get_all_pools()
            pool = self.rmt_client.get_pool_info(config_pool, remote_pool)
            if not pool:
                err_msg = _("Remote pool cannot be found.")
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)

            # Create remote lun.
            lun_params['PARENTID'] = pool['ID']
            remotelun_info = self.rmt_client.create_lun(lun_params)
            remote_lun_id = remotelun_info['ID']

            # Get hypermetro domain.
            try:
                self._wait_volume_ready(local_lun_id, True)
                self._wait_volume_ready(remote_lun_id, False)
                hypermetro = self._create_hypermetro_pair(domain_id,
                                                          local_lun_id,
                                                          remote_lun_id,
                                                          is_sync)
                if is_sync:
                    try:
                        self._sync_metro(hypermetro['ID'])
                    except Exception as err:
                        with excutils.save_and_reraise_exception():
                            LOG.error('Start synchronization failed. '
                                      'Error: %s.', err)
                            self.check_metro_need_to_stop(hypermetro['ID'])
                            self.client.delete_hypermetro(hypermetro['ID'])

                LOG.info(_LI("Hypermetro id: %(metro_id)s. "
                             "Remote lun id: %(remote_lun_id)s."),
                         {'metro_id': hypermetro['ID'],
                          'remote_lun_id': remote_lun_id})

                return hypermetro['ID']
            except exception.VolumeBackendAPIException as err:
                self.rmt_client.delete_lun(remote_lun_id)
                msg = _('Create hypermetro error. %s.') % err
                raise exception.VolumeBackendAPIException(data=msg)
        except exception.VolumeBackendAPIException:
            raise

    def delete_hypermetro(self, volume):
        """Delete hypermetro."""
        lun_name = huawei_utils.encode_name(volume.id)
        hypermetro = self.client.get_hypermetro_by_lun_name(lun_name)
        if not hypermetro:
            return

        metro_id = hypermetro['ID']
        remote_lun_id = hypermetro['REMOTEOBJID']

        # Delete hypermetro and remote lun.
        self.check_metro_need_to_stop(metro_id, hypermetro)
        self.client.delete_hypermetro(metro_id)
        self.rmt_client.delete_lun(remote_lun_id)

    @utils.synchronized('huawei_create_hypermetro_pair', external=True)
    def _create_hypermetro_pair(self, domain_id, lun_id, remote_lun_id,
                                is_sync=False):
        """Create a HyperMetroPair."""
        hcp_param = {"DOMAINID": domain_id,
                     "HCRESOURCETYPE": '1',
                     "ISFIRSTSYNC": False,
                     "LOCALOBJID": lun_id,
                     "RECONVERYPOLICY": '1',
                     "REMOTEOBJID": remote_lun_id,
                     "SPEED": self.configuration.hyper_sync_speed}
        if is_sync:
            hcp_param.update({"ISFIRSTSYNC": True})

        return self.client.create_hypermetro(hcp_param)

    def connect_volume_fc(self, volume, connector):
        """Create map between a volume and a host for FC."""
        wwns = connector['wwpns']
        LOG.info(_LI(
            'initialize_connection_fc, initiator: %(wwpns)s,'
            'volume id: %(id)s.'),
            {'wwpns': wwns,
             'id': volume.id})

        lun_id, _ = huawei_utils.get_volume_lun_id(self.rmt_client, volume)
        if not lun_id:
            msg = _("Can't get volume id. Volume name: %s.") % volume.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        original_host_name = connector['host']

        # Create hostgroup if not exist.
        host_id = self.rmt_client.add_host_with_check(original_host_name)

        online_wwns_in_host = (
            self.rmt_client.get_host_online_fc_initiators(host_id))
        online_free_wwns = self.rmt_client.get_online_free_wwns()
        fc_initiators_on_array = self.rmt_client.get_fc_initiator_on_array()
        wwns = [i for i in wwns if i in fc_initiators_on_array]
        for wwn in wwns:
            if (wwn not in online_wwns_in_host
                    and wwn not in online_free_wwns):
                wwns_in_host = (
                    self.rmt_client.get_host_fc_initiators(host_id))
                iqns_in_host = (
                    self.rmt_client.get_host_iscsi_initiators(host_id))
                if not (wwns_in_host or iqns_in_host):
                    self.rmt_client.remove_host(host_id)
                wwns.remove(wwn)

                if (self.configuration.rmt_min_fc_ini_online ==
                        constants.DEFAULT_MINIMUM_FC_INITIATOR_ONLINE):
                    msg = (("Can't add FC initiator %(wwn)s to host %(host)s,"
                            " please check if this initiator has been added "
                            "to other host or isn't present on array.")
                           % {"wwn": wwn, "host": host_id})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

        if len(wwns) < self.configuration.rmt_min_fc_ini_online:
            msg = (("The number of online fc initiator %(wwns)s less than"
                    " the set %(set)s number.") % {
                "wwns": wwns,
                "set": self.configuration.rmt_min_fc_ini_online})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for wwn in wwns:
            self.rmt_client.ensure_fc_initiator_added(wwn, host_id,
                                                      connector['host'])

        (tgt_port_wwns, init_targ_map) = (
            self.rmt_client.get_init_targ_map(wwns))

        # Add host into hostgroup.
        hostgroup_id = self.rmt_client.add_host_to_hostgroup(host_id)
        map_info = self.rmt_client.do_mapping(lun_id, hostgroup_id, host_id,
                                              hypermetro_lun=True)
        if not map_info:
            msg = _('Map info is None due to array version '
                    'not supporting hypermetro.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        host_lun_id = self.rmt_client.get_host_lun_id(host_id, lun_id)

        # Return FC properties.
        fc_info = {'driver_volume_type': 'fibre_channel',
                   'data': {'target_lun': int(host_lun_id),
                            'target_discovered': True,
                            'target_wwn': tgt_port_wwns,
                            'volume_id': volume.id,
                            'initiator_target_map': init_targ_map,
                            'map_info': map_info},
                   }

        LOG.info(_LI('Remote return FC info is: %s.'), fc_info)

        return fc_info

    def disconnect_volume_fc(self, volume, connector):
        """Delete map between a volume and a host for FC."""
        wwns = connector['wwpns']
        lun_id, _ = huawei_utils.get_volume_lun_id(self.rmt_client, volume)
        host_name = connector['host']
        lungroup_id = None

        LOG.info(_LI('terminate_connection_fc: volume: %(id)s, '
                     'wwpns: %(wwns)s, '
                     'lun_id: %(lunid)s.'),
                 {'id': volume.id,
                  'wwns': wwns,
                  'lunid': lun_id},)

        hostid = huawei_utils.get_host_id(self.rmt_client, host_name)
        if hostid:
            mapping_view_name = constants.MAPPING_VIEW_PREFIX + hostid
            view_id = self.rmt_client.find_mapping_view(
                mapping_view_name)
            if view_id:
                lungroup_id = self.rmt_client.find_lungroup_from_map(
                    view_id)

        if lun_id and self.rmt_client.check_lun_exist(lun_id):
            if lungroup_id:
                lungroup_ids = self.rmt_client.get_lungroupids_by_lunid(
                    lun_id)
                if lungroup_id in lungroup_ids:
                    self.rmt_client.remove_lun_from_lungroup(
                        lungroup_id, lun_id)
                else:
                    LOG.warning(_LW("Lun is not in lungroup. "
                                    "Lun id: %(lun_id)s, "
                                    "lungroup id: %(lungroup_id)s"),
                                {"lun_id": lun_id,
                                 "lungroup_id": lungroup_id})

    def _wait_volume_ready(self, lun_id, local=True):
        wait_interval = self.configuration.lun_ready_wait_interval
        client = self.client if local else self.rmt_client

        def _volume_ready():
            result = client.get_lun_info(lun_id)
            if (result['HEALTHSTATUS'] == constants.STATUS_HEALTH
               and result['RUNNINGSTATUS'] == constants.STATUS_VOLUME_READY):
                return True
            return False

        huawei_utils.wait_for_condition(_volume_ready,
                                        wait_interval,
                                        wait_interval * 10)

    def create_consistencygroup(self, group):
        LOG.info(_LI("Create Consistency Group: %(group)s."),
                 {'group': group['id']})
        group_name = huawei_utils.encode_name(group['id'])
        domain_id = self._valid_rmt_metro_domain()
        self.client.create_metrogroup(group_name, group['id'], domain_id)

    def delete_consistencygroup(self, context, group, volumes):
        LOG.info(_LI("Delete Consistency Group: %(group)s."),
                 {'group': group['id']})
        metrogroup_id = self.check_consistencygroup_need_to_stop(group)
        if not metrogroup_id:
            return

        # Remove pair from metrogroup.
        for volume in volumes:
            metadata = huawei_utils.get_lun_metadata(volume)
            if not metadata.get('hypermetro'):
                continue

            lun_name = huawei_utils.encode_name(volume.id)
            hypermetro = self.client.get_hypermetro_by_lun_name(lun_name)
            if not hypermetro:
                continue

            metro_id = hypermetro['ID']
            if self._check_metro_in_cg(metro_id, metrogroup_id):
                self.client.remove_metro_from_metrogroup(metrogroup_id,
                                                         metro_id)

        # Delete metrogroup.
        self.client.delete_metrogroup(metrogroup_id)

    def _ensure_hypermetro_added_to_cg(self, metro_id, metrogroup_id):
        def _check_added():
            return self._check_metro_in_cg(metro_id, metrogroup_id)

        huawei_utils.wait_for_condition(_check_added,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_INTERVAL * 10)

    def _ensure_hypermetro_removed_from_cg(self, metro_id, metrogroup_id):
        def _check_removed():
            return not self._check_metro_in_cg(metro_id, metrogroup_id)

        huawei_utils.wait_for_condition(_check_removed,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_INTERVAL * 10)

    def update_consistencygroup(self, context, group,
                                add_volumes, remove_volumes):
        LOG.info(_LI("Update Consistency Group: %(group)s. "
                     "This adds or removes volumes from a CG."),
                 {'group': group['id']})

        metrogroup_id = self.check_consistencygroup_need_to_stop(group)
        if not metrogroup_id:
            msg = _("The CG does not exist on array.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Deal with add volumes to CG
        for volume in add_volumes:
            metadata = huawei_utils.get_lun_metadata(volume)
            if not metadata.get('hypermetro'):
                err_msg = _("Volume %s is not in hypermetro pair.") % volume.id
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)

            lun_name = huawei_utils.encode_name(volume.id)
            hypermetro = self.client.get_hypermetro_by_lun_name(lun_name)
            if not hypermetro:
                err_msg = _("Volume %s is not in hypermetro pair.") % volume.id
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)

            metro_id = hypermetro['ID']
            if not self._check_metro_in_cg(metro_id, metrogroup_id):
                self.check_metro_need_to_stop(metro_id)
                self.client.add_metro_to_metrogroup(metrogroup_id,
                                                    metro_id)
                self._ensure_hypermetro_added_to_cg(
                    metro_id, metrogroup_id)

        # Deal with remove volumes from CG
        for volume in remove_volumes:
            metadata = huawei_utils.get_lun_metadata(volume)
            if not metadata.get('hypermetro'):
                continue

            lun_name = huawei_utils.encode_name(volume.id)
            hypermetro = self.client.get_hypermetro_by_lun_name(lun_name)
            if not hypermetro:
                continue

            metro_id = hypermetro['ID']
            if self._check_metro_in_cg(metro_id, metrogroup_id):
                self.check_metro_need_to_stop(metro_id)
                self.client.remove_metro_from_metrogroup(metrogroup_id,
                                                         metro_id)
                self._ensure_hypermetro_removed_from_cg(
                    metro_id, metrogroup_id)
                self.client.sync_hypermetro(metro_id)

        new_group_info = self.client.get_metrogroup_by_id(metrogroup_id)
        is_empty = new_group_info["ISEMPTY"]
        if is_empty == 'false':
            self.client.sync_metrogroup(metrogroup_id)

    def add_hypermetro_to_consistencygroup(self, group, metro_id):
        metrogroup_id = self.check_consistencygroup_need_to_stop(group)
        if metrogroup_id:
            self.check_metro_need_to_stop(metro_id)
            self.client.add_metro_to_metrogroup(metrogroup_id, metro_id)
            self._ensure_hypermetro_added_to_cg(metro_id, metrogroup_id)
            try:
                self.client.sync_metrogroup(metrogroup_id)
            except exception.VolumeBackendAPIException:
                # Ignore this sync error.
                LOG.warning(_LW('Resync metro group %(group)s failed '
                                'after add new metro %(metro)s.'),
                            {'group': metrogroup_id,
                             'metro': metro_id})

    def check_metro_need_to_stop(self, metro_id, metro_info=None):
        if not metro_info:
            metro_info = self.client.get_hypermetro_by_id(metro_id)

        if metro_info:
            metro_health_status = metro_info['HEALTHSTATUS']
            metro_running_status = metro_info['RUNNINGSTATUS']

            if (metro_health_status == constants.HEALTH_NORMAL and
                (metro_running_status == constants.RUNNING_NORMAL or
                    metro_running_status == constants.RUNNING_SYNC)):
                self.client.stop_hypermetro(metro_id)

    def _get_metro_group_id(self, id):
        group_name = huawei_utils.encode_name(id)
        metrogroup_id = self.client.get_metrogroup_by_name(group_name)

        if not metrogroup_id:
            group_name = huawei_utils.old_encode_name(id)
            metrogroup_id = self.client.get_metrogroup_by_name(group_name)

        return metrogroup_id

    def check_consistencygroup_need_to_stop(self, group):
        metrogroup_id = self._get_metro_group_id(group['id'])
        if metrogroup_id:
            self.stop_consistencygroup(metrogroup_id)

        return metrogroup_id

    def stop_consistencygroup(self, metrogroup_id):
        metrogroup_info = self.client.get_metrogroup_by_id(metrogroup_id)
        health_status = metrogroup_info['HEALTHSTATUS']
        running_status = metrogroup_info['RUNNINGSTATUS']

        if (health_status == constants.HEALTH_NORMAL
            and (running_status == constants.RUNNING_NORMAL
                 or running_status == constants.RUNNING_SYNC)):
            self.client.stop_metrogroup(metrogroup_id)

    def _check_metro_in_cg(self, metro_id, cg_id):
        metro_info = self.client.get_hypermetro_by_id(metro_id)
        return (metro_info and metro_info['ISINCG'] == 'true' and
                metro_info['CGID'] == cg_id)

    def _valid_rmt_metro_domain(self):
        domain_name = self.rmt_client.metro_domain
        if not domain_name:
            err_msg = _("Hypermetro domain doesn't config.")
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)
        domain_id = self.rmt_client.get_hyper_domain_id(domain_name)
        if not domain_id:
            err_msg = _("Hypermetro domain cannot be found.")
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        return domain_id

    def _is_sync_complete(self, metro_id):
        metro_info = self.client.get_hypermetro_by_id(metro_id)
        if (metro_info.get("HEALTHSTATUS") not in (constants.HEALTH_NORMAL, )
                or metro_info.get('RUNNINGSTATUS') not in
                constants.METRO_SYNC_NORMAL):
            msg = _("HyperMetro pair %(id)s is not in a available status, "
                    "RunningStatus is: %(run)s, HealthStatus is: %(health)s"
                    ) % {"id": metro_id,
                         "run": metro_info.get('RUNNINGSTATUS'),
                         "health": metro_info.get("HEALTHSTATUS")}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if metro_info.get('RUNNINGSTATUS') == constants.RUNNING_NORMAL:
            return True
        return False

    def _sync_metro(self, metro_id):
        def _is_sync_complete():
            return self._is_sync_complete(metro_id)

        try:
            self.client.sync_hypermetro(metro_id)
            if self.rmt_client.metro_sync_completed:
                huawei_utils.wait_for_condition(
                    _is_sync_complete, constants.HYPERMETRO_WAIT_INTERVAL,
                    constants.DEFAULT_WAIT_TIMEOUT)
        except Exception as err:
            raise exception.VolumeBackendAPIException(data=err)
