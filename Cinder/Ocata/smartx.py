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
from cinder import utils
from cinder.volume.drivers.huawei import constants


LOG = logging.getLogger(__name__)


class SmartQos(object):
    def __init__(self, client):
        self.client = client

    def _is_high_priority(self, qos):
        """Check QoS priority."""
        for key, value in qos.items():
            if (key.find('MIN') == 0) or (key.find('LATENCY') == 0):
                return True

        return False

    @utils.synchronized('huawei_qos', external=True)
    def add(self, qos, lun_id):
        policy_id = None
        try:
            # Check QoS priority.
            if self._is_high_priority(qos):
                self.client.change_lun_priority(lun_id)
            # Create QoS policy and activate it.
            policy_id = self.client.create_qos_policy(qos, lun_id)
            self.client.activate_deactivate_qos(policy_id, True)
        except exception.VolumeBackendAPIException:
            with excutils.save_and_reraise_exception():
                if policy_id is not None:
                    self.client.delete_qos_policy(policy_id)

    @utils.synchronized('huawei_qos', external=True)
    def remove(self, qos_id, lun_id):
        qos_info = self.client.get_qos_info(qos_id)
        lun_list = self.client.get_lun_list_in_qos(qos_id, qos_info)
        if len(lun_list) <= 1:
            qos_status = qos_info['RUNNINGSTATUS']
            # 2: Active status.
            if qos_status != constants.STATUS_QOS_INACTIVATED:
                self.client.activate_deactivate_qos(qos_id, False)
            self.client.delete_qos_policy(qos_id)
        else:
            self.client.remove_lun_from_qos(lun_id, lun_list, qos_id)


class SmartPartition(object):
    def __init__(self, client):
        self.client = client

    def add(self, opts, lun_id):
        if opts['smartpartition'] != 'true':
            return
        if not opts['partitionname']:
            raise exception.InvalidInput(
                reason=_('Partition name is None, please set '
                         'smartpartition:partitionname in key.'))

        partition_id = self.client.get_partition_id_by_name(
            opts['partitionname'])
        if not partition_id:
            raise exception.InvalidInput(
                reason=(_('Can not find partition id by name %(name)s.')
                        % {'name': opts['partitionname']}))

        self.client.add_lun_to_partition(lun_id, partition_id)


class SmartCache(object):
    def __init__(self, client):
        self.client = client

    def add(self, opts, lun_id):
        if opts['smartcache'] != 'true':
            return
        if not opts['cachename']:
            raise exception.InvalidInput(
                reason=_('Cache name is None, please set '
                         'smartcache:cachename in key.'))

        cache_id = self.client.get_cache_id_by_name(opts['cachename'])
        if not cache_id:
            raise exception.InvalidInput(
                reason=(_('Can not find cache id by cache name %(name)s.')
                        % {'name': opts['cachename']}))

        self.client.add_lun_to_cache(lun_id, cache_id)


class SmartX(object):
    def __init__(self, client):
        self.client = client

    def get_smartx_specs_opts(self, opts):
        # Check that smarttier is 0/1/2/3
        opts = self.get_smarttier_opts(opts)
        opts = self.get_smartthin_opts(opts)
        opts = self.get_smartcache_opts(opts)
        opts = self.get_smartpartition_opts(opts)
        opts = self.get_controller_opts(opts)
        return opts

    def get_smarttier_opts(self, opts):
        if opts['smarttier'] == 'true':
            if not opts['policy']:
                opts['policy'] = '1'
            elif opts['policy'] not in ['0', '1', '2', '3']:
                raise exception.InvalidInput(
                    reason=(_('Illegal value specified for smarttier: '
                              'set to either 0, 1, 2, or 3.')))
        else:
            opts['policy'] = '0'

        return opts

    def get_smartthin_opts(self, opts):
        if opts['thin_provisioning_support'] == 'true':
            if opts['thick_provisioning_support'] == 'true':
                raise exception.InvalidInput(
                    reason=(_('Illegal value specified for thin: '
                              'Can not set thin and thick at the same time.')))
            else:
                opts['LUNType'] = constants.THIN_LUNTYPE
        if opts['thick_provisioning_support'] == 'true':
            opts['LUNType'] = constants.THICK_LUNTYPE

        return opts

    def get_smartcache_opts(self, opts):
        if opts['smartcache'] == 'true':
            if not opts['cachename']:
                raise exception.InvalidInput(
                    reason=_('Cache name is None, please set '
                             'smartcache:cachename in key.'))
        else:
            opts['cachename'] = None

        return opts

    def get_smartpartition_opts(self, opts):
        if opts['smartpartition'] == 'true':
            if not opts['partitionname']:
                raise exception.InvalidInput(
                    reason=_('Partition name is None, please set '
                             'smartpartition:partitionname in key.'))
        else:
            opts['partitionname'] = None

        return opts

    def get_controller_opts(self, opts):
        if opts['huawei_controller'] == 'true':
            if not opts['controllername']:
                raise exception.InvalidInput(
                    reason=_('Controller name is None, please set '
                             'controllername:controllername in key.'))
            else:
                controller_name = opts['controllername']
                controller_id = self.client.get_controller_by_name(
                    controller_name)
                opts['controllerid'] = controller_id
        else:
            opts['controllerid'] = None

        return opts
