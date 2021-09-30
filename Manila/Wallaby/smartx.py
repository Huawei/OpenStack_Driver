# Copyright (c) 2015 Huawei Technologies Co., Ltd.
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

import json
from oslo_log import log as logging

from manila import exception
from manila.i18n import _
from manila.share.drivers.huawei import constants

LOG = logging.getLogger(__name__)


class SmartPartition(object):
    def __init__(self, helper):
        self.helper = helper

    def add(self, partitionname, fs_id):
        partition_id = self.helper.get_partition_id_by_name(partitionname)
        if not partition_id:
            msg = _('Partition %s not exist.') % partitionname
            raise exception.InvalidInput(reason=msg)

        self.helper.add_fs_to_partition(fs_id, partition_id)

    def remove(self, fs_id, partition_id):
        self.helper.remove_fs_from_partition(fs_id, partition_id)

    def update(self, fs_id, partitionname, partition_id):
        partition_info = self.helper.get_partition_info_by_id(partition_id)
        if partition_info['NAME'] == partitionname:
            return

        self.remove(fs_id, partition_id)
        self.add(partitionname, fs_id)


class SmartCache(object):
    def __init__(self, helper):
        self.helper = helper

    def add(self, cachename, fs_id):
        cache_id = self.helper.get_cache_id_by_name(cachename)
        if not cache_id:
            msg = _('Cache %s not exist.') % cachename
            raise exception.InvalidInput(reason=msg)

        self.helper.add_fs_to_cache(fs_id, cache_id)

    def remove(self, fs_id, cache_id):
        self.helper.remove_fs_from_cache(fs_id, cache_id)

    def update(self, fs_id, cachename, cache_id):
        cache_info = self.helper.get_cache_info_by_id(cache_id)
        if cache_info['NAME'] == cachename:
            return

        self.remove(fs_id, cache_id)
        self.add(cachename, fs_id)


class SmartQos(object):
    def __init__(self, helper):
        self.helper = helper

    @staticmethod
    def _check_qos_consistency(policy, qos):
        check_keys = set(constants.QOS_KEYS) & set(qos.keys())
        policy_keys = set(constants.QOS_KEYS) & set(policy.keys())

        if 'LATENCY' in policy_keys and policy['LATENCY'] == '0':
            policy_keys.remove('LATENCY')

        if check_keys != policy_keys:
            return False

        for key in check_keys:
            if qos[key] != policy[key]:
                return False

        return True

    def add(self, qos, fs_id):
        self._change_lun_priority(qos, fs_id)

        qos_id = self.helper.create_qos(qos, fs_id)
        try:
            self.helper.activate_deactivate_qos(qos_id, True)
        except exception.ShareBackendException:
            self.remove(qos_id, fs_id)
            raise

        return qos_id

    def _change_lun_priority(self, qos, fs_id):
        for key in qos:
            if key.startswith('MIN') or key.startswith('LATENCY'):
                data = {"IOPRIORITY": "3"}
                self.helper.update_filesystem(fs_id, data)
                break

    def remove(self, fs_id, qos_id, qos_info=None):
        if not qos_info:
            qos_info = self.helper.get_qos_info(qos_id)
        fs_list = json.loads(qos_info['FSLIST'])
        if fs_id in fs_list:
            fs_list.remove(fs_id)

        if len(fs_list) <= 0:
            if qos_info['RUNNINGSTATUS'] != constants.QOS_INACTIVATED:
                self.helper.activate_deactivate_qos(qos_id, False)
            self.helper.delete_qos(qos_id)
        else:
            self.helper.update_qos_fs(qos_id, fs_list)

    def update(self, fs_id, new_qos, qos_id):
        qos_info = self.helper.get_qos_info(qos_id)
        if self._check_qos_consistency(qos_info, new_qos):
            return

        self.remove(fs_id, qos_id, qos_info)
        self.add(new_qos, fs_id)
