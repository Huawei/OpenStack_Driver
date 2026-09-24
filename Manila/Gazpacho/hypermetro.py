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
from oslo_log import log

from manila import exception
from manila.i18n import _
from manila.share.drivers.huawei import constants
from manila.share.drivers.huawei import huawei_utils

LOG = log.getLogger(__name__)


class HyperPairManager(object):
    def __init__(self, helper, configuration):
        self.helper = helper
        self.configuration = configuration
        self.is_dorado_v6 = huawei_utils.is_dorado_v6(self.helper)

    def _get_storage_pool(self, params):
        selected_pool = {}
        thin_support = params["ALLOCTYPE"] == "1"
        max_over_subscription_ratio = self.configuration.safe_get(
            'max_over_subscription_ratio')
        for pool_name in self.configuration.storage_pools:
            pool_info = self.helper.get_pool_by_name(pool_name)
            if not pool_info:
                LOG.warning("The pool %s is not valid, please check on "
                            "the storage.", pool_name)
                continue

            _free = float(pool_info['USERFREECAPACITY'])
            _total = float(pool_info['USERTOTALCAPACITY'])
            _provisioned = float(pool_info['TOTALFSCAPACITY'])

            if thin_support:
                _free = (max_over_subscription_ratio * _total - _provisioned)

            if (_free >= float(params[constants.FILESYSTEM_CAPACITY]) and
                    _free > selected_pool.get(constants.FILESYSTEM_CAPACITY, 0)):
                selected_pool.update({"pool_id": pool_info["ID"],
                                      constants.FILESYSTEM_CAPACITY: _free})

        if not selected_pool:
            msg = _("There is no valid pool to create FS %s" % params)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        return selected_pool.get("pool_id")

    def create_remote_filesystem(self, params):
        pool_id = self._get_storage_pool(params)
        params.update({"PARENTID": pool_id})
        fs_id = self.helper.create_filesystem(params)
        huawei_utils.wait_fs_online(
            self.helper, fs_id, self.configuration.wait_interval,
            self.configuration.timeout)
        return fs_id

    def delete_remote_filesystem(self, params):
        self.helper.delete_filesystem(params)

    def update_filesystem(self, fs_id, params):
        self.helper.update_filesystem(fs_id, params)

    def create_metro_pair(self, domain_name, local_fs_id,
                          remote_fs_id, vstore_pair_id):
        try:
            domain_id = self.get_domain_id(domain_name)
            # Create a  HyperMetro Pair
            pair_params = {
                "DOMAINID": domain_id,
                "HCRESOURCETYPE": 2,
                "LOCALOBJID": local_fs_id,
                "REMOTEOBJID": remote_fs_id,
                "VSTOREPAIRID": vstore_pair_id,
            }
            pair_info = self.helper.create_hypermetro_pair(pair_params)
        except Exception:
            LOG.exception("Failed to create HyperMetro pair for share %s.",
                          local_fs_id)
            raise

        if not self.is_dorado_v6:
            self._sync_metro_pair(pair_info['ID'])
        return pair_info

    def get_domain_id(self, domain_name):
        if self.is_dorado_v6:
            return self._get_new_domain_id(domain_name)
        else:
            return self._get_domain_id(domain_name)

    def _get_domain_id(self, domain_name):
        domain_id = self.helper.get_hypermetro_domain_id(domain_name)
        if not domain_id:
            err_msg = _("HyperMetro domain cannot be found.")
            LOG.error(err_msg)
            raise exception.ShareBackendException(msg=err_msg)
        return domain_id

    def _get_new_domain_id(self, domain_name):
        fs_domain_info = self.helper.get_hypermetro_domain_info(domain_name)
        if not fs_domain_info:
            err_msg = _("HyperMetro domain %s cannot be found.") % domain_name
            LOG.error(err_msg)
            raise exception.ShareBackendException(msg=err_msg)
        return fs_domain_info["ID"]

    def _sync_metro_pair(self, pair_id):
        try:
            self.helper.sync_hypermetro_pair(pair_id)
        except Exception as err:
            LOG.warning('Failed to sync HyperMetro pair %(id)s. '
                        'Reason: %(err)s',
                        {'id': pair_id, 'err': err})
            raise

    def delete_metro_pair(self, metro_id):
        try:
            if not self.is_dorado_v6:
                self._suspend_metro_pair(metro_id)
            self.helper.delete_hypermetro_pair(metro_id)
        except Exception as err:
            LOG.exception('Failed to delete HyperMetro pair %(id)s. '
                          'Reason: %(err)s',
                          {'id': metro_id, 'err': err})
            raise

        huawei_utils.wait_hypermetro_pair_delete(
            self.helper, metro_id, self.configuration.wait_interval,
            self.configuration.timeout)

    def _suspend_metro_pair(self, pair_id):
        try:
            metro_info = self._get_metro_pair_info(pair_id)
            if not metro_info:
                msg = (_("The hypermetro pair %(pair_id)s does not exist.") %
                       {"pair_id": pair_id})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

            if metro_info["RUNNINGSTATUS"] in (
                    constants.METRO_RUNNING_STATUS_NORMAL,
                    constants.METRO_RUNNING_STATUS_SYNCING,
                    constants.METRO_RUNNING_STATUS_TO_BE_SYNC):
                self.helper.suspend_hypermetro_pair(pair_id)
            else:
                LOG.warning("Suspend the HyperMetro pair %s when it is in the "
                            "Normal, Synchronizing, or To Be Synchronized "
                            "state.", pair_id)
                return
        except Exception as err:
            LOG.exception('Failed to suspend HyperMetro pair %(id)s. '
                          'Reason: %(err)s',
                          {'id': pair_id, 'err': err})
            raise

    def _get_metro_pair_info(self, pair_id):
        try:
            pair_info = self.helper.get_hypermetro_pair_by_id(pair_id)
        except Exception as err:
            LOG.exception('Failed to get HyperMetro pair %(id)s. '
                          'Reason: %(err)s',
                          {'id': pair_id, 'err': err})
            raise
        return pair_info

    def check_remote_metro_info(self, domain_name, local_vstore,
                                remote_vstor, local_vstore_pair_id):
        remote_vstore_pair_id = huawei_utils.get_hypermetro_vstore_id(
            self.helper, domain_name, local_vstore, remote_vstor)
        if local_vstore_pair_id != remote_vstore_pair_id:
            msg = _("The local vStore pair and remote vStore pair are "
                    "inconsistent")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        return remote_vstore_pair_id

    def get_remote_fs_info(self, share_name):
        self.helper.get_fs_info_by_name(share_name)
