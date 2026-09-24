# Copyright (c) 2026 Huawei Technologies Co., Ltd.
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

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_driver
from cinder.volume.drivers.huawei import huawei_utils
from cinder.volume.drivers.huawei import smartx
from cinder.volume.drivers.huawei import ocean_disk_rest_client

LOG = logging.getLogger(__name__)


class OceanDiskBaseDriver(object):
    """Base driver for OceanDisk storage arrays.

    Inherits from HuaweiBaseDriver and overrides do_setup() to inject
    OceanDiskRestClientExtend which uses /namespace endpoints instead of /lun.
    """

    def do_setup(self, context):
        self.huawei_conf.update_config_value()

        self.get_local_and_remote_dev_conf()
        client_conf, replica_client_conf = (
            self.get_local_and_remote_client_conf())

        if not client_conf:
            msg = _('Get active client failed.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self.client = ocean_disk_rest_client.OceanDiskRestClientExtend(
            self.configuration, **client_conf)
        self.sn = self.client.login()
        self.client.check_storage_pools()
        self.is_dorado_v6 = True

    def delete_volume(self, volume):
        """Delete a volume.

        Three steps:
        Firstly, remove associate from QoS policy.
        Secondly, remove associate from lungroup.
        Thirdly, remove the lun.
        """
        lun_id = self._check_volume_exist_on_array(volume, constants.VOLUME_NOT_EXISTS_WARN)
        if not lun_id:
            return

        qos_id = self.client.get_qosid_by_lunid(lun_id)
        if qos_id:
            smart_qos = smartx.SmartQos(self.client)
            smart_qos.remove(qos_id, lun_id)
        self._delete_volume(volume, lun_id)

    def _add_extend_type_to_volume(self, volume, volume_type, opts, lun_params,
                                   lun_info, is_sync=False):
        """
        1. QoS are supported.
        2. Currently, hypermetro and replication are not supported.
        """
        lun_id = lun_info['ID']
        lun_params.update({"CAPACITY": huawei_utils.get_volume_size(volume)})

        qos = huawei_utils.get_qos_by_volume_type(volume_type)
        if qos:
            smart_qos = smartx.SmartQos(self.client)
            smart_qos.add(qos, lun_id)

        metro_id = None
        if opts.get('hypermetro') == 'true':
            raise exception.InvalidInput("OceanDisk Storage not supported hypermetro")

        replica_info = {}
        if opts.get('replication_enabled') == 'true':
            raise exception.InvalidInput("OceanDisk Storage not supported replication")

        return metro_id, replica_info


class OceanDiskISCSIDriver(OceanDiskBaseDriver, huawei_driver.HuaweiISCSIDriver):
    """iSCSI driver for OceanDisk storage arrays.

    Inherits from OceanDiskBaseDriver (for /namespace endpoints) and
    HuaweiISCSIDriver (for iSCSI-specific logic).
    """

    def __init__(self, *args, **kwargs):
        super(OceanDiskISCSIDriver, self).__init__(*args, **kwargs)


class OceanDiskFCDriver(OceanDiskBaseDriver, huawei_driver.HuaweiFCDriver):
    """FC driver for OceanDisk storage arrays.

    Inherits from OceanDiskBaseDriver (for /namespace endpoints) and
    HuaweiFCDriver (for FC-specific logic).
    """

    def __init__(self, *args, **kwargs):
        super(OceanDiskFCDriver, self).__init__(*args, **kwargs)
