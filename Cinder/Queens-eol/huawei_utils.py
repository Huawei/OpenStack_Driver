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

import hashlib
import json
import six
import time

from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import units

from cinder import exception
from cinder.i18n import _
from cinder import objects
from cinder.volume.drivers.huawei import constants

LOG = logging.getLogger(__name__)


def encode_name(id):
    encoded_name = hashlib.md5(id.encode('utf-8')).hexdigest()
    prefix = id.split('-')[0] + '-'
    postfix = encoded_name[:constants.MAX_NAME_LENGTH - len(prefix)]
    return prefix + postfix


def old_encode_name(id):
    pre_name = id.split("-")[0]
    vol_encoded = six.text_type(hash(id))
    if vol_encoded.startswith('-'):
        newuuid = pre_name + vol_encoded
    else:
        newuuid = pre_name + '-' + vol_encoded
    return newuuid


def encode_host_name(name):
    if name and len(name) > constants.MAX_NAME_LENGTH:
        encoded_name = hashlib.md5(name.encode('utf-8')).hexdigest()
        return encoded_name[:constants.MAX_NAME_LENGTH]
    return name


def old_encode_host_name(name):
    if name and len(name) > constants.MAX_NAME_LENGTH:
        name = six.text_type(hash(name))
    return name


def wait_for_condition(func, interval, timeout):
    start_time = time.time()

    def _inner():
        try:
            res = func()
        except Exception as ex:
            raise exception.VolumeBackendAPIException(data=ex)

        if res:
            raise loopingcall.LoopingCallDone()

        if int(time.time()) - start_time > timeout:
            msg = (_('wait_for_condition: %s timed out.')
                   % func.__name__)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    timer = loopingcall.FixedIntervalLoopingCall(_inner)
    timer.start(interval=interval).wait()


def get_volume_size(volume):
    """Calculate the volume size.

    We should divide the given volume size by 512 for the 18000 system
    calculates volume size with sectors, which is 512 bytes.
    """
    volume_size = units.Gi / 512  # 1G
    if int(volume.size) != 0:
        volume_size = int(volume.size) * units.Gi / 512

    return volume_size


def get_volume_metadata(volume):
    if isinstance(volume, objects.Volume):
        return volume.metadata
    if volume.get('volume_metadata'):
        return {item['key']: item['value'] for item in
                volume['volume_metadata']}
    return {}


def get_admin_metadata(volume):
    admin_metadata = {}
    if 'admin_metadata' in volume:
        admin_metadata = volume.admin_metadata
    elif 'volume_admin_metadata' in volume:
        metadata = volume.get('volume_admin_metadata', [])
        admin_metadata = {item['key']: item['value'] for item in metadata}

    LOG.debug("Volume ID: %(id)s, admin_metadata: %(admin_metadata)s.",
              {"id": volume.id, "admin_metadata": admin_metadata})
    return admin_metadata


def get_snapshot_metadata_value(snapshot):
    if type(snapshot) is objects.Snapshot:
        return snapshot.metadata

    if 'snapshot_metadata' in snapshot:
        metadata = snapshot.snapshot_metadata
        return {item['key']: item['value'] for item in metadata}

    return {}


def convert_connector_wwns(wwns):
    if wwns:
        return map(lambda x: x.lower(), wwns)


def to_string(**kwargs):
    return json.dumps(kwargs) if kwargs else ''


def get_lun_metadata(volume):
    if not volume.provider_location:
        return {}

    try:
        info = json.loads(volume.provider_location)
    except Exception as err:
        LOG.warning("get_lun_metadata get provider_location error, params: "
                    "%(loc)s, reason: %(err)s",
                    {"loc": volume.provider_location, "err": err})
        return {}

    if isinstance(info, dict):
        if "huawei" in volume.provider_location:
            return info
        else:
            return {}

    # To keep compatible with old driver version
    admin_metadata = get_admin_metadata(volume)
    metadata = get_volume_metadata(volume)
    return {'huawei_lun_id': six.text_type(info),
            'huawei_lun_wwn': admin_metadata.get('huawei_lun_wwn'),
            'huawei_sn': metadata.get('huawei_sn'),
            'hypermetro': True if metadata.get('hypermetro_id') else False,
            }


def get_snapshot_metadata(snapshot):
    if not snapshot.provider_location:
        return {}

    info = json.loads(snapshot.provider_location)
    if isinstance(info, dict):
        return info

    # To keep compatible with old driver version
    metadata = get_snapshot_metadata_value(snapshot)
    return {'huawei_snapshot_id': six.text_type(info),
            'huawei_snapshot_wwn': metadata.get('huawei_snapshot_wwn'),
            }


def get_volume_lun_id(client, volume):
    metadata = get_lun_metadata(volume)

    # First try the new encoded way.
    volume_name = encode_name(volume.id)
    lun_id = client.get_lun_id_by_name(volume_name)

    # If new encoded way not found, try the old encoded way.
    if not lun_id:
        volume_name = old_encode_name(volume.id)
        lun_id = client.get_lun_id_by_name(volume_name)

    # Judge whether this volume has experienced data migration or not
    if not lun_id:
        volume_name = encode_name(volume.name_id)
        lun_id = client.get_lun_id_by_name(volume_name)

    if not lun_id:
        volume_name = old_encode_name(volume.name_id)
        lun_id = client.get_lun_id_by_name(volume_name)

    if not lun_id:
        lun_id = metadata.get('huawei_lun_id')

    return lun_id, metadata.get('huawei_lun_wwn')


def get_snapshot_id(client, snapshot):
    metadata = get_snapshot_metadata(snapshot)
    snapshot_id = metadata.get('huawei_snapshot_id')

    # First try the new encoded way.
    if not snapshot_id:
        name = encode_name(snapshot.id)
        snapshot_id = client.get_snapshot_id_by_name(name)

    # If new encoded way not found, try the old encoded way.
    if not snapshot_id:
        name = old_encode_name(snapshot.id)
        snapshot_id = client.get_snapshot_id_by_name(name)

    return snapshot_id, metadata.get('huawei_snapshot_wwn')


def get_host_id(client, host_name):
    encoded_name = encode_host_name(host_name)
    host_id = client.get_host_id_by_name(encoded_name)
    if encoded_name == host_name:
        return host_id

    if not host_id:
        encoded_name = old_encode_host_name(host_name)
        host_id = client.get_host_id_by_name(encoded_name)

    return host_id


def check_feature_available(feature_status, features):
    for f in features:
        if feature_status.get(f) in constants.AVAILABLE_FEATURE_STATUS:
            return True

    return False


def get_apply_type_id(opts):
    if opts['huawei_application_type'] == 'true':
        if not opts['applicationname']:
            raise exception.InvalidInput(
                reason=_('WorkloadType name is None, please set huawei_'
                         'application_type:applicationname in extra specs.'))
        else:
            opts['application_type'] = opts['applicationname']
    else:
        opts['application_type'] = None

    return opts


def is_support_clone_pair(client):
    array_info = client.get_array_info()
    version_info = array_info['PRODUCTVERSION']
    if version_info >= constants.SUPPORT_CLONE_PAIR_VERSION:
        return True


def remove_lun_from_lungroup(client, lun_id, force_delete_volume):
    lun_group_ids = client.get_lungroupids_by_lunid(lun_id)
    if lun_group_ids:
        if force_delete_volume:
            for lun_group_id in lun_group_ids:
                client.remove_lun_from_lungroup(lun_group_id, lun_id,
                                                constants.LUN_TYPE)
        elif len(lun_group_ids) == 1:
            client.remove_lun_from_lungroup(lun_group_ids[0], lun_id,
                                            constants.LUN_TYPE)


def get_mapping_info(client, lun_id):
    host_name = None
    host_id = None
    lungroup_ids = client.get_lungroupids_by_lunid(lun_id)
    if len(lungroup_ids) == 1:
        lungroup_id = lungroup_ids[0]
        mapping_infos = client.get_mappingview_by_lungroup_id(lungroup_id)
        if len(mapping_infos) == 1:
            mapping_info = mapping_infos[0]
            hostgroup_id = mapping_info.get('hostGroupId')
            hosts = client.get_host_by_hostgroup_id(hostgroup_id)
            if len(hosts) == 1:
                host_name = hosts[0]['NAME']
                host_id = hosts[0]['ID']
    elif len(lungroup_ids) < 1:
        LOG.warning('lun %s is not added to the lungroup', lun_id)
    else:
        LOG.warning('lun %s is is added to multiple lungroups', lun_id)

    return host_name, host_id


def get_iscsi_mapping_info(client, lun_id):
    host_name, host_id = get_mapping_info(client, lun_id)
    iscsi_initiator = None
    initiators = client.get_host_iscsi_initiators(host_id)
    if len(initiators) == 1:
        iscsi_initiator = initiators[0]

    return host_name, iscsi_initiator


def get_fc_mapping_info(client, lun_id):
    host_name, host_id = get_mapping_info(client, lun_id)
    initiators = client.get_host_fc_initiators(host_id)

    return host_name, initiators


def is_snapshot_rollback_available(client, snapshot_id):
    snapshot_info = client.get_snapshot_info(snapshot_id)
    running_status = snapshot_info.get("RUNNINGSTATUS")
    health_status = snapshot_info.get("HEALTHSTATUS")
    if running_status not in (
            constants.SNAPSHOT_RUNNING_STATUS_ACTIVATED,
            constants.SNAPSHOT_RUNNING_STATUS_ROLLINGBACK):
        err_msg = (_("The running status %(status)s of snapshot %(name)s.")
                   % {"status": running_status, "name": snapshot_id})
        LOG.error(err_msg)
        raise exception.InvalidSnapshot(reason=err_msg)
    if health_status not in (constants.SNAPSHOT_HEALTH_STATUS_NORMAL, ):
        err_msg = (_("The health status %(status)s of snapshot %(name)s.")
                   % {"status": running_status, "name": snapshot_id})
        LOG.error(err_msg)
        raise exception.InvalidSnapshot(reason=err_msg)
    if constants.SNAPSHOT_RUNNING_STATUS_ACTIVATED == snapshot_info.get(
            'RUNNINGSTATUS'):
        return True
    return False
