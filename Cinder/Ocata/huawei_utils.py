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
import time

import six

from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import units
from oslo_utils import strutils

from cinder import context
from cinder import exception
from cinder.i18n import _
from cinder import objects
from cinder.volume.drivers.huawei import constants
from cinder.volume import qos_specs

LOG = logging.getLogger(__name__)


def encode_name(id_info):
    encoded_name = hashlib.md5(id_info.encode('utf-8')).hexdigest()
    prefix = id_info.split('-')[0] + '-'
    postfix = encoded_name[:constants.MAX_NAME_LENGTH - len(prefix)]
    return prefix + postfix


def old_encode_name(id_info):
    pre_name = id_info.split("-")[0]
    vol_encoded = six.text_type(hash(id_info))
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
    volume_size = constants.CAPACITY_UNIT  # 1G
    if int(volume.size) != 0:
        volume_size = int(volume.size) * constants.CAPACITY_UNIT

    return volume_size


def get_volume_metadata(volume):
    if isinstance(volume, objects.Volume):
        return volume.metadata
    if volume.get('volume_metadata'):
        volume_metadata = {}
        for item in volume.get('volume_metadata'):
            volume_metadata[item.get('key')] = item.get('value')
        return volume_metadata
    return {}


def set_volume_lun_wwn(model_update, lun_info, volume):
    metadata = get_volume_metadata(volume)
    metadata['lun_wwn'] = lun_info.get('WWN')
    model_update.update({"metadata": metadata})


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
    if isinstance(snapshot, objects.Snapshot):
        return snapshot.metadata

    if 'snapshot_metadata' in snapshot:
        metadata = snapshot.snapshot_metadata
        return {item['key']: item['value'] for item in metadata}

    return {}


def convert_connector_wwns(wwns):
    if wwns:
        return map(lambda x: x.lower(), wwns)
    return None


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

    if not lun_id and metadata.get(constants.HUAWEI_LUN_ID):
        if client.check_lun_exist(metadata.get(constants.HUAWEI_LUN_ID),
                                  metadata.get('huawei_lun_wwn')):
            lun_id = metadata.get(constants.HUAWEI_LUN_ID)

    # Judge whether this volume has experienced data migration or not
    if not lun_id:
        volume_name = encode_name(volume.name_id)
        lun_id = client.get_lun_id_by_name(volume_name)

    if not lun_id:
        volume_name = old_encode_name(volume.name_id)
        lun_id = client.get_lun_id_by_name(volume_name)

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
    return False


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


def check_group_volume_type_valid(opt):
    if opt.get('hypermetro') == 'true' and opt.get(
            'replication_enabled') == 'true':
        msg = _("Hypermetro and replication cannot be "
                "specified at the same volume_type.")
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)


def mask_dict_sensitive_info(data, secret="***"):
    # mask sensitive data in the dictionary
    if not isinstance(data, dict):
        return data

    out = {}
    for key, value in data.items():
        if isinstance(value, dict):
            value = mask_dict_sensitive_info(value, secret=secret)
        elif key in constants.SENSITIVE_KEYS:
            value = secret
        out[key] = value

    return strutils.mask_dict_password(out)


def mask_initiator_sensitive_info(data, sensitive_keys=None, need_mask_keys=None):
    """Mask iscsi/fc/nvme ini sensitive info by data type"""
    if isinstance(data, dict):
        return mask_initiator_dict_sensitive_info(data, sensitive_keys, need_mask_keys=need_mask_keys)
    elif isinstance(data, (list, tuple, set)):
        return mask_initiator_array_sensitive_info(data, sensitive_keys)
    else:
        return mask_initiator_string_sensitive_info(data)


def mask_initiator_dict_sensitive_info(data, sensitive_keys, need_mask_keys=None):
    """Mask iscsi/fc/nvme ini sensitive info with a dict type data"""
    out = {}
    for key, value in data.items():
        if isinstance(value, dict):
            value = mask_initiator_dict_sensitive_info(value, sensitive_keys, need_mask_keys)
        elif key in sensitive_keys and isinstance(value, (list, tuple, set)):
            value = mask_initiator_array_sensitive_info(value, sensitive_keys)
        elif key in sensitive_keys:
            value = mask_initiator_string_sensitive_info(value)
        if need_mask_keys and key in need_mask_keys:
            key = mask_initiator_string_sensitive_info(key)
        out[key] = value

    return out


def mask_initiator_array_sensitive_info(data, sensitive_keys):
    """Mask iscsi/fc/nvme ini sensitive info with a list type data"""
    out = []
    for sensitive_info in data:
        if isinstance(sensitive_info, (list, tuple, set)):
            value = mask_initiator_array_sensitive_info(sensitive_info, sensitive_keys)
        elif isinstance(sensitive_info, dict):
            value = mask_initiator_dict_sensitive_info(sensitive_info, sensitive_keys)
        else:
            value = mask_initiator_string_sensitive_info(sensitive_info)
        out.append(value)
    return out


def mask_initiator_string_sensitive_info(data):
    """Mask iscsi/fc/nvme ini sensitive info with a string type data"""
    secret_str = "******"
    if len(data) <= 6:
        return secret_str

    out_str = data[0:3] + secret_str + data[-3::]
    return out_str


def _check_and_set_qos_info(specs, qos):
    LOG.info('The QoS specs is: %s.', specs)

    for key, value in specs.items():
        if key in constants.QOS_IGNORED_PARAMS:
            LOG.info("this qos spec param %s is front-end qos param, "
                     "backend driver will ignore it", key)
            continue

        if key not in constants.QOS_SPEC_KEYS:
            msg = _('Invalid QoS %s specification.') % key
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if key != 'IOType' and int(value) <= 0:
            msg = _('QoS config is wrong. %s must > 0.') % key
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        qos[key.upper()] = value


def get_qos_by_volume_type(volume_type):
    # We prefer the qos_specs association
    # and override any existing extra-specs settings
    # if present.
    if not volume_type:
        return {}

    qos_specs_id = volume_type.get('qos_specs_id')
    if not qos_specs_id:
        return {}

    qos = {}
    ctxt = context.get_admin_context()
    qos_specs_info = qos_specs.get_qos_specs(ctxt, qos_specs_id)
    consumer = qos_specs_info.get('consumer')
    if consumer == 'front-end':
        return qos

    _check_and_set_qos_info(qos_specs_info.get('specs', {}), qos)

    if qos.get('IOTYPE') not in constants.QOS_IOTYPES:
        msg = _('IOType value must be in %(valid)s.'
                ) % {'valid': constants.QOS_IOTYPES}
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)

    if len(qos) < 2:
        msg = _('QoS policy must specify IOType and one of QoS specs.')
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)

    for upper_limit in constants.UPPER_LIMIT_KEYS:
        for lower_limit in constants.LOWER_LIMIT_KEYS:
            if upper_limit in qos and lower_limit in qos:
                msg = (_('QoS policy upper_limit and lower_limit '
                         'conflict, QoS policy: %(qos_policy)s.')
                       % {'qos_policy': qos})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

    return qos
