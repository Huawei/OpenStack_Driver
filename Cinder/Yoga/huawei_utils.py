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
import re

from oslo_log import log as logging
from oslo_utils import strutils
import tenacity as retry_module
import six

from cinder import context
from cinder import exception
from cinder.i18n import _
from cinder import objects
from cinder.objects import fields
from cinder.volume.drivers.huawei import constants
from cinder.volume import qos_specs
from cinder.volume import volume_types


LOG = logging.getLogger(__name__)


def encode_name(name):
    encoded_name = hashlib.md5(name.encode('utf-8')).hexdigest()
    prefix = name.split('-')[0] + '-'
    postfix = encoded_name[:constants.MAX_NAME_LENGTH - len(prefix)]
    return prefix + postfix


def old_encode_name(name):
    pre_name = name.split("-")[0]
    vol_encoded = six.text_type(hash(name))
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
    def _retry_on_result(result):
        return not result

    def _retry_on_exception():
        return False

    def _retry_use_retrying():
        ret = retry_module.Retrying(retry_on_result=_retry_on_result,
                                    retry_on_exception=_retry_on_exception,
                                    wait_fixed=interval * 1000,
                                    stop_max_delay=timeout * 1000)
        ret.call(func)

    def _retry_use_tenacity():
        ret = retry_module.Retrying(
            wait=retry_module.wait_fixed(interval),
            retry=retry_module.retry_if_result(_retry_on_result),
            stop=retry_module.stop_after_delay(timeout)
        )
        ret(func)

    _retry_use_tenacity()


def _get_volume_type(volume):
    if volume.volume_type:
        return volume.volume_type
    if volume.volume_type_id:
        return volume_types.get_volume_type(None, volume.volume_type_id)


def get_volume_params(volume, is_dorado_v6=False):
    volume_type = _get_volume_type(volume)
    return get_volume_type_params(volume_type, is_dorado_v6)


def get_volume_type_params(volume_type, is_dorado_v6=False):
    specs = {}
    if isinstance(volume_type, dict) and volume_type.get('extra_specs'):
        specs = volume_type['extra_specs']
    elif isinstance(volume_type, objects.VolumeType
                    ) and volume_type.extra_specs:
        specs = volume_type.extra_specs

    vol_params = get_volume_params_from_specs(specs)
    vol_params['qos'] = None

    if isinstance(volume_type, dict) and volume_type.get('qos_specs_id'):
        vol_params['qos'] = _get_qos_specs(volume_type['qos_specs_id'],
                                           is_dorado_v6)
    elif isinstance(volume_type, objects.VolumeType
                    ) and volume_type.qos_specs_id:
        vol_params['qos'] = _get_qos_specs(volume_type.qos_specs_id,
                                           is_dorado_v6)

    LOG.info('volume opts %s.', vol_params)
    return vol_params


def get_volume_params_from_specs(specs):
    opts = _get_opts_from_specs(specs)

    _verify_smartcache_opts(opts)
    _verify_smartpartition_opts(opts)
    _verify_smartthin_opts(opts)
    _verify_controller_opts(opts)
    _verify_application_type_opts(opts)

    return opts


def _get_bool_param(k, v):
    words = v.split()
    if len(words) == 2 and words[0] == '<is>':
        return strutils.bool_from_string(words[1], strict=True)

    msg = _("%(k)s spec must be specified as %(k)s='<is> True' "
            "or '<is> False'.") % {'k': k}
    LOG.error(msg)
    raise exception.InvalidInput(reason=msg)


def _get_replication_type_param(k, v):
    words = v.split()
    if len(words) == 2 and words[0] == '<in>':
        REPLICA_SYNC_TYPES = {'sync': constants.REPLICA_SYNC_MODEL,
                              'async': constants.REPLICA_ASYNC_MODEL}
        sync_type = words[1].lower()
        if sync_type in REPLICA_SYNC_TYPES:
            return REPLICA_SYNC_TYPES[sync_type]

    msg = _("replication_type spec must be specified as "
            "replication_type='<in> sync' or '<in> async'.")
    LOG.error(msg)
    raise exception.InvalidInput(reason=msg)


def _get_string_param(k, v):
    if not v:
        msg = _("%s spec must be specified as a string.") % k
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)
    return v


def _get_opts_from_specs(specs):
    """Get the well defined extra specs."""
    opts = {}

    opts_capability = {
        'capabilities:smarttier': (_get_bool_param, False),
        'capabilities:smartcache': (_get_bool_param, False),
        'capabilities:smartpartition': (_get_bool_param, False),
        'capabilities:thin_provisioning_support': (_get_bool_param, False),
        'capabilities:thick_provisioning_support': (_get_bool_param, False),
        'capabilities:hypermetro': (_get_bool_param, False),
        'capabilities:replication_enabled': (_get_bool_param, False),
        'replication_type': (_get_replication_type_param,
                             constants.REPLICA_ASYNC_MODEL),
        'smarttier:policy': (_get_string_param, None),
        'smartcache:cachename': (_get_string_param, None),
        'smartpartition:partitionname': (_get_string_param, None),
        'huawei_controller:controllername': (_get_string_param, None),
        'capabilities:dedup': (_get_bool_param, None),
        'capabilities:compression': (_get_bool_param, None),
        'capabilities:huawei_controller': (_get_bool_param, False),
        'capabilities:huawei_application_type': (_get_bool_param, False),
        'huawei_application_type:applicationname': (_get_string_param, None),
    }

    def _get_opt_key(spec_key):
        key_split = spec_key.split(':')
        if len(key_split) == 1:
            return key_split[0]
        else:
            return key_split[1]

    for spec_key in opts_capability:
        opt_key = _get_opt_key(spec_key)
        opts[opt_key] = opts_capability[spec_key][1]

    for key, value in six.iteritems(specs):
        if key not in opts_capability:
            continue

        func = opts_capability[key][0]
        opt_key = _get_opt_key(key)
        opts[opt_key] = func(key, value)

    return opts


def _get_qos_specs(qos_specs_id, is_dorado_v6):
    ctxt = context.get_admin_context()
    specs = qos_specs.get_qos_specs(ctxt, qos_specs_id)
    if specs is None:
        return {}

    if specs.get('consumer') == 'front-end':
        return {}

    kvs = specs.get('specs', {})
    LOG.info('The QoS specs is: %s.', kvs)

    qos = {'IOTYPE': kvs.pop('IOType', None)}

    if qos['IOTYPE'] not in constants.QOS_IOTYPES:
        msg = _('IOType must be in %(types)s.'
                ) % {'types': constants.QOS_IOTYPES}
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)

    for k, v in kvs.items():
        if k not in constants.QOS_SPEC_KEYS:
            msg = _('QoS key %s is not valid.') % k
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if int(v) <= 0:
            msg = _('QoS value for %s must > 0.') % k
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        qos[k.upper()] = v

    if len(qos) < 2:
        msg = _('QoS policy must specify both IOType and one another '
                'qos spec, got policy: %s.') % qos
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)

    if is_dorado_v6:
        return qos

    qos_keys = set(qos.keys())
    if (qos_keys & set(constants.UPPER_LIMIT_KEYS) and
            qos_keys & set(constants.LOWER_LIMIT_KEYS)):
        msg = _('QoS policy upper limit and lower limit '
                'conflict, QoS policy: %s.') % qos
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)

    return qos


def _verify_smartthin_opts(opts):
    if (opts['thin_provisioning_support'] and
            opts['thick_provisioning_support']):
        msg = _('Cannot set thin and thick at the same time.')
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)
    elif opts['thin_provisioning_support']:
        opts['LUNType'] = constants.THIN_LUNTYPE
    elif opts['thick_provisioning_support']:
        opts['LUNType'] = constants.THICK_LUNTYPE


def _verify_smartcache_opts(opts):
    if opts['smartcache'] and not opts['cachename']:
        msg = _('Cache name is not specified, please set '
                'smartcache:cachename in extra specs.')
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)


def _verify_application_type_opts(opts):
    if opts['huawei_application_type'] and not opts['applicationname']:
        msg = _('WorkloadType name is None, please set '
                'huawei_application_type:applicationname in extra specs.')
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)


def _verify_controller_opts(opts):
    if opts['huawei_controller'] and not opts['controllername']:
        msg = _('Controller name is None, please set '
                'huawei_controller:controllername in extra specs.')
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)


def _verify_smartpartition_opts(opts):
    if opts['smartpartition'] and not opts['partitionname']:
        msg = _('Partition name is not specified, please set '
                'smartpartition:partitionname in extra specs.')
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)


def wait_lun_online(client, lun_id, wait_interval=None, wait_timeout=None):
    def _lun_online():
        result = client.get_lun_info_by_id(lun_id)
        if result['HEALTHSTATUS'] not in (constants.STATUS_HEALTH,
                                          constants.STATUS_INITIALIZE):
            err_msg = _('LUN %s is abnormal.') % lun_id
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        if result['RUNNINGSTATUS'] in (constants.LUN_INITIALIZING,
                                       constants.STATUS_INITIALIZE):
            return False

        return True

    if not wait_interval:
        wait_interval = constants.DEFAULT_WAIT_INTERVAL
    if not wait_timeout:
        wait_timeout = wait_interval * 10

    wait_for_condition(_lun_online, wait_interval, wait_timeout)


def is_not_exist_exc(exc):
    msg = getattr(exc, 'msg', '')
    return 'not exist' in msg


def to_string(**kwargs):
    return json.dumps(kwargs) if kwargs else ''


def to_dict(text):
    return json.loads(text) if text else {}


def get_volume_private_data(volume):
    if not volume.provider_location:
        return {}

    try:
        info = json.loads(volume.provider_location)
    except Exception:
        LOG.exception("Decode provider_location error")
        return {}

    if isinstance(info, dict):
        if "huawei" in volume.provider_location:
            info['hypermetro'] = (info.get('hypermetro_id')
                                  or info.get('hypermetro'))
            return info
        else:
            return {}

    # To keep compatible with old driver version
    return {'huawei_lun_id': six.text_type(info),
            'huawei_lun_wwn': volume.admin_metadata.get('huawei_lun_wwn'),
            'huawei_sn': volume.metadata.get('huawei_sn'),
            'hypermetro': True if volume.metadata.get(
                'hypermetro_id') else False,
            }


def get_volume_metadata(volume):
    if isinstance(volume, objects.Volume):
        return volume.metadata
    if volume.get('volume_metadata'):
        return {item['key']: item['value'] for item in
                volume['volume_metadata']}
    return {}


def get_replication_data(volume):
    if not volume.replication_driver_data:
        return {}

    return json.loads(volume.replication_driver_data)


def get_snapshot_private_data(snapshot):
    if not snapshot.provider_location:
        return {}

    info = json.loads(snapshot.provider_location)
    if isinstance(info, dict):
        return info

    # To keep compatible with old driver version
    return {'huawei_snapshot_id': six.text_type(info),
            'huawei_snapshot_wwn': snapshot.metadata.get(
                'huawei_snapshot_wwn'),
            }


def get_external_lun_info(client, external_ref):
    lun_info = None
    if 'source-id' in external_ref:
        lun = client.get_lun_info_by_id(external_ref['source-id'])
        lun_info = client.get_lun_info_by_name(lun['NAME'])
    elif 'source-name' in external_ref:
        lun_info = client.get_lun_info_by_name(external_ref['source-name'])

    return lun_info


def get_external_snapshot_info(client, external_ref):
    snapshot_info = None
    if 'source-id' in external_ref:
        snapshot_info = client.get_snapshot_info_by_id(
            external_ref['source-id'])
    elif 'source-name' in external_ref:
        snapshot_info = client.get_snapshot_info_by_name(
            external_ref['source-name'])

    return snapshot_info


def get_lun_info(client, volume):
    metadata = get_volume_private_data(volume)

    volume_name = encode_name(volume.id)
    lun_info = client.get_lun_info_by_name(volume_name)

    # If new encoded way not found, try the old encoded way.
    if not lun_info:
        volume_name = old_encode_name(volume.id)
        lun_info = client.get_lun_info_by_name(volume_name)

    if not lun_info and metadata.get('huawei_lun_id'):
        lun_info = client.get_lun_info_filter_id(metadata['huawei_lun_id'])

    if lun_info and ('huawei_lun_wwn' in metadata and
                     lun_info.get('WWN') != metadata['huawei_lun_wwn']):
        lun_info = None

    # Judge whether this volume has experienced data migration or not
    if not lun_info:
        volume_name = encode_name(volume.name_id)
        lun_info = client.get_lun_info_by_name(volume_name)

    if not lun_info:
        volume_name = old_encode_name(volume.name_id)
        lun_info = client.get_lun_info_by_name(volume_name)

    return lun_info


def get_snapshot_info(client, snapshot):
    name = encode_name(snapshot.id)
    snapshot_info = client.get_snapshot_info_by_name(name)

    # If new encoded way not found, try the old encoded way.
    if not snapshot_info:
        name = old_encode_name(snapshot.id)
        snapshot_info = client.get_snapshot_info_by_name(name)

    return snapshot_info


def get_host_id(client, host_name):
    encoded_name = encode_host_name(host_name)
    host_id = client.get_host_id_by_name(encoded_name)
    if encoded_name == host_name:
        return host_id

    if not host_id:
        encoded_name = old_encode_host_name(host_name)
        host_id = client.get_host_id_by_name(encoded_name)

    return host_id


def get_hypermetro_group(client, group_id):
    encoded_name = encode_name(group_id)
    group = client.get_metrogroup_by_name(encoded_name)
    if not group:
        encoded_name = old_encode_name(group_id)
        group = client.get_metrogroup_by_name(encoded_name)
    return group


def get_replication_group(client, group_id):
    encoded_name = encode_name(group_id)
    group = client.get_replication_group_by_name(encoded_name)
    if not group:
        encoded_name = old_encode_name(group_id)
        group = client.get_replication_group_by_name(encoded_name)
    return group


def get_volume_model_update(volume, **kwargs):
    private_data = get_volume_private_data(volume)

    if kwargs.get('hypermetro_id'):
        private_data['hypermetro'] = True
    else:
        private_data['hypermetro'] = False
    if 'hypermetro_id' in private_data:
        private_data.pop('hypermetro_id')
        private_data['hypermetro'] = False

    if 'huawei_lun_id' in kwargs:
        private_data['huawei_lun_id'] = kwargs['huawei_lun_id']
    if 'huawei_lun_wwn' in kwargs:
        private_data['huawei_lun_wwn'] = kwargs['huawei_lun_wwn']
    if 'huawei_sn' in kwargs:
        private_data['huawei_sn'] = kwargs['huawei_sn']

    model_update = {'provider_location': to_string(**private_data)}

    if kwargs.get('replication_id'):
        model_update['replication_driver_data'] = to_string(
            pair_id=kwargs.get('replication_id'))
        model_update['replication_status'] = fields.ReplicationStatus.ENABLED
    else:
        model_update['replication_driver_data'] = None
        model_update['replication_status'] = fields.ReplicationStatus.DISABLED

    return model_update


def get_group_type_params(group, is_dorado_v6=False):
    opts = []
    for volume_type in group.volume_types:
        opt = get_volume_type_params(volume_type, is_dorado_v6)
        opts.append(opt)
    return opts


def get_hypermetro(client, volume):
    lun_name = encode_name(volume.id)
    hypermetro = client.get_hypermetro_by_lun_name(lun_name)
    return hypermetro


def _set_config_info(ini, find_info, tmp_find_info):
    if find_info is None and tmp_find_info:
        find_info = tmp_find_info

    if ini:
        config = ini
    elif find_info:
        config = find_info
    else:
        config = {}
    return config


def find_config_info(config_info, connector=None, initiator=None):
    if initiator:
        ini = config_info['initiators'].get(initiator)
        connector = {} if not connector else connector
    elif connector:
        ini = config_info['initiators'].get(connector['initiator'])
    else:
        return {}

    find_info = None
    tmp_find_info = None
    if not ini:
        for item in config_info['initiators']:
            ini_info = config_info['initiators'][item]
            if ini_info.get('HostName'):
                if ini_info.get('HostName') == '*':
                    tmp_find_info = ini_info
                elif re.search(ini_info.get('HostName'), connector.get('host', '')):
                    find_info = ini_info
                    break

    return _set_config_info(ini, find_info, tmp_find_info)


def is_support_clone_pair(client):
    array_info = client.get_array_info()
    version_info = array_info['PRODUCTVERSION']
    if version_info >= constants.SUPPORT_CLONE_PAIR_VERSION:
        return True


def need_migrate(volume, host, new_opts, orig_lun_info):
    if volume.host != host['host']:
        return True
    elif ('LUNType' in new_opts and
          new_opts['LUNType'] != orig_lun_info['ALLOCTYPE']):
        return True
    elif (new_opts['compression'] and
          not (orig_lun_info.get('ENABLECOMPRESSION') == 'true')):
        return True
    elif (new_opts['dedup'] and
          not (orig_lun_info.get('ENABLESMARTDEDUP') == 'true')):
        return True
    return False


def remove_lun_from_lungroup(client, lun_id, force_delete_volume):
    lun_group_ids = client.get_lungroup_ids_by_lun_id(lun_id)
    if lun_group_ids:
        if force_delete_volume:
            for lun_group_id in lun_group_ids:
                client.remove_lun_from_lungroup(lun_group_id, lun_id,
                                                constants.LUN_TYPE)
        elif len(lun_group_ids) == 1:
            client.remove_lun_from_lungroup(lun_group_ids[0], lun_id,
                                            constants.LUN_TYPE)


def get_mapping_info(client, lun_id):
    mappingview_id, lungroup_id, hostgroup_id, portgroup_id, host_id = (
        None, None, None, None, None)
    lungroup_ids = client.get_lungroup_ids_by_lun_id(lun_id)
    if len(lungroup_ids) == 1:
        lungroup_id = lungroup_ids[0]
        mapping_infos = client.get_mappingview_by_lungroup_id(lungroup_id)
        if len(mapping_infos) == 1:
            mapping_info = mapping_infos[0]
            mappingview_id = mapping_info.get('ID')
            hostgroup_id = mapping_info.get('hostGroupId')
            portgroup_id = mapping_info.get('portGroupId')
            host_ids = client.get_host_by_hostgroup_id(hostgroup_id)
            if len(host_ids) == 1:
                host_id = host_ids[0]
    elif len(lungroup_ids) < 1:
        LOG.warning('lun %s is not added to the lungroup', lun_id)
    else:
        LOG.warning('lun %s is is added to multiple lungroups', lun_id)

    return mappingview_id, lungroup_id, hostgroup_id, portgroup_id, host_id


def check_volume_type_valid(opt):
    if opt.get('hypermetro') and opt.get('replication_enabled'):
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
