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
import random
import string

import netaddr
from oslo_log import log
from oslo_utils import strutils
import retrying as retry_module

from manila import exception
from manila.i18n import _
from manila.share.drivers.huawei import constants
from manila.share import share_types

LOG = log.getLogger(__name__)


def get_share_extra_specs_params(type_id, is_dorado=False):
    specs = {}
    if type_id:
        specs = share_types.get_share_type_extra_specs(type_id)

    opts = _get_opts_from_specs(specs, is_dorado)
    _get_smartprovisioning_opts(opts)
    _check_smartcache_opts(opts)
    _check_smartpartition_opts(opts)
    _get_qos_opts(opts)

    LOG.info('Get share type extra specs: %s', opts)
    return opts


def get_share_privilege(type_id):
    specs = {}
    if type_id:
        specs = share_types.get_share_type_extra_specs(type_id)

    share_privilege = {
        'huawei_share_privilege:sync': _get_string_param,
        'huawei_share_privilege:allsquash': _get_string_param,
        'huawei_share_privilege:rootsquash': _get_string_param,
        'huawei_share_privilege:secure': _get_string_param,
    }

    opts = {}
    for spec_key in specs:
        key = spec_key.lower()
        if share_privilege.get(key):
            opt_key = _get_opt_key(key)
            opts[opt_key.upper()] = share_privilege[key](key, specs[spec_key])

    return opts


def _get_opt_key(spec_key):
    key_split = spec_key.split(':')
    if len(key_split) == 1:
        return key_split[0]
    else:
        return key_split[1]


def _get_bool_param(key, value):
    words = value.split()
    if len(words) == 2 and words[0] == '<is>':
        return strutils.bool_from_string(words[1], strict=True)

    msg = _("%(key)s spec must be specified as %(key)s='<is> True' "
            "or '<is> False'.") % {'key': key}
    LOG.error(msg)
    raise exception.InvalidInput(reason=msg)


def _get_string_param(key, value):
    if not value:
        msg = _("%s spec must be specified as a string.") % key
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)
    return value


def _get_snapshot_dir_param(key, value):
    value = _get_string_param(key, value)
    if value.lower() not in ('true', 'false'):
        err_msg = _("The show_snapshot_dir value consists "
                    "can only be 'true' or 'false'")
        LOG.error(err_msg)
        raise exception.InvalidInput(reason=err_msg)
    return value


def _get_integer_param(key, value):
    if value and value.isdigit():
        return int(value)

    msg = _("%s spec must be specified as an integer.") % key
    LOG.error(msg)
    raise exception.InvalidInput(reason=msg)


def _get_opts_from_specs(specs, is_dorado):
    default_support = True if is_dorado else False
    opts_capability = {
        'capabilities:dedupe': (_get_bool_param, False),
        'capabilities:compression': (_get_bool_param, default_support),
        'capabilities:huawei_smartcache': (_get_bool_param, False),
        'capabilities:huawei_smartpartition': (_get_bool_param, False),
        'capabilities:thin_provisioning': (_get_bool_param, default_support),
        'capabilities:qos': (_get_bool_param, False),
        'capabilities:hypermetro': (_get_bool_param, False),
        'huawei_smartcache:cachename': (_get_string_param, None),
        'huawei_smartpartition:partitionname': (_get_string_param, None),
        'huawei_sectorsize:sectorsize': (_get_string_param, None),
        'huawei_controller:controllername': (_get_string_param, None),
        'huawei_unixpermission:unix_permission': (_get_string_param, None),
        'huawei_snapshotreserveper:snapshot_reserve_percentage': (_get_integer_param, None),
        'huawei_showsnapshotdir:show_snapshot_dir': (_get_snapshot_dir_param, None),
        'qos:iotype': (_get_string_param, None),
        'qos:maxiops': (_get_string_param, None),
        'qos:miniops': (_get_string_param, None),
        'qos:minbandwidth': (_get_string_param, None),
        'qos:maxbandwidth': (_get_string_param, None),
        'qos:latency': (_get_string_param, None),
        'filesystem:mode': (_get_string_param, None),
    }

    opts = {}
    for key in opts_capability:
        opt_key = _get_opt_key(key)
        opts[opt_key] = opts_capability[key][1]

    for spec_key in specs:
        key = spec_key.lower()
        if key not in opts_capability:
            continue
        func = opts_capability[key][0]
        opt_key = _get_opt_key(key)
        opts[opt_key] = func(key, specs[spec_key])

    return opts


def _get_smartprovisioning_opts(opts):
    if opts['thin_provisioning'] is None:
        return

    if opts['thin_provisioning']:
        opts['LUNType'] = constants.ALLOC_TYPE_THIN_FLAG
    else:
        opts['LUNType'] = constants.ALLOC_TYPE_THICK_FLAG


def _check_smartcache_opts(opts):
    if opts['huawei_smartcache'] and not opts['cachename']:
        msg = _('Cache name is not set, please set '
                'huawei_smartcache:cachename in extra specs.')
        raise exception.InvalidInput(reason=msg)


def _check_smartpartition_opts(opts):
    if opts['huawei_smartpartition'] and not opts['partitionname']:
        msg = _('Partition name is not set, please set '
                'huawei_smartpartition:partitionname in extra specs.')
        raise exception.InvalidInput(reason=msg)


def _get_qos_opts(opts):
    if not opts['qos']:
        return

    qos = {}
    for key in ('maxiops', 'miniops', 'minbandwidth',
                'maxbandwidth', 'latency'):
        if not opts.get(key):
            opts.pop(key, None)
        elif int(opts[key]) <= 0:
            msg = _('QoS %s must be set greater than 0.') % key
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        else:
            qos[key.upper()] = opts.pop(key)

    if not opts['iotype'] or opts['iotype'] not in constants.QOS_IO_TYPES:
        msg = _('iotype must be set to one of %s.') % constants.QOS_IO_TYPES
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)
    else:
        qos['IOTYPE'] = opts.pop('iotype')

    if (set(constants.QOS_LOWER_LIMIT) & set(qos)
            and set(constants.QOS_UPPER_LIMIT) & set(qos)):
        msg = _('QoS policy conflict, both protection and '
                'restriction policy are set: %s.') % qos
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)

    opts['qos'] = qos


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

    _retry_use_retrying()


def wait_fs_online(helper, fs_id, wait_interval, timeout):
    def _wait_fs_online():
        fs = helper.get_fs_info_by_id(fs_id)
        return (fs['HEALTHSTATUS'] == constants.STATUS_FS_HEALTH and
                fs['RUNNINGSTATUS'] == constants.STATUS_FS_RUNNING)

    wait_for_condition(_wait_fs_online, wait_interval, timeout)


def wait_hypermetro_pair_delete(helper, pair_id, wait_interval, timeout):
    def _wait_hypermetro_pair_delete():
        pair_info = helper.get_hypermetro_pair_by_id(pair_id)
        return pair_info is None

    wait_for_condition(_wait_hypermetro_pair_delete, wait_interval, timeout)


def share_name(name):
    return name.replace('-', '_')


def snapshot_name(name):
    return name.replace('-', '_')


def snapshot_id(fs_id, name):
    return fs_id + "@" + snapshot_name(name)


def share_size(size):
    return int(size) * constants.CAPACITY_UNIT


def share_path(name, need_replace=True):
    if need_replace:
        name = name.replace("-", "_")
    return "/" + name + "/"


def get_share_by_location(export_location, share_proto):
    share_ip = None
    _share_name = None

    if share_proto == 'NFS':
        export_location_split = export_location.split(':/')
        if len(export_location_split) == 2:
            share_ip = export_location_split[0]
            _share_name = export_location_split[1]
    elif share_proto == 'CIFS':
        export_location_split = export_location.split('\\')
        if len(export_location_split) == 4:
            share_ip = export_location_split[2]
            _share_name = export_location_split[3]
    else:
        msg = _('Invalid NAS protocol %s.') % share_proto
        raise exception.InvalidInput(reason=msg)

    return share_ip, _share_name


def get_access_info(access):
    return access['access_to'], access['access_type'], access['access_level']


def get_replica_pair_id(helper, fs_name):
    fs_info = helper.get_fs_info_by_name(fs_name)
    if fs_info:
        replication_ids = json.loads(fs_info['REMOTEREPLICATIONIDS'])
        if replication_ids:
            return replication_ids[0]


def get_hypermetro_vstore_id(helper, domain_name, local_vstore, remote_vstore):
    try:
        vstore_pair_id = helper.get_hypermetro_vstore_id(
            domain_name, local_vstore, remote_vstore)
    except Exception as err:
        msg = _("Failed to get vStore pair id, reason: %s") % err
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)
    if vstore_pair_id is None:
        msg = _("Failed to get vStore pair id, please check relation "
                "among metro domain, local vStore name and remote "
                "vStore name.")
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)
    return vstore_pair_id


def is_dorado_v6(client):
    array_info = client.get_array_info()
    version_info = array_info['PRODUCTVERSION']
    if version_info >= constants.SUPPORT_CLONE_PAIR_VERSION:
        return True


def standard_ipaddr(access):
    """
    When the added client permission is an IP address,
    standardize it. Otherwise, do not process it.
    """
    try:
        format_ip = netaddr.IPAddress(access)
        access_to = str(format_ip.format(dialect=netaddr.ipv6_compact))
        return access_to
    except Exception:
        return access


def generate_random_alphanumeric(length):
    return ''.join(random.choice(string.ascii_letters + string.digits)
                   for _x in range(length))


def cidr_to_prefixlen(cidr):
    try:
        network = netaddr.IPNetwork(cidr)
        return network.prefixlen
    except netaddr.AddrFormatError as err:
        msg = _("Invalid cidr supplied, reason is %s") % err
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)


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
