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

import collections
import json
import re
import uuid

import six

from oslo_config import cfg
from oslo_config import types
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import strutils
from oslo_utils import units
from cinder import context as cinder_context
from cinder import coordination
from cinder import exception
from cinder import objects
from cinder import utils
from cinder.i18n import _
from cinder.objects import fields
from cinder.volume import driver
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import fc_zone_helper
from cinder.volume.drivers.huawei import huawei_conf
from cinder.volume.drivers.huawei import huawei_utils
from cinder.volume.drivers.huawei import hypermetro
from cinder.volume.drivers.huawei import replication
from cinder.volume.drivers.huawei import rest_client
from cinder.volume.drivers.huawei import smartx
from cinder.volume import utils as volume_utils
from cinder.volume import volume_types
from cinder.zonemanager import utils as fczm_utils

LOG = logging.getLogger(__name__)

huawei_opts = [
    cfg.StrOpt('cinder_huawei_conf_file',
               default='/etc/cinder/cinder_huawei_conf.xml',
               help='The configuration file for the Cinder Huawei driver.'),
    cfg.MultiOpt('hypermetro_device',
                 item_type=types.Dict(),
                 secret=True,
                 help='Multi opt of dictionaries to represent a hypermetro '
                      'target device. This option may be specified multiple '
                      'times in a single config section to specify multiple '
                      'hypermetro target devices. Each entry takes the '
                      'standard dict config form: hypermetro_device = '
                      'key1:value1,key2:value2...'),
    cfg.BoolOpt('libvirt_iscsi_use_ultrapath',
                default=False,
                help='use ultrapath connection of the iSCSI volume'),
    cfg.BoolOpt('retain_storage_mapping',
                default=False,
                help='Whether to retain the storage mapping when the last '
                     'volume on the host is unmapped'),
]

CONF = cfg.CONF
CONF.register_opts(huawei_opts)

snap_attrs = ('id', 'volume_id', 'volume', 'provider_location', 'metadata')
vol_attrs = (
    'id', 'lun_type', 'provider_location', 'metadata',
    'multiattach', 'volume_attachment'
)
Snapshot = collections.namedtuple('Snapshot', snap_attrs)
Volume = collections.namedtuple('Volume', vol_attrs)


class HuaweiBaseDriver(driver.VolumeDriver):
    VERSION = "2.7.2"

    def __init__(self, *args, **kwargs):
        super(HuaweiBaseDriver, self).__init__(*args, **kwargs)

        if not self.configuration:
            msg = _('Configuration is not found.')
            raise exception.InvalidInput(reason=msg)

        self.active_backend_id = kwargs.get('active_backend_id')

        self.configuration.append_config_values(huawei_opts)
        self.huawei_conf = huawei_conf.HuaweiConf(self.configuration)
        self.support_func = None
        self.metro_flag = False
        self.replica = None
        self.use_ultrapath = self.configuration.safe_get(
            'libvirt_iscsi_use_ultrapath')
        self.sn = 'NA'
        self.is_dorado_v6 = False
        self.client = None
        self.replica_client = None
        self.rmt_client = None
        self.replica_dev_conf = None
        self.loc_dev_conf = None

    def check_local_func_support(self, obj_name):
        try:
            self.client._get_object_count(obj_name)
            return True
        except Exception:
            return False

    def check_rmt_func_support(self, obj_name):
        try:
            self.rmt_client._get_object_count(obj_name)
            return True
        except Exception:
            return False

    def check_replica_func_support(self, obj_name):
        try:
            self.replica_client._get_object_count(obj_name)
            return True
        except Exception:
            return False

    def get_local_and_remote_dev_conf(self):
        self.loc_dev_conf = self.huawei_conf.get_local_device()

        # Now just support one replication_devices.
        replica_devs = self.huawei_conf.get_replication_devices()
        self.replica_dev_conf = replica_devs[0] if replica_devs else {}

    def get_local_and_remote_client_conf(self):
        if self.active_backend_id:
            return self.replica_dev_conf, self.loc_dev_conf
        else:
            return self.loc_dev_conf, self.replica_dev_conf

    def do_setup(self, context):
        """Instantiate common class and login storage system."""
        # Set huawei private configuration into Configuration object.
        self.huawei_conf.update_config_value()

        self.get_local_and_remote_dev_conf()
        client_conf, replica_client_conf = (
            self.get_local_and_remote_client_conf())

        # init local client
        if not client_conf:
            msg = _('Get active client failed.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self.client = rest_client.RestClient(self.configuration,
                                             **client_conf)
        self.sn = self.client.login()
        self.client.check_storage_pools()
        self.is_dorado_v6 = huawei_utils.is_support_clone_pair(self.client)
        self.client.is_dorado_v6 = self.is_dorado_v6

        # init hypermetro remote client
        hypermetro_devs = self.huawei_conf.get_hypermetro_devices()
        hypermetro_client_conf = hypermetro_devs[0] if hypermetro_devs else {}
        if hypermetro_client_conf:
            self.rmt_client = rest_client.RestClient(self.configuration,
                                                     **hypermetro_client_conf)
            self.rmt_client.login()
            self.metro_flag = True

        # init replication manager
        if replica_client_conf:
            self.replica_client = rest_client.RestClient(self.configuration,
                                                         **replica_client_conf)
            self.replica_client.try_login()
            self.replica = replication.ReplicaPairManager(self.client,
                                                          self.replica_client,
                                                          self.configuration)

    def check_for_setup_error(self):
        pass

    def get_volume_stats(self, refresh=False):
        """Get volume status and reload huawei config file."""
        self.huawei_conf.update_config_value()
        stats = self.client.update_volume_stats()
        stats = self.update_support_capability(stats)

        if self.replica:
            stats = self.replica.update_replica_capability(stats)
            targets = [self.replica_dev_conf.get('backend_id')]
            stats['replication_targets'] = targets
            stats['replication_enabled'] = True

        return stats

    def update_support_capability(self, stats):
        feature_status = self.client.get_license_feature_status()
        license_dict = {}
        for key in constants.FEATURES_DICTS:
            if key in feature_status:
                license_dict[key] = (feature_status.get(key) in
                                     constants.AVAILABLE_FEATURE_STATUS)
            else:
                # In order to adapt the storage array in lower version
                if constants.FEATURES_DICTS[key]:
                    license_dict[key] = self.check_local_func_support(
                        constants.FEATURES_DICTS[key])

        dedup_enabled = huawei_utils.check_feature_available(
            feature_status, constants.DEDUP_FEATURES) or self.is_dorado_v6
        compression_enabled = huawei_utils.check_feature_available(
            feature_status, constants.COMPRESSION_FEATURES) or self.is_dorado_v6

        feature_dict = {
            'smartcache': license_dict.get('SmartCache', False),
            'smartpartition': license_dict.get('SmartPartition', False),
            'QoS_support': license_dict.get('SmartQoS', False),
            'luncopy': license_dict.get('HyperCopy', False),
            constants.HYPERMETRO: license_dict.get('HyperMetro', False),
            'thin_provisioning_support': license_dict.get('SmartThin', False),
            constants.THICK_PROVISIONING_SUPPORT: True,
            'consistencygroup_support': True,
            'multiattach': True,
            'huawei_controller': True,
            'dedup': [str(dedup_enabled).lower(), "false"],
            'compression': [str(compression_enabled).lower(), "false"],
            constants.HUAWEI_APPLICATION_TYPE: False,
        }

        for pool in stats['pools']:
            pool.update(feature_dict)

            if self.configuration.san_product == "Dorado":
                pool[constants.SMARTTIER] = False
                pool[constants.THICK_PROVISIONING_SUPPORT] = False
                pool[constants.HUAWEI_APPLICATION_TYPE] = True
            elif self.configuration.san_product == "V6":
                pool[constants.SMARTTIER] = True
                pool[constants.THICK_PROVISIONING_SUPPORT] = False
                pool[constants.HUAWEI_APPLICATION_TYPE] = True

            pool[constants.SMARTTIER] = (feature_status.get('SmartTier') in
                                         constants.AVAILABLE_FEATURE_STATUS and
                                         pool[constants.SMARTTIER])
            pool[constants.HYPERMETRO] = (feature_dict[constants.HYPERMETRO] and
                                          self._get_rmt_license_features(
                                              "HyperMetro", "HyperMetroPair"))
            # Asign the support function to global paramenter,
            # except "smarttier".
            self.support_func = pool

        return stats

    def _get_rmt_license_features(self, obj_name, cnt_name):
        if self.metro_flag:
            rmt_feature_status = self.rmt_client.get_license_feature_status()
            if obj_name in rmt_feature_status:
                return (rmt_feature_status[obj_name] in
                        constants.AVAILABLE_FEATURE_STATUS)
            else:
                # In order to adapt the storage array in lower version
                return self.check_rmt_func_support(cnt_name)
        else:
            return False

    def _get_volume_type(self, volume):
        volume_type = None
        type_id = volume.volume_type_id
        if type_id:
            ctxt = cinder_context.get_admin_context()
            volume_type = volume_types.get_volume_type(ctxt, type_id)

        return volume_type

    def _get_volume_params(self, volume_type):
        """Return the parameters for creating the volume."""
        specs = {}
        if volume_type:
            specs = dict(volume_type).get('extra_specs')

        opts = self._get_volume_params_from_specs(specs)
        return opts

    def _get_volume_params_from_specs(self, specs):
        """Return the volume parameters from extra specs."""
        opts_capability = {
            'smarttier': False,
            'smartcache': False,
            'smartpartition': False,
            'thin_provisioning_support': False,
            'thick_provisioning_support': False,
            'hypermetro': False,
            'replication_enabled': False,
            'replication_type': 'async',
            'huawei_controller': False,
            'dedup': None,
            'compression': None,
            'huawei_application_type': False,
        }

        opts_value = {
            'policy': None,
            'partitionname': None,
            'cachename': None,
            'controllername': None,
            'applicationname': None,
        }

        opts_associate = {
            'smarttier': 'policy',
            'smartcache': 'cachename',
            'smartpartition': 'partitionname',
            'huawei_controller': 'controllername',
            'huawei_application_type': 'applicationname',
        }

        opts = self._get_opts_from_specs(opts_capability,
                                         opts_value,
                                         opts_associate,
                                         specs)

        opts = smartx.SmartX(self.client).get_smartx_specs_opts(opts)
        opts = huawei_utils.get_apply_type_id(opts)
        opts = replication.get_replication_opts(opts)
        LOG.debug('volume opts %(opts)s.', {'opts': opts})
        return opts

    def _get_opts_from_specs(self, opts_capability, opts_value,
                             opts_associate, specs):
        """Get the well defined extra specs."""
        opts = {}
        opts.update(opts_capability)
        opts.update(opts_value)

        # the analysis of key-value
        for key, value in specs.items():
            # Get the scope, if is using scope format.
            scope = None

            key_split = key.split(':')
            if len(key_split) > 2 and key_split[0] != "capabilities":
                continue

            if len(key_split) == 1:
                key = key_split[0].lower()
            else:
                scope = key_split[0].lower()
                key = key_split[1].lower()

            if ((not scope or scope == 'capabilities')
                    and key in opts_capability):
                words = value.split()
                if words and len(words) == 2 and words[0] in (
                        '<is>', '<in>'):
                    opts[key] = words[1].lower()
                elif key == 'replication_type':
                    LOG.error("Extra specs must be specified as "
                              "replication_type='<in> sync' or "
                              "'<in> async'.")
                else:
                    LOG.warning("Extra specs must be specified as "
                                "capabilities:%s='<is> True'.", key)
            if all(
                    [
                        scope in opts_capability,
                        key in opts_value,
                        scope in opts_associate,
                        opts_associate.get(scope) == key
                    ]
            ):
                opts[key] = value

        return opts

    def _get_lun_params(self, volume, opts, src_size=None):
        pool_name = volume_utils.extract_host(volume.host, level='pool')
        params = {
            'NAME': huawei_utils.encode_name(volume.id),
            'PARENTID': self.client.get_pool_id(pool_name),
            'DESCRIPTION': volume.name,
            'ALLOCTYPE': opts.get('LUNType', self.configuration.lun_type),
            'CAPACITY': int(int(src_size) * constants.CAPACITY_UNIT if src_size
                            else huawei_utils.get_volume_size(volume)),
            'WRITEPOLICY': self.configuration.lun_write_type,
            'PREFETCHPOLICY': self.configuration.lun_prefetch_type,
            'PREFETCHVALUE': self.configuration.lun_prefetch_value,
            'DATATRANSFERPOLICY': opts.get('policy', '0'),
        }

        if opts['controllerid']:
            params['OWNINGCONTROLLER'] = opts['controllerid']

        if opts.get(constants.DEDUP):
            params['ENABLESMARTDEDUP'] = opts[constants.DEDUP]
        elif "true" not in self.support_func[constants.DEDUP]:
            params['ENABLESMARTDEDUP'] = False

        if opts.get(constants.COMPRESSION):
            params['ENABLECOMPRESSION'] = opts[constants.COMPRESSION]
        elif "true" not in self.support_func[constants.COMPRESSION]:
            params['ENABLECOMPRESSION'] = False

        if opts.get(constants.APPLICATION_TYPE):
            workload_type_id = self.client.get_workload_type_id(
                opts[constants.APPLICATION_TYPE])
            if workload_type_id:
                params['WORKLOADTYPEID'] = workload_type_id
            else:
                msg = _("The workload type %s is not exist. Please create it "
                        "on the array") % opts[constants.APPLICATION_TYPE]
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        LOG.info('volume: %(volume)s, lun params: %(params)s.',
                 {'volume': volume.id, 'params': params})
        return params

    def _create_volume(self, lun_params):
        # Create LUN on the array.
        lun_info = self.client.create_lun(lun_params)
        metadata = {
            'huawei_lun_id': lun_info['ID'],
            'huawei_sn': self.sn,
            'huawei_lun_wwn': lun_info['WWN']
        }
        model_update = {'metadata': metadata}

        return lun_info, model_update

    def _create_base_type_volume(self, opts, volume, volume_type):
        """Create volume and add some base type.

        Base type is the services won't conflict with the other service.
        """
        lun_params = self._get_lun_params(volume, opts)
        lun_info, model_update = self._create_volume(lun_params)
        lun_id = lun_info['ID']

        try:
            qos = huawei_utils.get_qos_by_volume_type(volume_type)
            if qos:
                if not self.support_func.get('QoS_support'):
                    msg = (_("Can't support qos on the array"))
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                else:
                    smart_qos = smartx.SmartQos(self.client)
                    smart_qos.add(qos, lun_id)

            smartpartition = smartx.SmartPartition(self.client)
            smartpartition.add(opts, lun_id)

            smartcache = smartx.SmartCache(self.client)
            smartcache.add(opts, lun_id)
        except Exception as err:
            self._delete_lun_with_check(lun_id)
            msg = _('Create volume error. Because %s.') % six.text_type(err)
            raise exception.VolumeBackendAPIException(data=msg)

        return lun_params, lun_info, model_update

    def _add_extend_type_to_volume(self, volume, volume_type, opts, lun_params,
                                   lun_info, is_sync=False):
        lun_id = lun_info['ID']
        lun_params.update({"CAPACITY": huawei_utils.get_volume_size(volume)})

        qos = huawei_utils.get_qos_by_volume_type(volume_type)
        if qos:
            smart_qos = smartx.SmartQos(self.client)
            smart_qos.add(qos, lun_id)

        smartpartition = smartx.SmartPartition(self.client)
        smartpartition.add(opts, lun_id)

        smartcache = smartx.SmartCache(self.client)
        smartcache.add(opts, lun_id)

        metro_id = None
        if opts.get('hypermetro') == 'true':
            if "WORKLOADTYPEID" in lun_params and opts.get(constants.APPLICATION_TYPE):
                workload_type_id = self.rmt_client.get_workload_type_id(
                    opts[constants.APPLICATION_TYPE])
                if workload_type_id:
                    lun_params.update({"WORKLOADTYPEID": workload_type_id})
                else:
                    msg = _("The workload type %s is not exist. Please create "
                            "it on the array") % opts[constants.APPLICATION_TYPE]
                    LOG.error(msg)
                    raise exception.InvalidInput(reason=msg)

            metro = hypermetro.HuaweiHyperMetro(
                self.client, self.rmt_client, self.configuration)
            metro_id = metro.create_hypermetro(lun_id, lun_params, is_sync)

            if volume.consistencygroup_id:
                try:
                    metro.add_hypermetro_to_consistencygroup(
                        {'id': volume.consistencygroup_id}, metro_id)
                except Exception:
                    metro.delete_hypermetro(volume)
                    raise

        replica_info = {}
        if opts.get('replication_enabled') == 'true':
            replica_model = opts.get('replication_type')
            replica_info = self.replica.create_replica(lun_info, replica_model)

            if volume.consistencygroup_id:
                try:
                    replicg = replication.ReplicaCG(
                        self.client, self.replica_client, self.configuration)
                    replicg.add_replica_to_group(
                        volume.consistencygroup_id,
                        replica_info.get('replication_driver_data'))
                except Exception:
                    self.replica.delete_replica(
                        volume, replica_info.get('replication_driver_data'))
                    raise

        return metro_id, replica_info

    def _create_volume_from_src_by_fast_clone(
            self, volume, src_obj, src_type, lun_params, expect_size):
        if volume.volume_type_id != src_obj.volume_type_id:
            msg = _("Volume type must be the same as source "
                    "for fast clone.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if src_type == objects.Volume:
            src_id = self._check_volume_exist_on_array(
                src_obj, constants.VOLUME_NOT_EXISTS_RAISE)
        else:
            src_id = self._check_snapshot_exist_on_array(
                src_obj, constants.SNAPSHOT_NOT_EXISTS_RAISE)

        lun_info = self._create_volume_by_clone(
            src_id, lun_params, expect_size)
        return lun_info

    def _create_volume_from_src_by_clone_pair(
            self, src_type, src_obj, lun_params):
        clone_speed = self.configuration.lun_copy_speed
        if src_type == objects.Volume:
            src_id = self._check_volume_exist_on_array(
                src_obj, constants.VOLUME_NOT_EXISTS_RAISE)
        else:
            src_id = self._check_snapshot_exist_on_array(
                src_obj, constants.SNAPSHOT_NOT_EXISTS_RAISE)
        lun_info = self._create_volume_by_clone_pair(
            src_id, lun_params, clone_speed)
        return lun_info

    def _create_volume_from_src_by_lun_copy(
            self, metadata, src_type, src_obj, lun_params):
        copyspeed = metadata.get('copyspeed')
        if not copyspeed:
            copyspeed = self.configuration.lun_copy_speed
        elif copyspeed not in constants.LUN_COPY_SPEED_TYPES:
            msg = (_("LUN copy speed is: %(speed)s. It should be between "
                     "%(low)s and %(high)s.")
                   % {"speed": copyspeed,
                      "low": constants.LUN_COPY_SPEED_LOW,
                      "high": constants.LUN_COPY_SPEED_HIGH})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if src_type == objects.Volume:
            vol_kwargs = {
                'id': src_obj.id,
                'provider_location': src_obj.provider_location,
            }
            snapshot_kwargs = {
                'id': six.text_type(uuid.uuid4()),
                'volume_id': src_obj.id,
                'volume': objects.Volume(**vol_kwargs),
            }

            snapshot = objects.Snapshot(**snapshot_kwargs)
            src_id = self._create_snapshot(snapshot)
        else:
            src_id = self._check_snapshot_exist_on_array(
                src_obj, constants.SNAPSHOT_NOT_EXISTS_RAISE)

        try:
            lun_info = self._create_volume_by_luncopy(
                src_id, lun_params, copyspeed)
        except Exception as err:
            msg = _("Create volume by lun copy error. Reason: %s") % err
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)
        finally:
            if src_type == objects.Volume:
                self._delete_snapshot(src_id)

        return lun_info

    def _create_volume_from_src(self, volume, src_obj, src_type, lun_params,
                                clone_pair_flag=None):
        metadata = huawei_utils.get_volume_metadata(volume)
        expect_size = int(volume.size) * constants.CAPACITY_UNIT
        if (strutils.bool_from_string(metadata.get(constants.FASTCLONE)) or
                (metadata.get(constants.FASTCLONE) is None and
                 self.configuration.clone_mode == constants.FASTCLONE)):
            lun_info = self._create_volume_from_src_by_fast_clone(
                volume, src_obj, src_type, lun_params, expect_size)
        elif clone_pair_flag:
            lun_info = self._create_volume_from_src_by_clone_pair(
                src_type, src_obj, lun_params)
        else:
            lun_info = self._create_volume_from_src_by_lun_copy(
                metadata, src_type, src_obj, lun_params)

        try:
            if int(lun_info.get('CAPACITY')) < expect_size:
                self.client.extend_lun(lun_info.get(constants.ID_UPPER), expect_size)
                lun_info = self.client.get_lun_info(lun_info.get(constants.ID_UPPER))
        except Exception as err:
            LOG.exception('Extend lun %(lun_id)s error. Reason is %(err)s' %
                          {"lun_id": lun_info.get(constants.ID_UPPER), "err": err})
            self._delete_lun_with_check(lun_info.get(constants.ID_UPPER))
            raise

        return lun_info

    def _create_snapshot(self, snapshot):
        snapshot_id = self._create_snapshot_base(snapshot)

        try:
            self.client.activate_snapshot(snapshot_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error("Active snapshot %s failed, now deleting it.",
                          snapshot_id)
                self.client.delete_snapshot(snapshot_id)
        return snapshot_id

    def _delete_snapshot(self, snapshot_id):
        self.client.stop_snapshot(snapshot_id)
        self.client.delete_snapshot(snapshot_id)

    def _create_volume_by_clone(self, src_id, lun_params, expected_size):
        LOG.info('Create volume %s by clone from source %s.',
                 lun_params['NAME'], src_id)

        lun_info = self.client.create_clone_lun(src_id, lun_params['NAME'])
        lun_id = lun_info['ID']

        try:
            if int(lun_info['CAPACITY']) < expected_size:
                self.client.extend_lun(lun_id, expected_size)
            self.client.split_clone_lun(lun_id)
        except Exception:
            LOG.exception('Split clone lun %s error.', lun_id)
            self.client.delete_lun(lun_id)
            raise

        return lun_info

    def _create_volume_by_luncopy(self, src_id, lun_params, copyspeed):
        LOG.info('Create volume %s by luncopy from source %s.',
                 lun_params['NAME'], src_id)

        lun_info = self.client.create_lun(lun_params)
        tgt_lun_id = lun_info['ID']

        def _volume_ready():
            result = self.client.get_lun_info(tgt_lun_id)
            return (result['HEALTHSTATUS'] == constants.STATUS_HEALTH and
                    result['RUNNINGSTATUS'] == constants.STATUS_VOLUME_READY)

        try:
            huawei_utils.wait_for_condition(
                _volume_ready, self.configuration.lun_ready_wait_interval,
                self.configuration.lun_ready_wait_interval * 10)
            self._copy_volume(src_id, tgt_lun_id, copyspeed)
        except Exception:
            LOG.exception('Copy lun from source %s error.', src_id)
            self._delete_lun_with_check(tgt_lun_id)
            raise

        return lun_info

    def _create_volume_by_clone_pair(self, src_id, lun_params, clone_speed):
        LOG.info('Create volume %s by ClonePair from source %s.',
                 lun_params['NAME'], src_id)
        lun_info = self.client.create_lun(lun_params)
        tgt_id = lun_info['ID']

        def _volume_ready():
            result = self.client.get_lun_info(tgt_id)
            return (result['HEALTHSTATUS'] == constants.STATUS_HEALTH and
                    result['RUNNINGSTATUS'] == constants.STATUS_VOLUME_READY)

        try:
            huawei_utils.wait_for_condition(
                _volume_ready, self.configuration.lun_ready_wait_interval,
                self.configuration.lun_ready_wait_interval * 10)
            self._create_clone_pair(src_id, tgt_id, clone_speed)
        except Exception:
            LOG.exception('Copy lun from source %s error.', src_id)
            self._delete_lun_with_check(tgt_id)
            raise
        return lun_info

    def _create_clone_pair(self, source_id, target_id, clone_speed):
        clone_pair_id = self.client.create_clone_pair(
            source_id, target_id, clone_speed)

        def _pair_sync_completed():
            clone_pair_info = self.client.get_clone_pair_info(clone_pair_id)
            if clone_pair_info['copyStatus'] != constants.CLONE_STATUS_HEALTH:
                msg = _("ClonePair %s is abnormal.") % clone_pair_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            return (clone_pair_info['syncStatus'] in
                    constants.CLONE_STATUS_COMPLETE)

        self.client.sync_clone_pair(clone_pair_id)
        huawei_utils.wait_for_condition(
            _pair_sync_completed, self.configuration.lun_copy_wait_interval,
            self.configuration.lun_timeout)
        self.client.delete_clone_pair(clone_pair_id)

    def _common_create_volume(self, volume, src_obj=None, src_type=None,
                              is_sync=False, src_size=None):
        volume_type = self._get_volume_type(volume)
        opts = self._get_volume_params(volume_type)
        huawei_utils.check_group_volume_type_valid(opts)

        lun_params = self._get_lun_params(volume, opts, src_size)

        if not src_obj:
            lun_info = self.client.create_lun(lun_params)
        else:
            lun_info = self._create_volume_from_src(
                volume, src_obj, src_type, lun_params, self.is_dorado_v6)

        try:
            metro_id, replica_info = self._add_extend_type_to_volume(
                volume, volume_type, opts, lun_params, lun_info, is_sync)
        except Exception:
            LOG.exception('Add extend feature to volume %s failed.', volume.id)
            self._delete_lun_with_check(lun_info.get('ID'))
            raise

        hyper_metro = True if metro_id else False

        provider_location = huawei_utils.to_string(
            huawei_lun_id=lun_info.get('ID'), huawei_sn=self.sn,
            huawei_lun_wwn=lun_info.get('WWN'), hypermetro=hyper_metro)
        model_update = {'provider_location': provider_location}
        model_update.update(replica_info)
        huawei_utils.set_volume_lun_wwn(model_update, lun_info, volume)
        return model_update

    def create_volume(self, volume):
        return self._common_create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        snapshot_id, __ = huawei_utils.get_snapshot_id(self.client, snapshot)
        if not snapshot_id:
            msg = _('Snapshot %s does not exist.') % snapshot.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        snapshot_info = self.client.get_snapshot_info(snapshot_id)
        if snapshot_info.get('RUNNINGSTATUS') != constants.STATUS_ACTIVE:
            msg = _("Failed to create volume from snapshot due to "
                    "snapshot %s is not activated.") % snapshot_id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return self._common_create_volume(volume, snapshot, objects.Snapshot,
                                          is_sync=True,
                                          src_size=snapshot.volume_size)

    def create_cloned_volume(self, volume, src_vref):
        lun_id = self._check_volume_exist_on_array(
            src_vref, constants.VOLUME_NOT_EXISTS_RAISE)
        LOG.debug("The lun id of source volume is %s", lun_id)
        return self._common_create_volume(volume, src_vref, objects.Volume,
                                          is_sync=True, src_size=src_vref.size)

    def _delete_volume(self, volume, lun_id=None):
        if not lun_id:
            lun_id, lun_wwn = huawei_utils.get_volume_lun_id(
                self.client, volume)
        if not lun_id:
            return

        huawei_utils.remove_lun_from_lungroup(
            self.client, lun_id,
            self.configuration.force_delete_volume)

        self.client.delete_lun(lun_id)

    def _remove_remote_lun_from_lungroup(self, volume):
        rmt_lun_id, rmt_lun_wwn = huawei_utils.get_volume_lun_id(
                self.rmt_client, volume)
        if not rmt_lun_id:
            return

        huawei_utils.remove_lun_from_lungroup(
            self.rmt_client, rmt_lun_id,
            self.configuration.force_delete_volume)

    def delete_volume(self, volume):
        """Delete a volume.

        Three steps:
        Firstly, remove associate from lungroup.
        Secondly, remove associate from QoS policy.
        Thirdly, remove the lun.
        """
        metadata = huawei_utils.get_lun_metadata(volume)
        if metadata.get('hypermetro'):
            self._remove_remote_lun_from_lungroup(volume)

            metro = hypermetro.HuaweiHyperMetro(
                self.client, self.rmt_client, self.configuration)
            try:
                metro.delete_hypermetro(volume)
            except exception.VolumeBackendAPIException as err:
                LOG.error('Delete hypermetro error: %s.', err)
                lun_id = self._check_volume_exist_on_array(
                    volume, constants.VOLUME_NOT_EXISTS_WARN)
                if lun_id:
                    self._delete_volume(volume, lun_id)
                raise

        # Delete a replication volume
        replica_data = volume.replication_driver_data
        if replica_data:
            try:
                self.replica.delete_replica(volume)
            except exception.VolumeBackendAPIException as err:
                with excutils.save_and_reraise_exception():
                    LOG.exception("Delete replication error.")
                    lun_id = self._check_volume_exist_on_array(
                        volume, constants.VOLUME_NOT_EXISTS_WARN)
                    if lun_id:
                        self._delete_volume(volume, lun_id)

        lun_id = self._check_volume_exist_on_array(
            volume, constants.VOLUME_NOT_EXISTS_WARN)
        if not lun_id:
            return

        qos_id = self.client.get_qosid_by_lunid(lun_id)
        if qos_id:
            smart_qos = smartx.SmartQos(self.client)
            smart_qos.remove(qos_id, lun_id)
        self._delete_volume(volume, lun_id)

    def _delete_lun_with_check(self, lun_id, lun_wwn=None):
        if not lun_id:
            return

        if self.client.check_lun_exist(lun_id, lun_wwn):
            qos_id = self.client.get_qosid_by_lunid(lun_id)
            if qos_id:
                smart_qos = smartx.SmartQos(self.client)
                smart_qos.remove(qos_id, lun_id)

            self.client.delete_lun(lun_id)

    def _is_lun_migration_complete(self, src_id, dst_id):
        result = self.client.get_lun_migration_task()
        found_migration_task = False
        if not result:
            return False

        for item in result:
            if (src_id == item['PARENTID'] and dst_id == item['TARGETLUNID']):
                found_migration_task = True
                if constants.MIGRATION_COMPLETE == item['RUNNINGSTATUS']:
                    return True
                if constants.MIGRATION_FAULT == item['RUNNINGSTATUS']:
                    msg = _("Lun migration error.")
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

        if not found_migration_task:
            err_msg = _("Cannot find migration task.")
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        return False

    def _is_lun_migration_exist(self, src_id, dst_id):
        try:
            result = self.client.get_lun_migration_task()
        except Exception:
            LOG.error("Get LUN migration error.")
            return False

        for item in result:
            if (src_id == item['PARENTID']
                    and dst_id == item['TARGETLUNID']):
                return True
        return False

    def _migrate_lun(self, src_id, dst_id):
        try:
            self.client.create_lun_migration(src_id, dst_id)

            def _is_lun_migration_complete():
                return self._is_lun_migration_complete(src_id, dst_id)

            wait_interval = constants.MIGRATION_WAIT_INTERVAL
            huawei_utils.wait_for_condition(_is_lun_migration_complete,
                                            wait_interval,
                                            self.configuration.lun_timeout)
        # Clean up if migration failed.
        except Exception as ex:
            raise exception.VolumeBackendAPIException(data=ex)
        finally:
            if self._is_lun_migration_exist(src_id, dst_id):
                self.client.delete_lun_migration(src_id, dst_id)
            self._delete_lun_with_check(dst_id)

        LOG.debug("Migrate lun %s successfully.", src_id)
        return True

    def _wait_volume_ready(self, lun_id):
        wait_interval = self.configuration.lun_ready_wait_interval

        def _volume_ready():
            result = self.client.get_lun_info(lun_id)
            if (result['HEALTHSTATUS'] == constants.STATUS_HEALTH
               and result['RUNNINGSTATUS'] == constants.STATUS_VOLUME_READY):
                return True
            return False

        huawei_utils.wait_for_condition(_volume_ready,
                                        wait_interval,
                                        wait_interval * 10)

    def _get_original_status(self, volume):
        return 'in-use' if volume.volume_attachment else 'available'

    def _change_lun_name(self, lun_id, rmt_lun_id, new_name, description=None):
        if rmt_lun_id:
            self.rmt_client.rename_lun(rmt_lun_id, new_name, description)
        self.client.rename_lun(lun_id, new_name, description)

    def _get_lun_id(self, volume, metadata, new_metadata):
        """
        same storage situation, if new_volume is not
        hypermetro, we don't need to change remote lun name
        """
        rmt_lun_id = None
        if metadata.get('hypermetro') and new_metadata.get('hypermetro'):
            rmt_lun_id, rmt_lun_wwn = huawei_utils.get_volume_lun_id(
                self.rmt_client, volume)
        lun_id, lun_wwn = huawei_utils.get_volume_lun_id(
            self.client, volume)
        return lun_id, rmt_lun_id

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status=None):
        original_name = huawei_utils.encode_name(volume.id)
        new_name = huawei_utils.encode_name(new_volume.id)
        org_metadata = huawei_utils.get_lun_metadata(volume)
        new_metadata = huawei_utils.get_lun_metadata(new_volume)
        new_lun_id, new_rmt_lun_id = self._get_lun_id(new_volume, new_metadata, new_metadata)
        description = volume['name']

        try:
            if org_metadata.get('huawei_sn') == new_metadata.get('huawei_sn'):
                lun_id, rmt_lun_id = self._get_lun_id(volume, org_metadata, new_metadata)
                src_lun_name = str(uuid.uuid4())
                src_lun_name = huawei_utils.encode_name(src_lun_name)
                self._change_lun_name(lun_id, rmt_lun_id, src_lun_name)
                self._change_lun_name(new_lun_id, new_rmt_lun_id, original_name, description)
                self._change_lun_name(lun_id, rmt_lun_id, new_name)
            else:
                self._change_lun_name(new_lun_id, new_rmt_lun_id, original_name, description)

        except exception.VolumeBackendAPIException:
            LOG.error('Unable to rename lun %s on array.', new_lun_id)
            return {'_name_id': new_volume.name_id,
                    'provider_location': huawei_utils.to_string(**new_metadata)
                    }

        LOG.debug("Rename lun %(id)s to %(original_name)s successfully.",
                  {'id': new_lun_id,
                   'original_name': original_name})

        return {'_name_id': None,
                'provider_location': huawei_utils.to_string(**new_metadata)}

    def migrate_volume(self, ctxt, volume, host):
        """Migrate a volume within the same array."""
        lun_id = self._check_volume_exist_on_array(
            volume, constants.VOLUME_NOT_EXISTS_RAISE
        )
        LOG.debug("Migrate volume, the lun id of source volume is %s", lun_id)
        # NOTE(jlc): Replication volume can't migrate. But retype
        # can remove replication relationship first then do migrate.
        # So don't add this judgement into _check_migration_valid().
        volume_type = self._get_volume_type(volume)
        opts = self._get_volume_params(volume_type)
        if (opts.get('hypermetro') == 'true' or
                opts.get('replication_enabled') == 'true'):
            return False, None

        moved = self._migrate_volume(volume, host)
        if moved:
            lun_id, __ = huawei_utils.get_volume_lun_id(self.client, volume)
            smartpartition = smartx.SmartPartition(self.client)
            smartpartition.add(opts, lun_id)

            smartcache = smartx.SmartCache(self.client)
            smartcache.add(opts, lun_id)

        return moved, {}

    def _check_migration_valid(self, host, volume):
        if 'pool_name' not in host[constants.CAPABILITIES]:
            return False

        target_device = host[constants.CAPABILITIES]['location_info']

        # Source and destination should be on same array.
        if target_device != self.client.device_id:
            return False

        # Same protocol should be used if volume is in-use.
        protocol = self.configuration.san_protocol
        if (host[constants.CAPABILITIES]['storage_protocol'] != protocol
                and self._get_original_status(volume) == 'in-use'):
            return False

        pool_name = host[constants.CAPABILITIES]['pool_name']
        if len(pool_name) == 0:
            return False

        return True

    def _migrate_volume(self, volume, host, new_type=None):
        if not self._check_migration_valid(host, volume):
            return False

        type_id = volume.volume_type_id

        volume_type = None
        if type_id:
            volume_type = volume_types.get_volume_type(None, type_id)

        pool_name = host['capabilities']['pool_name']
        pools = self.client.get_all_pools()
        pool_info = self.client.get_pool_info(pool_name, pools)
        dst_volume_name = six.text_type(uuid.uuid4())

        src_id, lun_wwn = huawei_utils.get_volume_lun_id(self.client, volume)
        opts = None
        if new_type:
            # If new type exists, use new type.
            new_specs = new_type['extra_specs']
            opts = self._get_volume_params_from_specs(new_specs)
            if 'LUNType' not in opts:
                opts['LUNType'] = self.configuration.lun_type

        if not opts:
            opts = self._get_volume_params(volume_type)

        lun_info = self.client.get_lun_info(src_id)
        lun_params = {
            'NAME': huawei_utils.encode_name(dst_volume_name),
            'PARENTID': pool_info['ID'],
            'DESCRIPTION': lun_info['DESCRIPTION'],
            'ALLOCTYPE': opts.get('LUNType', lun_info['ALLOCTYPE']),
            'CAPACITY': lun_info['CAPACITY'],
            'WRITEPOLICY': lun_info['WRITEPOLICY'],
        }

        if 'DATATRANSFERPOLICY' in lun_info:
            lun_params['DATATRANSFERPOLICY'] = opts.get(
                'policy', lun_info['DATATRANSFERPOLICY'])

        if lun_info.get("WORKLOADTYPENAME") and lun_info.get("WORKLOADTYPEID"):
            lun_params["WORKLOADTYPEID"] = lun_info["WORKLOADTYPEID"]

        for k in ('PREFETCHPOLICY', 'PREFETCHVALUE', 'READCACHEPOLICY',
                  'WRITECACHEPOLICY', 'OWNINGCONTROLLER'):
            if k in lun_info:
                lun_params[k] = lun_info[k]

        for item in list(lun_params.keys()):
            if lun_params.get(item) == '--':
                lun_params.pop(item, None)

        lun_info = self.client.create_lun(lun_params)
        dst_id = lun_info['ID']
        self._wait_volume_ready(dst_id)
        moved = self._migrate_lun(src_id, dst_id)

        return moved

    def _check_volume_exist_on_array(self, volume, action, local=True):
        """Check whether the volume exists on the array.

        If the volume exists on the array, return the LUN ID.
        If not exists, raise or log warning.
        """
        # Determine use which client, local or remote.
        client = self.client if local else self.rmt_client

        # Firstly, try to find LUN ID from volume.
        lun_id, lun_wwn = huawei_utils.get_volume_lun_id(client, volume)
        if not lun_id:
            msg = _("Volume %s does not exist on the array."
                    ) % volume.id
            if action == constants.VOLUME_NOT_EXISTS_WARN:
                LOG.warning(msg)
            if action == constants.VOLUME_NOT_EXISTS_RAISE:
                raise exception.VolumeBackendAPIException(data=msg)
            return None

        if not lun_wwn:
            LOG.debug("No LUN WWN recorded for volume %s.", volume.id)

        if not client.check_lun_exist(lun_id, lun_wwn):
            msg = (_("Volume %s does not exist on the array.")
                   % volume.id)
            if action == constants.VOLUME_NOT_EXISTS_WARN:
                LOG.warning(msg)
            if action == constants.VOLUME_NOT_EXISTS_RAISE:
                raise exception.VolumeBackendAPIException(data=msg)
            return None
        return lun_id

    def _extend_hypermetro_volume(self, volume, new_size):
        lun_name = huawei_utils.encode_name(volume.id)
        metro_info = self.client.get_hypermetro_by_lun_name(lun_name)
        if not metro_info:
            msg = _('Volume %s is not in hypermetro pair') % lun_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        metro_id = metro_info['ID']

        metro = hypermetro.HuaweiHyperMetro(
            self.client, self.rmt_client, self.configuration)
        if metro_info['ISINCG'] == 'true':
            metro.stop_consistencygroup(metro_info['CGID'])
        else:
            metro.check_metro_need_to_stop(metro_id)

        try:
            self.rmt_client.extend_lun(metro_info['REMOTEOBJID'], new_size)
            self.client.extend_lun(metro_info['LOCALOBJID'], new_size)
        finally:
            if metro_info['ISINCG'] == 'true':
                self.client.sync_metrogroup(metro_info['CGID'])
            else:
                self.client.sync_hypermetro(metro_id)

    def _extend_replica_volume(self, pair_id, new_size):
        replica_info = self.client.get_pair_by_id(pair_id)
        if replica_info['ISINCG'] == 'true':
            cg_info = self.client.get_replicg_info(replica_info['CGID'])
            replica_cg = replication.ReplicaCG(
                self.client, self.replica_client, self.configuration)
            replica_cg.split_replicg(cg_info)
        else:
            self.replica.split_replica(pair_id)

        try:
            self.replica_client.extend_lun(replica_info['REMOTERESID'],
                                           new_size)
            self.client.extend_lun(replica_info['LOCALRESID'], new_size)
        finally:
            if replica_info['ISINCG'] == 'true':
                self.client.sync_replicg(replica_info['CGID'])
            else:
                self.client.sync_pair(pair_id)

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        lun_id = self._check_volume_exist_on_array(
            volume, constants.VOLUME_NOT_EXISTS_RAISE)
        lun_info = self.client.get_lun_info(lun_id)

        old_size = int(lun_info.get('CAPACITY'))
        new_size = int(new_size) * constants.CAPACITY_UNIT
        if new_size == old_size:
            LOG.info("New size is equal to the real size from backend"
                     " storage, no need to extend."
                     " realsize: %(oldsize)s, newsize: %(newsize)s.",
                     {'oldsize': old_size,
                      constants.NEWSIZE: new_size})
            return
        if new_size < old_size:
            msg = (
                _("New size should be bigger than the real size from "
                  "backend storage."
                  " realsize: %(oldsize)s, newsize: %(newsize)s."),
                {'oldsize': old_size, constants.NEWSIZE: new_size}
            )
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.info(
            'Extend volume: %(id)s, '
            'oldsize: %(oldsize)s, newsize: %(newsize)s.',
            {'id': volume.id,
             'oldsize': old_size,
             constants.NEWSIZE: new_size})

        metadata = huawei_utils.get_lun_metadata(volume)
        if metadata.get('hypermetro'):
            self._extend_hypermetro_volume(volume, new_size)
        elif volume.replication_driver_data:
            replica_data = replication.get_replication_driver_data(volume)
            self._extend_replica_volume(replica_data['pair_id'], new_size)
        else:
            self.client.extend_lun(lun_id, new_size)

    def _create_snapshot_base(self, snapshot):
        volume = snapshot.volume
        if not volume:
            msg = (_("Can't get volume id from snapshot, snapshot: %(id)s")
                   % {"id": snapshot.id})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        lun_id, lun_wwn = huawei_utils.get_volume_lun_id(self.client, volume)
        snapshot_name = huawei_utils.encode_name(snapshot.id)
        snapshot_description = snapshot.id
        try:
            snapshot_info = self.client.create_snapshot(lun_id,
                                                        snapshot_name,
                                                        snapshot_description)
        except Exception as err:
            with excutils.save_and_reraise_exception():
                LOG.error("Create snapshot %s failed, reason is %s, now "
                          "deleting it.", snapshot_name, err)
                snapshot_id = self.client.get_snapshot_id_by_name(
                    snapshot_name)
                if snapshot_id:
                    self.client.delete_snapshot(snapshot_id)

        snapshot_id = snapshot_info['ID']

        def _snapshot_ready():
            result = self.client.get_snapshot_info(snapshot_id)
            if result['HEALTHSTATUS'] != constants.STATUS_HEALTH:
                err_msg = _("The snapshot created is fault.")
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)

            if (result['RUNNINGSTATUS'] in (constants.STATUS_SNAPSHOT_INACTIVE,
                                            constants.STATUS_SNAPSHOT_ACTIVE)):
                return True

            return False

        huawei_utils.wait_for_condition(_snapshot_ready,
                                        constants.DEFAULT_WAIT_INTERVAL,
                                        constants.DEFAULT_WAIT_INTERVAL * 10)
        return snapshot_id

    def create_snapshot(self, snapshot):
        snapshot_id = self._create_snapshot_base(snapshot)
        try:
            self.client.activate_snapshot(snapshot_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error("Active snapshot %s failed, now deleting it.",
                          snapshot_id)
                self.client.delete_snapshot(snapshot_id)

        snapshot_info = self.client.get_snapshot_info(snapshot_id)
        location = huawei_utils.to_string(
            huawei_snapshot_id=snapshot_id,
            huawei_snapshot_wwn=snapshot_info['WWN'])
        return {'provider_location': location}

    def delete_snapshot(self, snapshot):
        snapshot_id = self._check_snapshot_exist_on_array(
            snapshot, constants.SNAPSHOT_NOT_EXISTS_WARN)
        if not snapshot_id:
            return
        self.client.stop_snapshot(snapshot_id)
        self.client.delete_snapshot(snapshot_id)

    def _check_snapshot_exist_on_array(self, snapshot, action):
        snapshot_id, snapshot_wwn = huawei_utils.get_snapshot_id(
            self.client, snapshot)
        if not (snapshot_id and
                self.client.check_snapshot_exist(snapshot_id, snapshot_wwn)):
            msg = (_("Snapshot %s does not exist on the array.")
                   % snapshot.id)
            if action == constants.SNAPSHOT_NOT_EXISTS_WARN:
                LOG.warning(msg)
            if action == constants.SNAPSHOT_NOT_EXISTS_RAISE:
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            return None
        return snapshot_id

    def retype(self, ctxt, volume, new_type, diff, host):
        """Convert the volume to be of the new type."""
        LOG.debug("Enter retype: id=%(id)s, new_type=%(new_type)s, "
                  "diff=%(diff)s, host=%(host)s.", {'id': volume.id,
                                                    'new_type': new_type,
                                                    'diff': diff,
                                                    'host': host})
        lun_id = self._check_volume_exist_on_array(
            volume, constants.VOLUME_NOT_EXISTS_RAISE)
        LOG.debug("Retype, the lun id of source volume is %s", lun_id)

        # Check what changes are needed
        migration, change_opts, lun_id = self.determine_changes_when_retype(
            volume, new_type, host)

        model_update = {}

        replica_type_change = change_opts.get('replication_type')
        if change_opts.get('delete_replica') == 'true':
            self.replica.delete_replica(volume)
            model_update.update({'replication_status': 'disabled',
                                 'replication_driver_data': None})
        elif (replica_type_change and
              replica_type_change[0] != replica_type_change[1]):
            msg = _("Cannot retype replication model.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if change_opts.get('delete_hypermetro') == 'true':
            metro = hypermetro.HuaweiHyperMetro(
                self.client, self.rmt_client, self.configuration)
            metro.delete_hypermetro(volume)
            metadata = huawei_utils.get_lun_metadata(volume)
            metadata['hypermetro'] = False
            model_update['provider_location'] = huawei_utils.to_string(
                **metadata)

        if migration:
            LOG.debug("Begin to migrate LUN(id: %(lun_id)s) with "
                      "change %(change_opts)s.",
                      {"lun_id": lun_id, "change_opts": change_opts})
            moved = self._migrate_volume(volume, host, new_type)
            if not moved:
                LOG.warning("Storage-assisted migration failed during "
                            "retype.")
                return False, model_update

        # Modify lun to change policy
        metro_info, replica_info = self.modify_lun(
            lun_id, change_opts, migration)

        if metro_info:
            metadata = huawei_utils.get_lun_metadata(volume)
            metadata.update({'hypermetro': True})
            model_update['provider_location'] = huawei_utils.to_string(
                **metadata)
        if replica_info:
            model_update.update(replica_info)

        return True, model_update

    def modify_lun(self, lun_id, change_opts, migration):
        data = {}
        if change_opts.get('dedup') is not None:
            data['ENABLESMARTDEDUP'] = change_opts['dedup']
        if change_opts.get('compression') is not None:
            data['ENABLECOMPRESSION'] = change_opts['compression']
        if data:
            self.client.update_lun(lun_id, data)
            LOG.info("Retype LUN(id: %(lun_id)s) dedup & compression success.",
                     {'lun_id': lun_id})

        if change_opts.get('partitionid'):
            old, new = change_opts['partitionid']
            old_id = old[0]
            old_name = old[1]
            new_id = new[0]
            new_name = new[1]
            if not migration and old_id:
                self.client.remove_lun_from_partition(lun_id, old_id)
            if new_id:
                self.client.add_lun_to_partition(lun_id, new_id)
            LOG.info("Retype LUN(id: %(lun_id)s) smartpartition from "
                     "(name: %(old_name)s, id: %(old_id)s) to "
                     "(name: %(new_name)s, id: %(new_id)s) success.",
                     {"lun_id": lun_id,
                      "old_id": old_id, "old_name": old_name,
                      "new_id": new_id, "new_name": new_name})

        if change_opts.get('cacheid'):
            old, new = change_opts['cacheid']
            old_id = old[0]
            old_name = old[1]
            new_id = new[0]
            new_name = new[1]
            if not migration and old_id:
                self.client.remove_lun_from_cache(lun_id, old_id)
            if new_id:
                self.client.add_lun_to_cache(lun_id, new_id)
            LOG.info("Retype LUN(id: %(lun_id)s) smartcache from "
                     "(name: %(old_name)s, id: %(old_id)s) to "
                     "(name: %(new_name)s, id: %(new_id)s) successfully.",
                     {'lun_id': lun_id,
                      'old_id': old_id, "old_name": old_name,
                      'new_id': new_id, "new_name": new_name})

        if change_opts.get('policy'):
            old_policy, new_policy = change_opts['policy']
            self.client.change_lun_smarttier(lun_id, new_policy)
            LOG.info("Retype LUN(id: %(lun_id)s) smarttier policy from "
                     "%(old_policy)s to %(new_policy)s success.",
                     {'lun_id': lun_id,
                      'old_policy': old_policy,
                      'new_policy': new_policy})

        if change_opts.get('qos'):
            old_qos, new_qos = change_opts['qos']
            old_qos_id = old_qos[0]
            old_qos_value = old_qos[1]
            if old_qos_id:
                smart_qos = smartx.SmartQos(self.client)
                smart_qos.remove(old_qos_id, lun_id)
            if new_qos:
                smart_qos = smartx.SmartQos(self.client)
                smart_qos.add(new_qos, lun_id)
            LOG.info("Retype LUN(id: %(lun_id)s) smartqos from "
                     "%(old_qos_value)s to %(new_qos)s success.",
                     {'lun_id': lun_id,
                      'old_qos_value': old_qos_value,
                      'new_qos': new_qos})

        metro_info = {}
        if change_opts.get('add_hypermetro') == 'true':
            metro = hypermetro.HuaweiHyperMetro(
                self.client, self.rmt_client, self.configuration)
            __, lun_params = self.get_lun_specs(lun_id)
            metro_info = metro.create_hypermetro(lun_id, lun_params,
                                                 is_sync=True)

        replica_info = {}
        if change_opts.get('add_replica') == 'true':
            lun_info = self.client.get_lun_info(lun_id)
            replica_info = self.replica.create_replica(
                lun_info, change_opts['replication_type'][1])

        return metro_info, replica_info

    def get_lun_specs(self, lun_id):
        lun_opts = {
            'policy': None,
            'partitionid': None,
            'cacheid': None,
            'LUNType': None,
            'dedup': None,
            'compression': None,
        }

        lun_info = self.client.get_lun_info(lun_id)
        lun_opts['LUNType'] = int(lun_info['ALLOCTYPE'])
        if lun_info.get('DATATRANSFERPOLICY'):
            lun_opts['policy'] = lun_info['DATATRANSFERPOLICY']
        if lun_info.get('SMARTCACHEPARTITIONID'):
            lun_opts['cacheid'] = lun_info['SMARTCACHEPARTITIONID']
        if lun_info.get('CACHEPARTITIONID'):
            lun_opts['partitionid'] = lun_info['CACHEPARTITIONID']
        if lun_info.get('ENABLESMARTDEDUP'):
            lun_opts['dedup'] = lun_info['ENABLESMARTDEDUP']
        if lun_info.get('ENABLECOMPRESSION'):
            lun_opts['compression'] = lun_info['ENABLECOMPRESSION']

        lun_params = {
            'NAME': lun_info['NAME'],
            'PARENTID': lun_info['PARENTID'],
            'DESCRIPTION': lun_info['DESCRIPTION'],
            'ALLOCTYPE': lun_info['ALLOCTYPE'],
            'CAPACITY': lun_info['CAPACITY'],
            'WRITEPOLICY': lun_info['WRITEPOLICY'],
            'EXPOSEDTOINITIATOR': lun_info['EXPOSEDTOINITIATOR'],
            'ENABLESMARTDEDUP': lun_info['ENABLESMARTDEDUP'],
            'ENABLECOMPRESSION': lun_info['ENABLECOMPRESSION'],
        }

        for k in ('DATATRANSFERPOLICY', 'PREFETCHPOLICY', 'PREFETCHVALUE',
                  'READCACHEPOLICY', 'WRITECACHEPOLICY'):
            if k in lun_info:
                lun_params[k] = lun_info[k]

        # Check whether the LUN exists in a HyperMetroPair.
        if self.support_func.get('hypermetro'):
            try:
                hypermetro_pairs = self.client.get_hypermetro_pairs()
            except exception.VolumeBackendAPIException:
                hypermetro_pairs = []
                LOG.info("Can't get hypermetro info, pass the check.")

            for pair in hypermetro_pairs:
                if pair.get('LOCALOBJID') == lun_id:
                    lun_opts['hypermetro'] = 'true'

        if 'REMOTEREPLICATIONIDS' in lun_info:
            replica_ids = json.loads(lun_info['REMOTEREPLICATIONIDS'])
            if replica_ids:
                lun_opts['replication_enabled'] = 'true'

        return lun_opts, lun_params

    def _check_capability_support(self, new_opts, new_type, pool_name):
        new_cache_name = new_opts['cachename']
        if new_cache_name:
            if not self.support_func.get('smartcache'):
                msg = (_(
                    "Can't support cache on the array, cache name is: "
                    "%(name)s.") % {'name': new_cache_name})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        new_partition_name = new_opts['partitionname']
        if new_partition_name:
            if not self.support_func.get('smartpartition'):
                msg = (_(
                    "Can't support partition on the array, partition name is: "
                    "%(name)s.") % {'name': new_partition_name})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if new_opts['policy']:
            if (not self._check_smarttier_support(pool_name)
                    and new_opts['policy'] != '0'):
                msg = (_("Can't support tier on the array."))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        new_qos = huawei_utils.get_qos_by_volume_type(new_type)
        if not self.support_func.get('QoS_support'):
            if new_qos:
                msg = (_("Can't support qos on the array."))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

    def _check_smarttier_support(self, pool_name):
        license_info = self.client.get_license_feature_status()
        if license_info.get('SmartTier') in constants.AVAILABLE_FEATURE_STATUS:
            pool_info = self.client.get_pool_by_name(pool_name)
            if pool_info['NAME'] == pool_name:
                return pool_info.get('ISSMARTTIERENABLE') == 'true'
            else:
                return False
        else:
            LOG.info("License does not support SmartTier")
            return False

    def _check_needed_changes(self, lun_id, old_opts, new_opts,
                              change_opts, new_type):
        migration = False
        new_cache_id = None
        new_cache_name = new_opts['cachename']
        if new_cache_name:
            if self.support_func.get('smartcache'):
                new_cache_id = self.client.get_cache_id_by_name(
                    new_cache_name)
            if new_cache_id is None:
                msg = (_(
                    "Can't find cache name on the array, cache name is: "
                    "%(name)s.") % {'name': new_cache_name})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        new_partition_id = None
        new_partition_name = new_opts['partitionname']
        if new_partition_name:
            if self.support_func.get('smartpartition'):
                new_partition_id = self.client.get_partition_id_by_name(
                    new_partition_name)
            if new_partition_id is None:
                msg = (_(
                    "Can't find partition name on the array, partition name "
                    "is: %(name)s.") % {'name': new_partition_name})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        # smarttier
        if (new_opts.get(constants.POLICY) and
                new_opts.get(constants.POLICY) != old_opts.get(constants.POLICY)):
            change_opts[constants.POLICY] = (
                old_opts.get(constants.POLICY),
                new_opts.get(constants.POLICY)
            )

        # smartcache
        old_cache_id = old_opts['cacheid']
        if old_cache_id == '--':
            old_cache_id = None
        if old_cache_id != new_cache_id:
            old_cache_name = None
            if self.support_func.get('smartcache'):
                if old_cache_id:
                    cache_info = self.client.get_cache_info_by_id(
                        old_cache_id)
                    old_cache_name = cache_info['NAME']
            change_opts['cacheid'] = (
                [old_cache_id, old_cache_name],
                [new_cache_id, new_cache_name]
            )

        # smartpartition
        old_partition_id = old_opts['partitionid']
        if old_partition_id == '--':
            old_partition_id = None
        if old_partition_id != new_partition_id:
            old_partition_name = None
            if self.support_func.get('smartpartition'):
                if old_partition_id:
                    partition_info = self.client.get_partition_info_by_id(
                        old_partition_id)
                    old_partition_name = partition_info['NAME']

            change_opts['partitionid'] = (
                [old_partition_id, old_partition_name],
                [new_partition_id, new_partition_name]
            )

        # smartqos
        new_qos = huawei_utils.get_qos_by_volume_type(new_type)
        if not self.support_func.get('QoS_support'):
            if new_qos:
                msg = (_("Can't support qos on the array."))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
        else:
            old_qos_id = self.client.get_qosid_by_lunid(lun_id)
            old_qos = self._get_qos_specs_from_array(old_qos_id)
            if old_qos != new_qos:
                change_opts['qos'] = ([old_qos_id, old_qos], new_qos)

        # hypermetro
        if new_opts.get('hypermetro') == constants.TRUE:
            if old_opts.get('hypermetro') != constants.TRUE:
                change_opts['add_hypermetro'] = constants.TRUE
        else:
            if old_opts.get('hypermetro') == constants.TRUE:
                change_opts['delete_hypermetro'] = constants.TRUE

        if new_opts.get('replication_enabled') == constants.TRUE:
            if old_opts.get('replication_enabled') != constants.TRUE:
                change_opts['add_replica'] = constants.TRUE
        else:
            if old_opts.get('replication_enabled') == constants.TRUE:
                change_opts['delete_replica'] = constants.TRUE

        # dedup
        if new_opts.get('dedup'):
            if (old_opts['dedup'] != constants.TRUE and
                    new_opts['dedup'] == constants.TRUE):
                migration = True
            elif (old_opts['dedup'] == constants.TRUE and
                  new_opts['dedup'] != constants.TRUE):
                change_opts['dedup'] = False
        else:
            if self.configuration.san_product == "Dorado":
                if old_opts['dedup'] != constants.TRUE:
                    migration = True
            else:
                if old_opts['dedup'] == constants.TRUE:
                    change_opts['dedup'] = False

        # compression
        if new_opts.get('compression'):
            if (old_opts['compression'] != constants.TRUE and
                    new_opts['compression'] == constants.TRUE):
                migration = True
            elif (old_opts['compression'] == constants.TRUE and
                  new_opts['compression'] != constants.TRUE):
                change_opts['compression'] = False
        else:
            if self.configuration.san_product == "Dorado":
                if old_opts['compression'] != constants.TRUE:
                    migration = True
            else:
                if old_opts['compression'] == constants.TRUE:
                    change_opts['compression'] = False

        return change_opts, migration

    def determine_changes_when_retype(self, volume, new_type, host):
        migration = False
        change_opts = {
            'policy': None,
            'partitionid': None,
            'cacheid': None,
            'qos': None,
            'host': None,
            'LUNType': None,
            'replication_enabled': None,
            'replication_type': None,
            'hypermetro': None,
            'dedup': None,
            'compression': None,
        }

        lun_id, lun_wwn = huawei_utils.get_volume_lun_id(self.client, volume)
        old_opts, lun_params = self.get_lun_specs(lun_id)

        new_specs = new_type['extra_specs']
        new_opts = self._get_volume_params_from_specs(new_specs)

        if volume.host != host['host']:
            migration = True
            change_opts['host'] = (volume.host, host['host'])
        if ('LUNType' in new_opts and
                old_opts['LUNType'] != new_opts['LUNType']):
            migration = True
            change_opts['LUNType'] = (old_opts['LUNType'], new_opts['LUNType'])

        volume_type = self._get_volume_type(volume)
        volume_opts = self._get_volume_params(volume_type)
        if (volume_opts['replication_enabled'] == constants.TRUE
                or new_opts['replication_enabled'] == constants.TRUE):
            # If replication_enabled changes,
            # then replication_type in change_opts will be set.
            change_opts['replication_enabled'] = (
                volume_opts['replication_enabled'],
                new_opts['replication_enabled']
            )

            change_opts['replication_type'] = (
                volume_opts['replication_type'],
                new_opts['replication_type']
            )

        if (volume_opts.get(constants.HYPERMETRO) == constants.TRUE
                or new_opts.get(constants.HYPERMETRO) == constants.TRUE):
            change_opts[constants.HYPERMETRO] = (
                volume_opts.get(constants.HYPERMETRO, 'false'),
                new_opts.get(constants.HYPERMETRO, 'false')
            )

        change_opts, _migration = self._check_needed_changes(
            lun_id, old_opts, new_opts, change_opts, new_type)

        migration = migration or _migration

        if (change_opts.get('add_hypermetro') == constants.TRUE
                or change_opts.get('delete_hypermetro') == constants.TRUE):
            if lun_params.get('EXPOSEDTOINITIATOR') == constants.TRUE:
                msg = _("Cann't add hypermetro to the volume in use.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug("Determine changes when retype. Migration: "
                  "%(migration)s, change_opts: %(change_opts)s.",
                  {'migration': migration, 'change_opts': change_opts})
        return migration, change_opts, lun_id

    def _get_qos_specs_from_array(self, qos_id):
        qos = {}
        qos_info = {}
        if qos_id:
            qos_info = self.client.get_qos_info(qos_id)

        qos_key = [k.upper() for k in constants.QOS_SPEC_KEYS]
        for key, value in qos_info.items():
            key = key.upper()
            if key in qos_key:
                if key == 'LATENCY' and value == '0':
                    continue
                else:
                    qos[key] = value
        return qos

    def create_export(self, context, volume, connector=None):
        """Export a volume."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def create_export_snapshot(self, context, snapshot, connector):
        """Exports the snapshot."""
        pass

    def remove_export_snapshot(self, context, snapshot):
        """Removes an export for a snapshot."""
        pass

    def backup_use_temp_snapshot(self):
        # This config option has a default to be False, So just return it.
        return self.configuration.safe_get("backup_use_temp_snapshot")

    def _copy_volume(self, src_lun, tgt_lun, copyspeed):
        luncopy_id = self.client.create_luncopy(src_lun, tgt_lun, copyspeed)

        def _luncopy_complete():
            luncopy_info = self.client.get_luncopy_info(luncopy_id)
            if not luncopy_info:
                msg = (_("Failed to get luncopy %s by luncopy id.")
                       % luncopy_id)
                raise exception.VolumeBackendAPIException(data=msg)
            if luncopy_info['status'] == constants.STATUS_LUNCOPY_READY:
                # luncopy_info['status'] means for the running status of
                # the luncopy. If luncopy_info['status'] is equal to '40',
                # this luncopy is completely ready.
                return True
            elif luncopy_info['state'] != constants.STATUS_HEALTH:
                # luncopy_info['state'] means for the healthy status of the
                # luncopy. If luncopy_info['state'] is not equal to '1',
                # this means that an error occurred during the LUNcopy
                # operation and we should abort it.
                err_msg = _(
                    'An error occurred during the LUNcopy operation. '
                    'LUNcopy name: %(luncopyname)s. '
                    'LUNcopy status: %(luncopystatus)s. '
                    'LUNcopy state: %(luncopystate)s.'
                ) % {'luncopyname': luncopy_id,
                     'luncopystatus': luncopy_info['status'],
                     'luncopystate': luncopy_info['state']}
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)
            else:
                return False

        try:
            self.client.start_luncopy(luncopy_id)
            huawei_utils.wait_for_condition(
                _luncopy_complete, self.configuration.lun_copy_wait_interval,
                self.configuration.lun_timeout)
        finally:
            self.client.delete_luncopy(luncopy_id)

    def _check_lun_valid_for_manage(self, lun_info, external_ref):
        lun_id = lun_info.get('ID')

        # Check whether the LUN is already in LUN group.
        if lun_info.get('ISADD2LUNGROUP') == 'true':
            msg = (_("Can't import LUN %s to Cinder. Already exists in a LUN "
                     "group.") % lun_id)
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        # Check whether the LUN is Normal.
        if lun_info.get('HEALTHSTATUS') != constants.STATUS_HEALTH:
            msg = _("Can't import LUN %s to Cinder. LUN status is not "
                    "normal.") % lun_id
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        # Check whether the LUN exists in a HyperMetroPair.
        if self.support_func.get('hypermetro'):
            try:
                hypermetro_pairs = self.client.get_hypermetro_pairs()
            except exception.VolumeBackendAPIException:
                hypermetro_pairs = []
                LOG.debug("Can't get hypermetro info, pass the check.")

            for pair in hypermetro_pairs:
                if pair.get('LOCALOBJID') == lun_id:
                    msg = (_("Can't import LUN %s to Cinder. Already exists "
                             "in a HyperMetroPair.") % lun_id)
                    raise exception.ManageExistingInvalidReference(
                        existing_ref=external_ref, reason=msg)

        # Check whether the LUN exists in a SplitMirror.
        if self.support_func.get('splitmirror'):
            try:
                split_mirrors = self.client.get_split_mirrors()
            except exception.VolumeBackendAPIException as ex:
                if re.search('License is unavailable', ex.msg):
                    # Can't check whether the LUN has SplitMirror with it,
                    # just pass the check and log it.
                    split_mirrors = []
                    LOG.warning('No license for SplitMirror.')
                else:
                    msg = _("Failed to get SplitMirror.")
                    raise exception.VolumeBackendAPIException(data=msg)

            for mirror in split_mirrors:
                try:
                    target_luns = self.client.get_target_luns(mirror.get('ID'))
                except exception.VolumeBackendAPIException as err:
                    msg = _("Failed to get target LUN of SplitMirror, err:%s" % err)
                    raise exception.VolumeBackendAPIException(data=msg)

                if ((mirror.get('PRILUNID') == lun_id)
                        or (lun_id in target_luns)):
                    msg = (_("Can't import LUN %s to Cinder. Already exists "
                             "in a SplitMirror.") % lun_id)
                    raise exception.ManageExistingInvalidReference(
                        existing_ref=external_ref, reason=msg)

        # Check whether the LUN exists in a migration task.
        try:
            migration_tasks = self.client.get_migration_task()
        except exception.VolumeBackendAPIException as ex:
            if re.search('License is unavailable', ex.msg):
                # Can't check whether the LUN has migration task with it,
                # just pass the check and log it.
                migration_tasks = []
                LOG.warning('No license for migration.')
            else:
                msg = _("Failed to get migration task.")
                raise exception.VolumeBackendAPIException(data=msg)

        for migration in migration_tasks:
            if lun_id in (migration.get('PARENTID'),
                          migration.get('TARGETLUNID')):
                msg = (_("Can't import LUN %s to Cinder. Already exists in a "
                         "migration task.") % lun_id)
                raise exception.ManageExistingInvalidReference(
                    existing_ref=external_ref, reason=msg)

        # Check whether the LUN exists in a LUN copy task.
        if self.support_func.get('luncopy'):
            lun_copy = lun_info.get('LUNCOPYIDS')
            if lun_copy and lun_copy[1:-1]:
                msg = (_("Can't import LUN %s to Cinder. Already exists in "
                         "a LUN copy task.") % lun_id)
                raise exception.ManageExistingInvalidReference(
                    existing_ref=external_ref, reason=msg)

        # Check whether the LUN exists in a remote replication task.
        rmt_replication = lun_info.get('REMOTEREPLICATIONIDS')
        if rmt_replication and rmt_replication[1:-1]:
            msg = (_("Can't import LUN %s to Cinder. Already exists in "
                     "a remote replication task.") % lun_id)
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

    def manage_existing(self, volume, external_ref):
        """Manage an existing volume on the backend storage."""
        # Check whether the LUN is belonged to the specified pool.
        pool = volume_utils.extract_host(volume.host, 'pool')
        LOG.debug("Pool specified is: %s.", pool)
        lun_info = self._get_lun_info_by_ref(external_ref)
        description = lun_info.get('DESCRIPTION', '')
        if len(description) <= (constants.MAX_VOL_DESCRIPTION - len(volume.name) - 1):
            description = volume.name + ' ' + description
        self._check_pool_valid_for_manage(lun_info, external_ref, pool)

        # Check other stuffs to determine whether this LUN can be imported.
        self._check_lun_valid_for_manage(lun_info, external_ref)

        # Check the attribution in volume type is valid for manage
        change_opts = self._check_volume_type_is_valid_for_manage(volume, lun_info.get('ID'), pool)

        # Rename the LUN to make it manageable for Cinder.
        new_name = huawei_utils.encode_name(volume.id)
        LOG.debug("Rename LUN %(old_name)s to %(new_name)s.",
                  {'old_name': lun_info.get('NAME'),
                   'new_name': new_name})
        self.client.rename_lun(lun_info.get('ID'), new_name, description)
        model_update = self._set_model_update(volume, change_opts, lun_info)
        huawei_utils.set_volume_lun_wwn(model_update, lun_info, volume)
        return model_update

    def _get_lun_info_by_ref(self, external_ref):
        LOG.debug("Get external_ref: %s", external_ref)
        name = external_ref.get('source-name')
        source_id = external_ref.get('source-id')
        if not (name or source_id):
            msg = _('Must specify source-name or source-id.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        lun_id = source_id or self.client.get_lun_id_by_name(name)
        if not lun_id:
            msg = _("Can't find LUN on the array, please check the "
                    "source-name or source-id.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        lun_info = self.client.get_lun_info(lun_id)
        return lun_info

    def unmanage(self, volume):
        """Export Huawei volume from Cinder."""
        lun_id = self._check_volume_exist_on_array(
            volume, constants.VOLUME_NOT_EXISTS_WARN)
        if not lun_id:
            return

        # Remove volume uuid from array lun description.
        lun_info = self.client.get_lun_info(lun_id)
        description = lun_info.get('DESCRIPTION', '')
        des_list = description.split(volume.name)
        des = ' '.join(des.strip() for des in des_list)
        self.client.update_obj_desc(lun_id, des)

        LOG.debug("Unmanage volume: %s.", volume.id)

    def manage_existing_get_size(self, volume, external_ref):
        """Get the size of the existing volume."""
        lun_info = self._get_lun_info_by_ref(external_ref)
        size = float(lun_info.get('CAPACITY')) // constants.CAPACITY_UNIT
        remainder = float(lun_info.get('CAPACITY')) % constants.CAPACITY_UNIT
        if int(remainder) > 0:
            msg = _("Volume size must be multiple of 1 GB.")
            raise exception.VolumeBackendAPIException(data=msg)
        return int(size)

    def _check_snapshot_valid_for_manage(self, snapshot_info, external_ref):
        snapshot_id = snapshot_info.get('ID')

        # Check whether the snapshot is normal.
        if snapshot_info.get('HEALTHSTATUS') != constants.STATUS_HEALTH:
            msg = _("Can't import snapshot %s to Cinder. "
                    "Snapshot status is not normal"
                    " or running status is not online.") % snapshot_id
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        if snapshot_info.get('EXPOSEDTOINITIATOR') != 'false':
            msg = _("Can't import snapshot %s to Cinder. "
                    "Snapshot is exposed to initiator.") % snapshot_id
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

    def _get_snapshot_info_by_ref(self, external_ref):
        LOG.debug("Get snapshot external_ref: %s.", external_ref)
        name = external_ref.get('source-name')
        source_id = external_ref.get('source-id')
        if not (name or source_id):
            msg = _('Must specify snapshot source-name or source-id.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        snapshot_id = source_id or self.client.get_snapshot_id_by_name(name)
        if not snapshot_id:
            msg = _("Can't find snapshot on array, please check the "
                    "source-name or source-id.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

        snapshot_info = self.client.get_snapshot_info(snapshot_id)
        return snapshot_info

    def manage_existing_snapshot(self, snapshot, existing_ref):
        snapshot_info = self._get_snapshot_info_by_ref(existing_ref)
        snapshot_id = snapshot_info.get('ID')

        parent_lun_id, lun_wwn = huawei_utils.get_volume_lun_id(
            self.client, snapshot.volume)
        if parent_lun_id != snapshot_info.get('PARENTID'):
            msg = (_("Can't import snapshot %s to Cinder. Snapshot doesn't belong to volume."), snapshot_id)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        # Check whether this snapshot can be imported.
        self._check_snapshot_valid_for_manage(snapshot_info, existing_ref)

        # Add snapshot uuid to array snapshot description and
        # keep the original array snapshot description.
        description = snapshot_info.get('DESCRIPTION', '')
        if len(description) <= (
                constants.MAX_VOL_DESCRIPTION - len(snapshot.id) - 1):
            description = snapshot.id + ' ' + description

        # Rename the snapshot to make it manageable for Cinder.
        snapshot_name = huawei_utils.encode_name(snapshot.id)
        self.client.rename_snapshot(snapshot_id, snapshot_name, description)
        if snapshot_info.get('RUNNINGSTATUS') != constants.STATUS_ACTIVE:
            self.client.activate_snapshot(snapshot_id)

        LOG.debug("Rename snapshot %(old_name)s to %(new_name)s.",
                  {'old_name': snapshot_info.get('NAME'),
                   'new_name': snapshot_name})

        location = huawei_utils.to_string(
            huawei_snapshot_id=snapshot_id,
            huawei_snapshot_wwn=snapshot_info['WWN'])
        return {'provider_location': location}

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        """Get the size of the existing snapshot."""
        snapshot_info = self._get_snapshot_info_by_ref(existing_ref)
        size = (float(snapshot_info.get('USERCAPACITY'))
                // constants.CAPACITY_UNIT)
        remainder = (float(snapshot_info.get('USERCAPACITY'))
                     % constants.CAPACITY_UNIT)
        if int(remainder) > 0:
            msg = _("Snapshot size must be multiple of 1 GB.")
            raise exception.VolumeBackendAPIException(data=msg)
        return int(size)

    def unmanage_snapshot(self, snapshot):
        """Unmanage the specified snapshot from Cinder management."""
        snapshot_id = self._check_snapshot_exist_on_array(
            snapshot, constants.SNAPSHOT_NOT_EXISTS_WARN)
        if not snapshot_id:
            return

        # Remove snapshot uuid from array lun description.
        snapshot_info = self.client.get_snapshot_info(snapshot_id)
        description = snapshot_info.get('DESCRIPTION', '')
        des_list = description.split(snapshot.id)
        des = ' '.join(des.strip() for des in des_list)
        self.client.update_obj_desc(snapshot_id, des, constants.SNAPSHOT_TYPE)

        LOG.debug("Unmanage snapshot: %s.", snapshot.id)

    def remove_host_with_check(self, host_id):
        wwns_in_host = (
            self.client.get_host_fc_initiators(host_id))
        iqns_in_host = (
            self.client.get_host_iscsi_initiators(host_id))
        if not (wwns_in_host or iqns_in_host or
           self.client.is_host_associated_to_hostgroup(host_id)):
            self.client.remove_host(host_id)

    def _get_group_type(self, group):
        opts = []
        for vol_type in group.volume_types:
            specs = vol_type.extra_specs
            opts.append(self._get_volume_params_from_specs(specs))

        return opts

    def _check_group_type_support(self, opts, vol_type):
        if not opts:
            return False

        for opt in opts:
            if opt.get(vol_type) == 'true':
                return True

        return False

    def _get_group_type_value(self, opts, vol_type):
        if not opts:
            return None

        for opt in opts:
            if vol_type in opt:
                return opt[vol_type]
        return None

    def create_group(self, context, group):
        """Creates a group."""
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            raise NotImplementedError()

        model_update = {'status': fields.GroupStatus.AVAILABLE}
        opts = self._get_group_type(group)
        for opt in opts:
            huawei_utils.check_group_volume_type_valid(opt)
        if self._check_group_type_support(opts, 'hypermetro'):
            if not self.check_local_func_support("HyperMetro_ConsistentGroup"):
                msg = (_("Can't create consistency group, array not "
                         "support hypermetro consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if not self.check_rmt_func_support("HyperMetro_ConsistentGroup"):
                msg = (_("Can't create consistency group, remote array "
                         "not support hypermetro consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            metro = hypermetro.HuaweiHyperMetro(self.client,
                                                self.rmt_client,
                                                self.configuration)
            metro.create_consistencygroup(group)
            return model_update

        if self._check_group_type_support(opts, 'replication_enabled'):
            if not self.check_local_func_support("CONSISTENTGROUP"):
                msg = (_("Can't create consistency group, array not "
                         "support replication consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if not self.check_replica_func_support("CONSISTENTGROUP"):
                msg = (_("Can't create consistency group, remote array "
                         "not support replication consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            replica_model = self._get_group_type_value(
                opts, 'replication_type')
            if not replica_model:
                replica_model = constants.REPLICA_ASYNC_MODEL
            replicg = replication.ReplicaCG(self.client,
                                            self.replica_client,
                                            self.configuration)
            replicg.create(group, replica_model)
            return model_update

        # Array will create CG at create_cgsnapshot time. Cinder will
        # maintain the CG and volumes relationship in the db.
        return model_update

    def create_group_from_src(self, context, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None):
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            raise NotImplementedError()

        if self.configuration.clone_mode == "fastclone":
            msg = ("Can't config fastclone when create "
                   "consisgroup from cgsnapshot or consisgroup")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)

        model_update = self.create_group(context, group)
        volumes_model_update = []
        delete_snapshots = False

        if not snapshots and source_vols:
            snapshots = []
            for src_vol in source_vols:
                vol_kwargs = {
                    constants.ID: src_vol.id,
                    'provider_location': src_vol.provider_location,
                }
                snapshot_kwargs = {
                    constants.ID: six.text_type(uuid.uuid4()),
                    'volume': objects.Volume(**vol_kwargs),
                    'volume_size': src_vol.size
                }
                snapshot = objects.Snapshot(**snapshot_kwargs)
                snapshots.append(snapshot)

            snapshots_model_update = self._create_group_snapshot(snapshots)
            for i, model in enumerate(snapshots_model_update):
                snapshot = snapshots[i]
                snapshot.provider_location = model['provider_location']

            delete_snapshots = True

        if snapshots:
            for i, vol in enumerate(volumes):
                snapshot = snapshots[i]
                vol_model_update = self.create_volume_from_snapshot(
                    vol, snapshot)
                vol_model_update.update({constants.ID: vol.id})
                volumes_model_update.append(vol_model_update)

        if delete_snapshots:
            self._delete_group_snapshot(snapshots)

        return model_update, volumes_model_update

    def delete_group(self, context, group, volumes):
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            raise NotImplementedError()

        opts = self._get_group_type(group)
        model_update = {constants.STATUS: fields.GroupStatus.DELETED}
        volumes_model_update = []

        if self._check_group_type_support(opts, 'hypermetro'):
            metro = hypermetro.HuaweiHyperMetro(self.client,
                                                self.rmt_client,
                                                self.configuration)
            metro.delete_consistencygroup(context, group, volumes)

        if self._check_group_type_support(opts, 'replication_enabled'):
            replicg = replication.ReplicaCG(self.client,
                                            self.replica_client,
                                            self.configuration)
            replicg.delete(group, volumes)

        for volume in volumes:
            volume_model_update = {'id': volume.id}
            try:
                self.delete_volume(volume)
            except Exception:
                LOG.exception('Delete volume %s failed.', volume)
                volume_model_update.update({constants.STATUS: 'error_deleting'})
            else:
                volume_model_update.update({constants.STATUS: 'deleted'})

            volumes_model_update.append(volume_model_update)

        return model_update, volumes_model_update

    def update_group(self, context, group,
                     add_volumes=None, remove_volumes=None):
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            raise NotImplementedError()

        model_update = {'status': fields.GroupStatus.AVAILABLE}
        opts = self._get_group_type(group)
        if self._check_group_type_support(opts, 'hypermetro'):
            metro = hypermetro.HuaweiHyperMetro(self.client,
                                                self.rmt_client,
                                                self.configuration)
            metro.update_consistencygroup(context, group,
                                          add_volumes,
                                          remove_volumes)
            return model_update, None, None

        if self._check_group_type_support(opts, 'replication_enabled'):
            replica_model = self._get_group_type_value(
                opts, 'replication_type')
            if not replica_model:
                replica_model = constants.REPLICA_ASYNC_MODEL
            replicg = replication.ReplicaCG(self.client,
                                            self.replica_client,
                                            self.configuration)
            replicg.update(group, add_volumes, remove_volumes, replica_model)
            return model_update, None, None

        for volume in add_volumes:
            try:
                self._check_volume_exist_on_array(
                    volume, constants.VOLUME_NOT_EXISTS_RAISE)
            except Exception as err:
                raise exception.VolumeDriverException(message=err)

        # Array will create CG at create_cgsnapshot time. Cinder will
        # maintain the CG and volumes relationship in the db.
        return model_update, None, None

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        """Create group snapshot."""
        if not volume_utils.is_group_a_cg_snapshot_type(group_snapshot):
            raise NotImplementedError()

        LOG.info('Create group snapshot for group: %(group_id)s',
                 {'group_id': group_snapshot.group_id})

        try:
            snapshots_model_update = self._create_group_snapshot(snapshots)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error("Create group snapshots failed. "
                          "Group snapshot id: %s.", group_snapshot.id)

        model_update = {'status': fields.GroupSnapshotStatus.AVAILABLE}
        return model_update, snapshots_model_update

    def _create_group_snapshot(self, snapshots):
        snapshots_model_update = []
        added_snapshots_info = []

        try:
            for snapshot in snapshots:
                snapshot_id = self._create_snapshot_base(snapshot)
                info = self.client.get_snapshot_info(snapshot_id)
                location = huawei_utils.to_string(
                    huawei_snapshot_id=info[constants.ID_UPPER],
                    huawei_snapshot_wwn=info['WWN'])
                snapshot_model_update = {
                    'id': snapshot.id,
                    'status': fields.SnapshotStatus.AVAILABLE,
                    'provider_location': location,
                }
                snapshots_model_update.append(snapshot_model_update)
                added_snapshots_info.append(info)
        except Exception:
            with excutils.save_and_reraise_exception():
                for added_snapshot in added_snapshots_info:
                    self.client.delete_snapshot(added_snapshot[constants.ID_UPPER])

        snapshot_ids = [added_snapshot[constants.ID_UPPER]
                        for added_snapshot in added_snapshots_info]
        try:
            self.client.activate_snapshot(snapshot_ids)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error("Active group snapshots %s failed.", snapshot_ids)
                for snapshot_id in snapshot_ids:
                    self.client.delete_snapshot(snapshot_id)

        return snapshots_model_update

    def delete_group_snapshot(self, context, group_snapshot, snapshots):
        """Delete group snapshot."""
        if not volume_utils.is_group_a_cg_snapshot_type(group_snapshot):
            raise NotImplementedError()

        LOG.info('Delete group snapshot %(snap_id)s for group: '
                 '%(group_id)s',
                 {'snap_id': group_snapshot.id,
                  'group_id': group_snapshot.group_id})

        try:
            snapshots_model_update = self._delete_group_snapshot(snapshots)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error("Delete group snapshots failed. "
                          "Group snapshot id: %s", group_snapshot.id)

        model_update = {'status': fields.GroupSnapshotStatus.DELETED}
        return model_update, snapshots_model_update

    def _delete_group_snapshot(self, snapshots):
        snapshots_model_update = []
        for snapshot in snapshots:
            self.delete_snapshot(snapshot)
            snapshot_model_update = {
                'id': snapshot.id,
                'status': fields.SnapshotStatus.DELETED
            }
            snapshots_model_update.append(snapshot_model_update)

        return snapshots_model_update

    def _get_consistencygroup_type(self, group):
        cg_opts = []
        type_ids = []

        if group['volume_type_id']:
            type_ids = filter(lambda x: x, group['volume_type_id'].split(","))
        for type_id in type_ids:
            ctxt = cinder_context.get_admin_context()
            volume_type = volume_types.get_volume_type(ctxt, type_id)
            specs = dict(volume_type).get('extra_specs')
            opts = self._get_volume_params_from_specs(specs)
            cg_opts.append(opts)

        return cg_opts

    def _cg_volume_type_support(self, cg_opts, vol_type):
        if not cg_opts:
            return False

        for opt in cg_opts:
            if opt.get(vol_type) == 'true':
                return True

        return False

    def _get_cg_volume_type_value(self, cg_opts, vol_type):
        if not cg_opts:
            return None

        for opt in cg_opts:
            if vol_type in opt:
                return opt[vol_type]
        return None

    def create_consistencygroup(self, context, group):
        """Creates a consistencygroup."""
        model_update = {'status': 'available'}
        cg_opts = self._get_consistencygroup_type(group)

        for opt in cg_opts:
            huawei_utils.check_group_volume_type_valid(opt)

        if self._cg_volume_type_support(cg_opts, 'hypermetro'):
            if not self.check_local_func_support("HyperMetro_ConsistentGroup"):
                msg = (_("Can't create consistency group, array not "
                         "support hypermetro consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if not self.check_rmt_func_support("HyperMetro_ConsistentGroup"):
                msg = (_("Can't create consistency group, remote array "
                         "not support hypermetro consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            metro = hypermetro.HuaweiHyperMetro(self.client,
                                                self.rmt_client,
                                                self.configuration)
            metro.create_consistencygroup(group)
            return model_update

        if self._cg_volume_type_support(cg_opts, 'replication_enabled'):
            if not self.check_local_func_support("CONSISTENTGROUP"):
                msg = (_("Can't create consistency group, array not "
                         "support replication consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if not self.check_replica_func_support("CONSISTENTGROUP"):
                msg = (_("Can't create consistency group, remote array "
                         "not support replication consistentgroup, "
                         "group id: %(group_id)s.")
                       % {"group_id": group.id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            replica_model = self._get_cg_volume_type_value(
                cg_opts, 'replication_type')
            if not replica_model:
                replica_model = constants.REPLICA_ASYNC_MODEL
            replicg = replication.ReplicaCG(self.client,
                                            self.replica_client,
                                            self.configuration)
            replicg.create(group, replica_model)
            return model_update

        # Array will create CG at create_cgsnapshot time. Cinder will
        # maintain the CG and volumes relationship in the db.
        return model_update

    def create_consistencygroup_from_src(self, context, group, volumes,
                                         cgsnapshot=None, snapshots=None,
                                         source_cg=None, source_vols=None):
        model_update = self.create_consistencygroup(context, group)
        volumes_model_update = []
        delete_snapshots = False

        if not snapshots and source_vols:
            snapshots = []
            for src_vol in source_vols:
                vol_kwargs = {
                    constants.ID: src_vol.id,
                    'provider_location': src_vol.provider_location,
                }
                snapshot_kwargs = {
                    constants.ID: six.text_type(uuid.uuid4()),
                    'volume': objects.Volume(**vol_kwargs),
                    'volume_size': src_vol.size
                }
                snapshot = objects.Snapshot(**snapshot_kwargs)
                snapshots.append(snapshot)

            snapshots_model_update = self._create_cgsnapshot(snapshots)
            for i, model in enumerate(snapshots_model_update):
                snapshot = snapshots[i]
                snapshot.provider_location = model['provider_location']

            delete_snapshots = True

        if snapshots:
            for i, vol in enumerate(volumes):
                snapshot = snapshots[i]
                vol_model_update = self.create_volume_from_snapshot(
                    vol, snapshot)
                vol_model_update.update({constants.ID: vol.id})
                volumes_model_update.append(vol_model_update)

        if delete_snapshots:
            self._delete_cgsnapshot(snapshots)

        return model_update, volumes_model_update

    def delete_consistencygroup(self, context, group, volumes):
        cg_opts = self._get_consistencygroup_type(group)
        volumes_model_update = []

        if self._cg_volume_type_support(cg_opts, 'hypermetro'):
            metro = hypermetro.HuaweiHyperMetro(self.client,
                                                self.rmt_client,
                                                self.configuration)
            metro.delete_consistencygroup(context, group, volumes)

        if self._cg_volume_type_support(cg_opts, 'replication_enabled'):
            replicg = replication.ReplicaCG(self.client,
                                            self.replica_client,
                                            self.configuration)
            replicg.delete(group, volumes)

        for volume in volumes:
            volume_model_update = {'id': volume.id}
            try:
                self.delete_volume(volume)
            except Exception:
                LOG.exception(_('Delete volume %s failed.'), volume)
                volume_model_update[constants.STATUS] = 'error_deleting'
            else:
                volume_model_update[constants.STATUS] = 'deleted'

            volumes_model_update.append(volume_model_update)

        model_update = {constants.STATUS: 'deleted'}
        return model_update, volumes_model_update

    def update_consistencygroup(self, context, group,
                                add_volumes,
                                remove_volumes):
        model_update = {'status': 'available'}
        cg_opts = self._get_consistencygroup_type(group)

        if self._cg_volume_type_support(cg_opts, 'hypermetro'):
            metro = hypermetro.HuaweiHyperMetro(self.client,
                                                self.rmt_client,
                                                self.configuration)
            metro.update_consistencygroup(context, group,
                                          add_volumes,
                                          remove_volumes)
            return model_update, None, None

        if self._cg_volume_type_support(cg_opts, 'replication_enabled'):
            replica_model = self._get_cg_volume_type_value(
                cg_opts, 'replication_type')
            if not replica_model:
                replica_model = constants.REPLICA_ASYNC_MODEL
            replicg = replication.ReplicaCG(self.client,
                                            self.replica_client,
                                            self.configuration)
            replicg.update(group, add_volumes, remove_volumes, replica_model)
            return model_update, None, None

        for volume in add_volumes:
            try:
                self._check_volume_exist_on_array(
                    volume, constants.VOLUME_NOT_EXISTS_RAISE)
            except Exception as err:
                raise exception.VolumeDriverException(message=err)

        # Array will create CG at create_cgsnapshot time. Cinder will
        # maintain the CG and volumes relationship in the db.
        return model_update, None, None

    def create_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Create cgsnapshot."""
        LOG.info(_('Create cgsnapshot for consistency group'
                     ': %(group_id)s'),
                 {'group_id': cgsnapshot.consistencygroup_id})

        try:
            snapshots_model_update = self._create_cgsnapshot(snapshots)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Create cgsnapshot for %s failed."),
                          cgsnapshot.id)

        model_update = {'status': 'available'}
        return model_update, snapshots_model_update

    def _create_cgsnapshot(self, snapshots):
        snapshots_model_update = []
        added_snapshots_info = []

        try:
            for snapshot in snapshots:
                snapshot_id = self._create_snapshot_base(snapshot)
                info = self.client.get_snapshot_info(snapshot_id)
                location = huawei_utils.to_string(
                    huawei_snapshot_id=info[constants.ID_UPPER],
                    huawei_snapshot_wwn=info['WWN'])
                snapshot_model_update = {
                    'id': snapshot.id,
                    'status': 'available',
                    'provider_location': location
                }
                snapshots_model_update.append(snapshot_model_update)
                added_snapshots_info.append(info)
        except Exception:
            with excutils.save_and_reraise_exception():
                for added_snapshot in added_snapshots_info:
                    self.client.delete_snapshot(added_snapshot[constants.ID_UPPER])

        snapshot_ids = [added_snapshot[constants.ID_UPPER]
                        for added_snapshot in added_snapshots_info]
        try:
            self.client.activate_snapshot(snapshot_ids)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Active cgsnapshots %s failed."), snapshot_ids)
                for snapshot_id in snapshot_ids:
                    self.client.delete_snapshot(snapshot_id)

        return snapshots_model_update

    def delete_cgsnapshot(self, context, cgsnapshot, snapshots):
        """Delete consistency group snapshot."""
        LOG.info(_('Delete cgsnapshot %(snap_id)s for consistency group: '
                     '%(group_id)s'),
                 {'snap_id': cgsnapshot.id,
                  'group_id': cgsnapshot.consistencygroup_id})

        try:
            snapshots_model_update = self._delete_cgsnapshot(snapshots)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Delete cg snapshots failed. "
                              "Cgsnapshot id: %s"), cgsnapshot.id)

        model_update = {'status': 'deleted'}
        return model_update, snapshots_model_update

    def _delete_cgsnapshot(self, snapshots):
        snapshots_model_update = []
        for snapshot in snapshots:
            self.delete_snapshot(snapshot)
            snapshot_model_update = {'id': snapshot.id, 'status': 'deleted'}
            snapshots_model_update.append(snapshot_model_update)

        return snapshots_model_update

    def _classify_volume(self, volumes):
        normal_volumes = []
        replica_volumes = []

        for v in volumes:
            volume_type = self._get_volume_type(v)
            opts = self._get_volume_params(volume_type)
            if opts.get('replication_enabled') == 'true':
                replica_volumes.append(v)
            else:
                normal_volumes.append(v)

        return normal_volumes, replica_volumes

    def _failback_normal_volumes(self, volumes):
        volumes_update = []
        for v in volumes:
            v_update = {}
            v_update['volume_id'] = v.id
            metadata = huawei_utils.get_volume_metadata(v)
            old_status = 'available'
            if constants.OLD_STATUS in metadata:
                old_status = metadata[constants.OLD_STATUS]
                del metadata[constants.OLD_STATUS]
            v_update['updates'] = {'status': old_status, 'metadata': metadata}
            volumes_update.append(v_update)

        return volumes_update

    def _failback(self, volumes):
        if self.active_backend_id in ('', None):
            return 'default', []

        normal_volumes, replica_volumes = self._classify_volume(volumes)
        volumes_update = []

        replica_volumes_update = self.replica.failback(replica_volumes)
        volumes_update.extend(replica_volumes_update)

        normal_volumes_update = self._failback_normal_volumes(normal_volumes)
        volumes_update.extend(normal_volumes_update)

        self.active_backend_id = ""
        secondary_id = 'default'

        # Switch array connection.
        self.client, self.replica_client = self.replica_client, self.client
        self.replica = replication.ReplicaPairManager(self.client,
                                                      self.replica_client,
                                                      self.configuration)
        return secondary_id, volumes_update

    def _failover_normal_volumes(self, volumes):
        volumes_update = []

        for v in volumes:
            v_update = {}
            v_update['volume_id'] = v.id
            metadata = huawei_utils.get_volume_metadata(v)
            metadata.update({'old_status': v['status']})
            v_update['updates'] = {'status': 'error', 'metadata': metadata}
            volumes_update.append(v_update)

        return volumes_update

    def _failover(self, volumes):
        if self.active_backend_id not in ('', None):
            return self.replica_dev_conf.get('backend_id'), []

        normal_volumes, replica_volumes = self._classify_volume(volumes)
        volumes_update = []

        replica_volumes_update = self.replica.failover(replica_volumes)
        volumes_update.extend(replica_volumes_update)

        normal_volumes_update = self._failover_normal_volumes(normal_volumes)
        volumes_update.extend(normal_volumes_update)

        self.active_backend_id = self.replica_dev_conf.get('backend_id')
        secondary_id = self.active_backend_id

        # Switch array connection.
        self.client, self.replica_client = self.replica_client, self.client
        self.replica = replication.ReplicaPairManager(self.client,
                                                      self.replica_client,
                                                      self.configuration)
        return secondary_id, volumes_update

    def failover_host(self, context, volumes, secondary_id=None):
        """Failover all volumes to secondary."""
        if secondary_id == 'default':
            try:
                secondary_id, volumes_update = self._failback(volumes)
            except exception.VolumeBackendAPIException as err:
                msg = _("Error encountered during failback,err:%s." % err)
                LOG.exception(msg)
                raise exception.VolumeDriverException(data=msg)
        elif (secondary_id == self.replica_dev_conf.get('backend_id')
                or secondary_id is None):
            try:
                secondary_id, volumes_update = self._failover(volumes)
            except exception.VolumeBackendAPIException as err:
                msg = _("Error encountered during failover, err:%s" % err)
                LOG.exception(msg)
                raise exception.VolumeDriverException(data=msg)
        else:
            msg = _("Invalid secondary id %s.") % secondary_id
            LOG.error(msg)
            raise exception.InvalidReplicationTarget(reason=msg)

        return secondary_id, volumes_update

    def initialize_connection_snapshot(self, snapshot, connector, **kwargs):
        """Map a snapshot to a host and return target iSCSI information."""
        LOG.info(('initialize_connection_snapshot for snapshot: '
                  '%(snapshot_id)s.')
                 % {'snapshot_id': snapshot.id})

        # From the volume structure.
        volume = Volume(id=snapshot.id,
                        provider_location=snapshot.provider_location,
                        lun_type=constants.SNAPSHOT_TYPE,
                        metadata=None,
                        multiattach=False,
                        volume_attachment=[])

        return self.initialize_connection(volume, connector)

    def terminate_connection_snapshot(self, snapshot, connector, **kwargs):
        """Delete map between a snapshot and a host."""
        LOG.info(('terminate_connection_snapshot for snapshot: '
                  '%(snapshot_id)s.')
                 % {'snapshot_id': snapshot.id})

        # From the volume structure.
        volume = Volume(id=snapshot.id,
                        provider_location=snapshot.provider_location,
                        lun_type=constants.SNAPSHOT_TYPE,
                        metadata=None,
                        multiattach=False,
                        volume_attachment=[])

        return self.terminate_connection(volume, connector)

    def get_lun_id_and_type(self, volume, action, local=True):
        if hasattr(volume, 'lun_type'):
            metadata = huawei_utils.get_snapshot_metadata(volume)
            lun_id = metadata['huawei_snapshot_id']
            lun_type = constants.SNAPSHOT_TYPE
            sp_info = self.client.get_snapshot_info(lun_id)
            if not self.client.check_snapshot_exist(lun_id):
                msg = ("Snapshot %s does not exist on the array."
                       % volume.id)
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if (sp_info.get('HEALTHSTATUS') != constants.STATUS_HEALTH or
                    sp_info.get('RUNNINGSTATUS') != constants.STATUS_ACTIVE):
                msg = ("Snapshot %s status is not normal "
                       "or running status is not online." % lun_id)
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
        else:
            lun_id = self._check_volume_exist_on_array(
                volume, action, local)
            lun_type = constants.LUN_TYPE

        return lun_id, lun_type

    def _get_same_hostid(self, loc_fc_info, rmt_fc_info):
        loc_aval_luns = loc_fc_info['aval_luns']
        loc_aval_luns = json.loads(loc_aval_luns)

        rmt_aval_luns = rmt_fc_info['aval_luns']
        rmt_aval_luns = json.loads(rmt_aval_luns)
        same_host_id = None

        for i in range(1, 512):
            if i in rmt_aval_luns and i in loc_aval_luns:
                same_host_id = i
                break

        LOG.info("The same hostid is: %s.", same_host_id)
        if not same_host_id:
            msg = _("Can't find the same host id from arrays.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return same_host_id

    def _rollback_snapshot(self, snapshot_id):

        def _snapshot_rollback_finish():
            snapshot_info = self.client.get_snapshot_info(snapshot_id)
            if not snapshot_info:
                msg = (_("Failed to get rollback info with snapshot %s.")
                       % snapshot_id)
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            if snapshot_info.get('HEALTHSTATUS') not in (
                    constants.SNAPSHOT_HEALTH_STATUS_NORMAL,):
                msg = _("The snapshot %s is abnormal.") % snapshot_id
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            if (snapshot_info.get('ROLLBACKRATE') ==
                    constants.SNAPSHOT_ROLLBACK_PROGRESS_FINISH or
                    snapshot_info.get('ROLLBACKENDTIME') != '-1'):
                LOG.info("Snapshot %s rollback successful.", snapshot_id)
                return True
            return False

        if huawei_utils.is_snapshot_rollback_available(self.client,
                                                       snapshot_id):
            self.client.rollback_snapshot(snapshot_id,
                                          self.configuration.rollback_speed)
        try:
            huawei_utils.wait_for_condition(_snapshot_rollback_finish,
                                            constants.DEFAULT_WAIT_INTERVAL,
                                            constants.DEFAULT_WAIT_TIMEOUT)
        except exception.VolumeBackendAPIException:
            self.client.cancel_rollback_snapshot(snapshot_id)
            raise

    def revert_to_snapshot(self, context, volume, snapshot):
        snapshot_id = self._check_snapshot_exist_on_array(
            snapshot, constants.SNAPSHOT_NOT_EXISTS_WARN)
        if not snapshot_id:
            msg = _("Snapshot %s does not exist.") % snapshot.id
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        self._rollback_snapshot(snapshot_id)

    def _check_pool_valid_for_manage(self, lun_info, external_ref, pool):
        lun_pool = lun_info.get('PARENTNAME')
        LOG.debug("Storage pool of existing LUN %(lun)s is %(pool)s.",
                  {"lun": lun_info.get('ID'), "pool": lun_pool})
        if pool != lun_pool:
            msg = (_("The specified LUN does not belong to the given "
                     "pool: %s.") % pool)
            raise exception.ManageExistingInvalidReference(
                existing_ref=external_ref, reason=msg)

    def _check_volume_type_is_valid_for_manage(self, volume, lun_id, pool):
        if not volume.volume_type_id:
            return {}
        old_opts, lun_params = self.get_lun_specs(lun_id)
        LOG.debug("Begin to check volume type, the lun params is %s", lun_params)
        volume_type = volume_types.get_volume_type(
            None, volume.volume_type_id)
        new_specs = volume_type.get('extra_specs')
        new_opts = self._get_volume_params_from_specs(new_specs)
        if ('LUNType' in new_opts and
                old_opts['LUNType'] != new_opts['LUNType']):
            msg = (_("Can't import LUN %s to Cinder. "
                     "LUN type mismatched.") % lun_id)
            raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        if (new_opts.get('dedup') and
                old_opts['dedup'] != new_opts['dedup']):
            msg = (_("Can't import LUN %s to Cinder. "
                     "Dedup function mismatched.") % lun_id)
            raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        if (new_opts.get('compression') and
                old_opts['compression'] != new_opts['compression']):
            msg = (_("Can't import LUN %s to Cinder. "
                     "Compression function mismatched.") % lun_id)
            raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        self._check_capability_support(new_opts, volume_type, pool)

        change_opts = {
            'policy': None, 'partitionid': None, 'cacheid': None,
            'qos': None,
            'replication_type': (constants.REPLICA_SYNC_MODEL,
                                 new_opts['replication_type']),
        }
        change_opts, migration = self._check_needed_changes(
            lun_id, old_opts, new_opts, change_opts, volume_type)
        LOG.debug("End to check volume,the change_opts:%s, migration:%s", change_opts, migration)
        return change_opts

    def _set_model_update(self, volume, change_opts, lun_info):
        # Handle volume type if specified.
        model_update = {}
        provider_location = {
            'huawei_lun_id': lun_info.get('ID'),
            'huawei_sn': self.sn,
            'huawei_lun_wwn': lun_info.get('WWN')
        }

        if volume.volume_type_id:
            metro_info, replica_info = self.modify_lun(lun_info.get('ID'), change_opts, False)
            if metro_info:
                provider_location.update(metro_info)
            if replica_info:
                model_update.update(replica_info)

        model_update['provider_location'] = huawei_utils.to_string(**provider_location)
        return model_update


class HuaweiISCSIDriver(HuaweiBaseDriver, driver.ISCSIDriver):
    """ISCSI driver for Huawei storage arrays.

    Version history:
        1.0.0 - Initial driver
        1.1.0 - Provide Huawei OceanStor storage 18000 driver
        1.1.1 - Code refactor
                CHAP support
                Multiple pools support
                ISCSI multipath support
                SmartX support
                Volume migration support
                Volume retype support
        2.0.0 - Rename to HuaweiISCSIDriver
        2.0.1 - Manage/unmanage volume support
        2.0.2 - Refactor HuaweiISCSIDriver
        2.0.3 - Manage/unmanage snapshot support
        2.0.5 - Replication V2 support
        2.0.6 - Support iSCSI configuration in Replication
        2.0.7 - Hypermetro support
                Hypermetro consistency group support
                Consistency group support
                Cgsnapshot support
        2.0.8 - Backup snapshot optimal path support
        2.0.9 - Support reporting disk type of pool
        2.2.RC1 - Add force delete volume
    """


    def __init__(self, *args, **kwargs):
        super(HuaweiISCSIDriver, self).__init__(*args, **kwargs)

    def get_volume_stats(self, refresh=False):
        """Get volume status."""
        data = HuaweiBaseDriver.get_volume_stats(self, refresh=False)
        backend_name = self.configuration.safe_get('volume_backend_name')
        data['volume_backend_name'] = backend_name or self.__class__.__name__
        data['storage_protocol'] = 'iSCSI'
        data['driver_version'] = self.VERSION
        data['vendor_name'] = 'Huawei'
        return data

    def initialize_connection(self, volume, connector):
        """Map a volume to a host and return target iSCSI information."""
        host = "" if connector is None else connector.get('host', "")
        lock_mapping = 'huawei-mapping-%s' % host

        @utils.synchronized(lock_mapping, external=True)
        def lock_host_when_initialize_connection():
            return self._initialize_connection_lock(volume, connector)
        return lock_host_when_initialize_connection()

    def _initialize_connection_lock(self, volume, connector):
        """Map a volume to a host and return target iSCSI information."""
        # Attach local lun.
        iscsi_info = self._initialize_connection(volume, connector)

        # Attach remote lun if exists.
        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("Attach Volume, metadata is: %s.", metadata)
        if metadata.get('hypermetro'):
            try:
                rmt_iscsi_info = (
                    self._initialize_connection(volume, connector, False))
            except Exception:
                with excutils.save_and_reraise_exception():
                    self._terminate_connection(volume, connector)

            iscsi_info = self._multipath_iscsi_info(
                connector, iscsi_info, rmt_iscsi_info)

        LOG.info('initialize_common_connection_iscsi, '
                 'return data is: %s.',
                 huawei_utils.mask_initiator_sensitive_info(
                     huawei_utils.mask_dict_sensitive_info(iscsi_info),
                     sensitive_keys=['target_iqns', 'target_iqn']))
        return iscsi_info

    def _initialize_connection(self, volume, connector, local=True):
        LOG.info('_initialize_connection, attach %(local)s volume.',
                 {'local': 'local' if local else 'remote'})

        # Determine use which client, local or remote.
        client = self.client if local else self.rmt_client

        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_RAISE, local)
        lun_info = client.get_lun_info(lun_id, lun_type)

        initiator_name = connector['initiator']
        LOG.info(
            'The info of initialize connection is:LUN ID: %s, lun type: %s, connector: %s.', lun_id, lun_type,
            huawei_utils.mask_initiator_sensitive_info(connector, sensitive_keys=constants.SENSITIVE_INI_KEYS)
        )

        iscsi_iqns, target_ips, portgroup_id = client.get_iscsi_params(connector)
        LOG.info('initialize_connection, iscsi_iqn: %(iscsi_iqn)s, '
                 'target_ip: %(target_ip)s, '
                 'portgroup_id: %(portgroup_id)s.',
                 {'iscsi_iqn': huawei_utils.mask_initiator_sensitive_info(list(iscsi_iqns)),
                  'target_ip': target_ips,
                  'portgroup_id': portgroup_id})

        # Create hostgroup if not exist.
        host_id = client.add_host_with_check(connector['host'], self.is_dorado_v6, initiator_name)
        try:
            client.ensure_initiator_added(initiator_name, host_id, connector['host'])
        except Exception:
            with excutils.save_and_reraise_exception():
                self.remove_host_with_check(host_id)

        hostgroup_id = client.add_host_to_hostgroup(host_id)

        metadata = huawei_utils.get_lun_metadata(volume)
        hypermetro_lun = metadata.get('hypermetro')

        # Mapping lungroup and hostgroup to view.
        map_info = client.do_mapping(lun_info, hostgroup_id, host_id,
                                     portgroup_id, lun_type, hypermetro_lun)

        hostlun_id = client.get_host_lun_id(host_id, lun_info, lun_type)

        LOG.info("initialize_connection, host lun id is: %s.", hostlun_id)

        chapinfo = client.find_chap_info(client.iscsi_info, initiator_name, connector['host'])

        if not chapinfo and client.is_initiator_used_chap(initiator_name):
            msg = (_("Chap is not configured but initiator %s used chap on "
                     "array, please check and remove chap for this initiator.")
                   % huawei_utils.mask_initiator_sensitive_info(initiator_name))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        mapping_param = {
            'map_info': map_info, 'hostlun_id': hostlun_id, 'iscsi_iqns': iscsi_iqns,
            'target_ips': target_ips, 'chapinfo': chapinfo
        }
        return self._get_iscsi_properties_info(volume, lun_type, lun_info, connector, mapping_param)

    def _get_iscsi_properties_info(self, volume, lun_type, lun_info, connector, mapping_param):
        # Return iSCSI properties.
        iscsi_iqns = mapping_param.get('iscsi_iqns')
        target_ips = mapping_param.get('target_ips')
        chapinfo = mapping_param.get('chapinfo')
        properties = {}
        properties['map_info'] = mapping_param.get('map_info')
        properties['target_discovered'] = False
        properties['volume_id'] = volume.id
        port_string = '%s:3260'

        if lun_type == constants.LUN_TYPE and \
                int(lun_info.get('ALLOCTYPE')) == constants.THIN_LUNTYPE:
            properties['discard'] = True

        multipath = connector.get('multipath', False)
        hostlun_id = int(mapping_param.get('hostlun_id'))
        if multipath:
            properties['target_iqns'] = [iqn for iqn in iscsi_iqns]
            properties['target_portals'] = [port_string % ip for ip in target_ips]
            properties['target_luns'] = [hostlun_id] * len(target_ips)
        elif self.use_ultrapath:
            properties['target_iqns'] = [iqn for iqn in iscsi_iqns]
            properties['target_portals'] = [port_string % ip for ip in target_ips]
            properties['target_luns'] = [hostlun_id] * len(target_ips)
            properties['target_num'] = len(target_ips)
            properties['libvirt_iscsi_use_ultrapath'] = self.use_ultrapath
            properties['lun_wwn'] = lun_info['WWN']
        else:
            properties['target_portal'] = (port_string % target_ips[0])
            properties['target_iqn'] = iscsi_iqns[0]
            properties['target_lun'] = hostlun_id

        LOG.info("initialize_connection success. Return data: %s.",
                 huawei_utils.mask_initiator_sensitive_info(
                     properties, sensitive_keys=['target_iqns', 'target_iqn']))

        # If use CHAP, return CHAP info.
        if chapinfo:
            chap_username, chap_password = chapinfo.split(';')
            properties['auth_method'] = 'CHAP'
            properties['auth_username'] = chap_username
            properties['auth_password'] = chap_password

        return {'driver_volume_type': 'iscsi', 'data': properties}

    def _multipath_iscsi_info(self, connector, iscsi_info, rmt_iscsi_info):
        multipath = connector.get('multipath', False)
        if multipath:
            target_iqn = []
            target_iqn.extend(iscsi_info[constants.DATA][constants.TARGET_IQNS])
            target_iqn.extend(rmt_iscsi_info[constants.DATA][constants.TARGET_IQNS])
            iscsi_info[constants.DATA][constants.TARGET_IQNS] = target_iqn

            target_portal = []
            target_portal.extend(iscsi_info[constants.DATA][constants.TARGET_PORTALS])
            target_portal.extend(rmt_iscsi_info[constants.DATA][constants.TARGET_PORTALS])
            iscsi_info[constants.DATA][constants.TARGET_PORTALS] = target_portal
            iscsi_info[constants.DATA][constants.TARGET_LUNS].extend(
                rmt_iscsi_info[constants.DATA][constants.TARGET_LUNS])
        elif self.use_ultrapath:
            target_iqn = []
            target_iqn.extend(iscsi_info[constants.DATA][constants.TARGET_IQNS])
            target_iqn.extend(rmt_iscsi_info[constants.DATA][constants.TARGET_IQNS])
            iscsi_info[constants.DATA][constants.TARGET_IQNS] = target_iqn
            target_portal = []
            target_portal.extend(iscsi_info[constants.DATA][constants.TARGET_PORTALS])
            target_portal.extend(rmt_iscsi_info[constants.DATA][constants.TARGET_PORTALS])
            iscsi_info[constants.DATA][constants.TARGET_PORTALS] = target_portal
            iscsi_info[constants.DATA][constants.TARGET_LUNS].extend(
                rmt_iscsi_info[constants.DATA][constants.TARGET_LUNS])
        elif self.configuration.enforce_multipath_for_hypermetro:
            msg = (_("Hypermetro must use multipath or ultrapath."))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return iscsi_info

    def terminate_connection(self, volume, connector, **kwargs):
        """Delete map between a volume and a host."""
        host = "" if connector is None else connector.get('host', "")
        lock_mapping = 'huawei-mapping-%s' % host

        @utils.synchronized(lock_mapping, external=True)
        def lock_host_when_terminate_connection():
            return self._terminate_connection_locked(volume, connector, **kwargs)

        return lock_host_when_terminate_connection()

    def _terminate_connection_locked(self, volume, connector, **kwargs):
        """Delete map between a volume and a host."""
        attachments = volume.volume_attachment
        if volume.multiattach and len(attachments) > 1 and sum(
                1 for a in attachments if a.connector == connector) > 1:
            LOG.info("Volume is multi-attach and attached to the same host"
                     " multiple times")
            return

        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("terminate_connection, metadata is: %s.", metadata)
        self._terminate_connection(volume, connector)

        if metadata.get('hypermetro'):
            self._terminate_connection(volume, connector, False)

        LOG.info('terminate_connection success.')
        return

    def _terminate_connection(self, volume, connector, local=True):
        LOG.info('_terminate_connection, detach %(local)s volume.',
                 {'local': 'local' if local else 'remote'})

        # Determine use which client, local or remote.
        client = self.client if local else self.rmt_client

        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_WARN, local)

        lungroup_id = None
        if connector is None or 'host' not in connector:
            host_name, initiator_name = huawei_utils.get_iscsi_mapping_info(
                client, lun_id)
        else:
            initiator_name = connector.get('initiator')
            host_name = connector.get('host')

        LOG.info(
            'terminate_connection: initiator name: %(ini)s, '
            'LUN ID: %(lunid)s, lun type: %(lun_type)s, '
            'connector: %(connector)s.',
            {'ini': huawei_utils.mask_initiator_sensitive_info(initiator_name),
             'lunid': lun_id,
             'lun_type': lun_type,
             'connector': huawei_utils.mask_initiator_sensitive_info(
                 connector, sensitive_keys=constants.SENSITIVE_INI_KEYS)})

        portgroup_id = None
        view_id = None

        host_id = huawei_utils.get_host_id(client, host_name)
        if host_id:
            mapping_view_name = constants.MAPPING_VIEW_PREFIX + host_id
            view_id = client.find_mapping_view(mapping_view_name)
            if view_id:
                lungroup_id = client.find_lungroup_from_map(view_id)
                portgroup_id = client.get_portgroup_by_view(view_id)

        # Remove lun from lungroup.
        if lun_id and lungroup_id:
            lungroup_ids = client.get_lungroupids_by_lunid(lun_id, lun_type)
            if lungroup_id in lungroup_ids:
                client.remove_lun_from_lungroup(lungroup_id, lun_id, lun_type)
            else:
                LOG.warning("LUN is not in lungroup. "
                            "LUN ID: %(lun_id)s. "
                            "Lungroup id: %(lungroup_id)s.",
                            {"lun_id": lun_id, "lungroup_id": lungroup_id})
        if self.configuration.retain_storage_mapping:
            return
        # Remove portgroup from mapping view if no lun left in lungroup.
        mapping_param = {
            'lungroup_id': lungroup_id, 'view_id': view_id, 'portgroup_id': portgroup_id,
            'initiator_name': initiator_name, 'host_id': host_id
        }
        self._delete_storage_mapping(client, mapping_param)

    def _delete_storage_mapping(self, client, mapping_param):
        left_lun_num = -1
        lungroup_id = mapping_param.get('lungroup_id')
        view_id = mapping_param.get('view_id')
        portgroup_id = mapping_param.get('portgroup_id')
        initiator_name = mapping_param.get('initiator_name')
        host_id = mapping_param.get('host_id')
        if lungroup_id:
            left_lun_num = client.get_obj_count_from_lungroup(lungroup_id)

        if portgroup_id and view_id and (int(left_lun_num) <= 0):
            if client.is_portgroup_associated_to_view(view_id, portgroup_id):
                client.delete_portgroup_mapping_view(view_id, portgroup_id)
        if view_id and (int(left_lun_num) <= 0):
            client.remove_chap(initiator_name)

            if client.lungroup_associated(view_id, lungroup_id):
                client.delete_lungroup_mapping_view(view_id, lungroup_id)
            client.delete_lungroup(lungroup_id)
            if client.is_initiator_associated_to_host(initiator_name, host_id):
                client.remove_iscsi_from_host(initiator_name)
            hostgroup_name = constants.HOSTGROUP_PREFIX + host_id
            hostgroup_id = client.find_hostgroup(hostgroup_name)
            if hostgroup_id:
                if client.hostgroup_associated(view_id, hostgroup_id):
                    client.delete_hostgoup_mapping_view(view_id, hostgroup_id)
                client.remove_host_from_hostgroup(hostgroup_id, host_id)
                client.delete_hostgroup(hostgroup_id)
            client.remove_host(host_id)
            client.delete_mapping_view(view_id)


class HuaweiFCDriver(HuaweiBaseDriver, driver.FibreChannelDriver):
    """FC driver for Huawei OceanStor storage arrays.

    Version history:
        1.0.0 - Initial driver
        1.1.0 - Provide Huawei OceanStor 18000 storage volume driver
        1.1.1 - Code refactor
                Multiple pools support
                SmartX support
                Volume migration support
                Volume retype support
                FC zone enhancement
                Volume hypermetro support
        2.0.0 - Rename to HuaweiFCDriver
        2.0.1 - Manage/unmanage volume support
        2.0.2 - Refactor HuaweiFCDriver
        2.0.3 - Manage/unmanage snapshot support
        2.0.4 - Balanced FC port selection
        2.0.5 - Replication V2 support
        2.0.7 - Hypermetro support
                Hypermetro consistency group support
                Consistency group support
                Cgsnapshot support
        2.0.8 - Backup snapshot optimal path support
        2.0.9 - Support reporting disk type of pool
        2.2.RC1 - Add force delete volume
    """


    def __init__(self, *args, **kwargs):
        super(HuaweiFCDriver, self).__init__(*args, **kwargs)
        self.fcsan = None

    def get_volume_stats(self, refresh=False):
        """Get volume status."""
        data = HuaweiBaseDriver.get_volume_stats(self, refresh=False)
        backend_name = self.configuration.safe_get('volume_backend_name')
        data['volume_backend_name'] = backend_name or self.__class__.__name__
        data['storage_protocol'] = 'FC'
        data['driver_version'] = self.VERSION
        data['vendor_name'] = 'Huawei'
        return data

    def _check_fc_links(self, wwns, host_id):
        effective_wwns = []
        for wwn in wwns:
            wwn_info = self.client.get_fc_init_info(wwn)
            if not wwn_info:
                LOG.info("%s is not found in device, ignore it.", huawei_utils.mask_initiator_sensitive_info(wwn))
                continue

            if wwn_info.get('RUNNINGSTATUS') == constants.FC_INIT_ONLINE:
                if wwn_info.get('ISFREE') == 'true':
                    effective_wwns.append(wwn)
                    continue

                if wwn_info.get('PARENTTYPE') == constants.PARENT_TYPE_HOST \
                        and wwn_info.get('PARENTID') == host_id:
                    effective_wwns.append(wwn)
                    continue

            msg = (("Can't add FC initiator %(wwn)s to host "
                    "%(host)s, please check if this initiator has"
                    " been added to other host or isn't present "
                    "on array.")
                   % {"wwn": huawei_utils.mask_initiator_sensitive_info(wwn), "host": host_id})
            LOG.warning(msg)

            if (self.configuration.min_fc_ini_online ==
                    constants.DEFAULT_MINIMUM_FC_INITIATOR_ONLINE):
                wwns_in_host = (
                    self.client.get_host_fc_initiators(host_id))
                iqns_in_host = (
                    self.client.get_host_iscsi_initiators(host_id))
                if not (wwns_in_host or iqns_in_host or
                        self.client.is_host_associated_to_hostgroup(
                            host_id)):
                    self.client.remove_host(host_id)
                msg = ("There is an Fc initiator in an invalid "
                       "state. If you want to continue to attach "
                       "volume to host, configure MinFCIniOnline "
                       "in the XML file.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        return effective_wwns

    def initialize_connection(self, volume, connector):
        host = "" if connector is None else connector.get('host', "")
        lock_mapping = 'huawei-mapping-%s' % host

        @utils.synchronized(lock_mapping, external=True)
        @fczm_utils.AddFCZone
        def lock_host_when_initialize_connection(volume, connector):
            return self._initialize_connection_lock(volume, connector)

        return lock_host_when_initialize_connection(volume, connector)

    def _initialize_connection_lock(self, volume, connector):
        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_RAISE)
        lun_info = self.client.get_lun_info(lun_id, lun_type)

        conn_wwpns = huawei_utils.convert_connector_wwns(connector['wwpns'])
        wwns = conn_wwpns
        LOG.info(
            'initialize_connection, initiator: %(wwpns)s,'
            ' LUN ID: %(lun_id)s, lun type: %(lun_type)s,'
            ' connector: %(connector)s.',
            {'wwpns': huawei_utils.mask_initiator_sensitive_info(list(wwns)),
             'lun_id': lun_id,
             'lun_type': lun_type,
             'connector': huawei_utils.mask_initiator_sensitive_info(
                 connector, sensitive_keys=constants.SENSITIVE_INI_KEYS)})

        fc_info = self._initialize_connection(
            connector, volume, wwns, lun_info, lun_type)

        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("initialize_connection, metadata is: %s.", metadata)
        hypermetro_lun = metadata.get('hypermetro')

        # Deal with hypermetro connection.
        if hypermetro_lun:
            loc_tgt_wwn = fc_info.get(constants.DATA, {}).get(constants.TARGET_WWN)
            local_ini_tgt_map = fc_info.get(constants.DATA, {}).get(constants.INITIATOR_TARGET_MAP)
            hyperm = hypermetro.HuaweiHyperMetro(
                self.client, self.rmt_client, self.configuration)
            rmt_fc_info = hyperm.connect_volume_fc(volume, connector)
            rmt_tgt_wwn = rmt_fc_info[constants.DATA][constants.TARGET_WWN]
            fc_info.get(constants.DATA)[constants.TARGET_WWN] = (loc_tgt_wwn + rmt_tgt_wwn)
            rmt_ini_tgt_map = rmt_fc_info[constants.DATA][constants.INITIATOR_TARGET_MAP]
            for k in rmt_ini_tgt_map:
                local_ini_tgt_map[k] = (local_ini_tgt_map.get(k, []) + rmt_ini_tgt_map[k])
            loc_map_info = fc_info.get(constants.DATA, {}).get('map_info')
            rmt_map_info = rmt_fc_info[constants.DATA]['map_info']
            same_host_id = self._get_same_hostid(loc_map_info, rmt_map_info)
            self.client.change_hostlun_id(loc_map_info, same_host_id)
            hyperm.rmt_client.change_hostlun_id(rmt_map_info, same_host_id)
            fc_info.get(constants.DATA)['target_lun'] = same_host_id
        LOG.info("Return FC info is: %s.", huawei_utils.mask_initiator_sensitive_info(
            fc_info, sensitive_keys=[constants.INITIATOR_TARGET_MAP, 'target_wwn'] + wwns, need_mask_keys=wwns))
        return fc_info

    def _initialize_connection(self, connector, volume, wwns, lun_info, lun_type):
        host_id = self.client.add_host_with_check(
            connector['host'], self.is_dorado_v6, wwns)
        (tgt_port_wwns, portg_id, init_targ_map) = self._get_target_info(
            connector, wwns, host_id)
        hostgroup_id = self.client.add_host_to_hostgroup(host_id)
        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("initialize_connection, metadata is: %s.", metadata)
        hypermetro_lun = metadata.get('hypermetro')
        map_info = self.client.do_mapping(lun_info, hostgroup_id, host_id, portg_id,
                                          lun_type, hypermetro_lun)
        host_lun_id = self.client.get_host_lun_id(host_id, lun_info, lun_type)

        # Return FC properties.
        fc_info = {
            'driver_volume_type': 'fibre_channel',
            'data': {
                'target_lun': int(host_lun_id),
                'target_discovered': True,
                'target_wwn': huawei_utils.mask_initiator_sensitive_info(tgt_port_wwns),
                'volume_id': volume.id,
                'initiator_target_map': init_targ_map,
                'map_info': map_info,
                'lun_wwn': lun_info['WWN'],
                'libvirt_iscsi_use_ultrapath': self.use_ultrapath
            },
        }
        if lun_type == constants.LUN_TYPE and \
                int(lun_info.get('ALLOCTYPE')) == constants.THIN_LUNTYPE:
            fc_info['data']['discard'] = True
        return fc_info

    def _get_target_info(self, connector, wwns, host_id):
        portg_id = None
        if not self.fcsan:
            self.fcsan = fczm_utils.create_lookup_service()

        if self.fcsan:
            # Use FC switch.
            zone_helper = fc_zone_helper.FCZoneHelper(self.fcsan, self.client)
            try:
                (tgt_port_wwns, portg_id, init_targ_map) = (
                    zone_helper.build_ini_targ_map(wwns, host_id))
            except Exception as err:
                self.remove_host_with_check(host_id)
                msg = _('build_ini_targ_map fails. %s') % err
                raise exception.VolumeBackendAPIException(data=msg)

            for ini in init_targ_map:
                self.client.ensure_fc_initiator_added(ini, host_id,
                                                      connector['host'])
        else:
            # Not use FC switch.
            wwns = self._check_fc_links(wwns, host_id)
            LOG.info("initialize_connection, "
                     "effective initiators on the array: %s.", huawei_utils.mask_initiator_sensitive_info(wwns))

            if len(wwns) < self.configuration.min_fc_ini_online:
                msg = (("The number of online fc initiator %(wwns)s less than"
                        " the set number: %(set)s.") % {
                           "wwns": huawei_utils.mask_initiator_sensitive_info(wwns),
                           "set": self.configuration.min_fc_ini_online})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for wwn in wwns:
                self.client.ensure_fc_initiator_added(wwn, host_id, connector['host'])

            tgt_port_wwns, init_targ_map = self.client.get_init_targ_map(wwns)

        return tgt_port_wwns, portg_id, init_targ_map

    def terminate_connection(self, volume, connector, **kwargs):
        """Delete map between a volume and a host."""
        host = "" if connector is None else connector.get('host', "")
        lock_mapping = 'huawei-mapping-%s' % host

        @utils.synchronized(lock_mapping, external=True)
        @fczm_utils.RemoveFCZone
        def lock_host_when_terminate_connection(volume, connector):
            return self._terminate_connection_locked(volume, connector, **kwargs)

        return lock_host_when_terminate_connection(volume, connector)

    def _terminate_connection_locked(self, volume, connector, **kwargs):
        """Delete map between a volume and a host."""
        attachments = volume.volume_attachment
        if volume.multiattach and len(attachments) > 1 and sum(
                1 for a in attachments if a.connector == connector) > 1:
            LOG.info("Volume is multi-attach and attached to the same host"
                     " multiple times")
            return {}

        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_WARN)

        if connector is None or 'host' not in connector:
            host_name, wwns = huawei_utils.get_fc_mapping_info(self.client, lun_id)
        else:
            host_name = connector.get('host')
            conn_wwpns = huawei_utils.convert_connector_wwns(
                connector.get('wwpns'))
            wwns = conn_wwpns
        left_lunnum = -1
        lungroup_id = None
        view_id = None
        LOG.info('terminate_connection: wwpns: %(wwns)s, '
                 'LUN ID: %(lun_id)s, lun type: %(lun_type)s, '
                 'connector: %(connector)s.',
                 {'wwns': huawei_utils.mask_initiator_sensitive_info(wwns),
                  'lun_id': lun_id, 'lun_type': lun_type,
                  'connector': huawei_utils.mask_initiator_sensitive_info(
                      connector, sensitive_keys=constants.SENSITIVE_INI_KEYS)})

        host_id = huawei_utils.get_host_id(self.client, host_name)
        if host_id:
            mapping_view_name = constants.MAPPING_VIEW_PREFIX + host_id
            view_id = self.client.find_mapping_view(mapping_view_name)
            if view_id:
                lungroup_id = self.client.find_lungroup_from_map(view_id)

        if lun_id and lungroup_id:
            lungroup_ids = self.client.get_lungroupids_by_lunid(lun_id,
                                                                lun_type)
            if lungroup_id in lungroup_ids:
                self.client.remove_lun_from_lungroup(lungroup_id,
                                                     lun_id,
                                                     lun_type)
            else:
                LOG.warning("LUN is not in lungroup. "
                            "LUN ID: %(lun_id)s. "
                            "Lungroup id: %(lungroup_id)s.",
                            {"lun_id": lun_id,
                             "lungroup_id": lungroup_id})

        else:
            LOG.warning("Can't find lun on the array.")
        if lungroup_id:
            left_lunnum = self.client.get_obj_count_from_lungroup(lungroup_id)
        if int(left_lunnum) > 0 or self.configuration.retain_storage_mapping:
            fc_info = {'driver_volume_type': 'fibre_channel', 'data': {}}
        else:
            fc_info, portg_id = self._delete_zone_and_remove_fc_initiators(wwns, host_id)
            if lungroup_id:
                if view_id and self.client.lungroup_associated(
                        view_id, lungroup_id):
                    self.client.delete_lungroup_mapping_view(view_id,
                                                             lungroup_id)
                self.client.delete_lungroup(lungroup_id)
            if portg_id:
                if view_id and self.client.is_portgroup_associated_to_view(
                        view_id, portg_id):
                    self.client.delete_portgroup_mapping_view(view_id,
                                                              portg_id)
                    self.client.delete_portgroup(portg_id)

            if host_id:
                hostgroup_name = constants.HOSTGROUP_PREFIX + host_id
                hostgroup_id = self.client.find_hostgroup(hostgroup_name)
                if hostgroup_id:
                    if view_id and self.client.hostgroup_associated(
                            view_id, hostgroup_id):
                        self.client.delete_hostgoup_mapping_view(
                            view_id, hostgroup_id)
                    self.client.remove_host_from_hostgroup(
                        hostgroup_id, host_id)
                    self.client.delete_hostgroup(hostgroup_id)

                if not self.client.check_fc_initiators_exist_in_host(
                        host_id):
                    self.client.remove_host(host_id)

            if view_id:
                self.client.delete_mapping_view(view_id)

        # Deal with hypermetro connection.
        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("Detach Volume, metadata is: %s.", metadata)

        if metadata.get('hypermetro'):
            hyperm = hypermetro.HuaweiHyperMetro(
                self.client, self.rmt_client, self.configuration)
            hyperm.disconnect_volume_fc(volume, connector)

        LOG.info("terminate_connection, return data is: %s.", huawei_utils.mask_initiator_sensitive_info(
            fc_info, sensitive_keys=['initiator_target_map', 'target_wwn'], need_mask_keys=wwns))

        return fc_info

    def _delete_zone_and_remove_fc_initiators(self, wwns, host_id):
        # Get tgt_port_wwns and init_targ_map to remove zone.
        portg_id = None
        tgt_port_wwns = []
        init_targ_map = {}
        if not self.fcsan:
            self.fcsan = fczm_utils.create_lookup_service()
        if self.fcsan:
            zone_helper = fc_zone_helper.FCZoneHelper(self.fcsan,
                                                      self.client)
            (tgt_port_wwns, portg_id, init_targ_map) = (
                zone_helper.get_init_targ_map(wwns, host_id))

        # Remove the initiators from host if need.
        if host_id:
            fc_initiators = self.client.get_host_fc_initiators(host_id)
            for wwn in wwns:
                if wwn in fc_initiators:
                    self.client.remove_fc_from_host(wwn)

        info = {
            'driver_volume_type': 'fibre_channel',
            'data': {
                'target_wwn': tgt_port_wwns,
                'initiator_target_map': init_targ_map
            }
        }
        return info, portg_id


class HuaweiROCEDriver(HuaweiBaseDriver):
    """RoCE driver for Huawei storage arrays.

    Version history:
        2.6.4 - start to support RoCE.
    """

    def __init__(self, *args, **kwargs):
        super(HuaweiROCEDriver, self).__init__(*args, **kwargs)

    def get_volume_stats(self, refresh=False):
        """Get volume status."""
        data = HuaweiBaseDriver.get_volume_stats(self, refresh=False)
        backend_name = self.configuration.safe_get('volume_backend_name')
        data['volume_backend_name'] = backend_name or self.__class__.__name__
        data['storage_protocol'] = 'nvmeof'
        data['driver_version'] = self.VERSION
        data['vendor_name'] = 'Huawei'
        return data

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        """Map a volume to a host and return target RoCE information."""
        self._check_roce_params(volume, connector)

        # Attach local lun.
        roce_info = self._initialize_connection(volume, connector)

        # Attach remote lun if exists.
        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("Attach Volume, metadata is: %s.", metadata)
        if metadata.get('hypermetro'):
            try:
                rmt_roce_info = (
                    self._initialize_connection(volume, connector, False))
            except Exception:
                with excutils.save_and_reraise_exception():
                    self._terminate_connection(volume, connector)

            roce_info.get(constants.DATA).get('target_portals').extend(
                rmt_roce_info.get(constants.DATA).get('target_portals'))
            roce_info.get(constants.DATA).get('target_luns').extend(
                rmt_roce_info.get(constants.DATA).get('target_luns'))

        LOG.info('initialize_common_connection_roce, return data is: %s.',
                 huawei_utils.mask_initiator_sensitive_info(roce_info, sensitive_keys=['host_nqn']))
        return roce_info

    def _initialize_connection(self, volume, connector, local=True):
        LOG.info('Initialize RoCE connection for volume %(id)s, '
                 'connector info %(conn)s. array is in %(location)s.',
                 {'id': volume.id,
                  'conn': huawei_utils.mask_initiator_sensitive_info(
                      connector, sensitive_keys=constants.SENSITIVE_INI_KEYS),
                  'location': 'local' if local else 'remote'})

        host_nqn = connector.get("host_nqn")

        client = self.client if local else self.rmt_client

        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_RAISE, local)
        lun_info = client.get_lun_info(lun_id, lun_type)

        target_ips = client.get_roce_params(connector)

        host_id = client.add_host_with_check(
            connector.get('host'), self.is_dorado_v6, host_nqn)

        try:
            client.ensure_roceini_added(host_nqn, host_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.remove_host_with_check(host_id)

        hostgroup_id = client.add_host_to_hostgroup(host_id)

        metadata = huawei_utils.get_lun_metadata(volume)
        hypermetro_lun = metadata.get('hypermetro')

        map_info = client.do_mapping(
            lun_info, hostgroup_id, host_id,
            lun_type=lun_type, hypermetro_lun=hypermetro_lun)
        host_lun_id = client.get_host_lun_id(host_id, lun_info, lun_type)
        LOG.info('initialize_connection, host lun id is: %(id)s. '
                 'View info is %(view)s.',
                 {'id': host_lun_id, 'view': map_info})
        host_lun_id = int(host_lun_id)
        mapping_info = {
            'target_portals': ['%s:4420' % ip for ip in target_ips],
            'target_luns': [host_lun_id] * len(target_ips),
            'transport_type': 'rdma',
            'host_nqn': host_nqn,
            'discard': True,
            'volume_nguid': lun_info.get("NGUID")
        }
        conn = {
            'driver_volume_type': 'nvmeof',
            'data': mapping_info
        }
        LOG.info('Initialize RoCE connection successfully: %s.',
                 huawei_utils.mask_initiator_sensitive_info(conn, sensitive_keys=constants.SENSITIVE_INI_KEYS))
        return conn

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def terminate_connection(self, volume, connector, **kwargs):
        """Delete map between a volume and a host."""
        self._check_roce_params(volume, connector)

        metadata = huawei_utils.get_lun_metadata(volume)
        LOG.info("terminate_connection, metadata is: %s.", metadata)
        self._terminate_connection(volume, connector)

        if metadata.get('hypermetro'):
            self._terminate_connection(volume, connector, False)

        LOG.info('terminate_connection success.')

    def _terminate_connection(self, volume, connector, local=True):
        LOG.info('_terminate_connection, detach %(local)s volume.',
                 {'local': 'local' if local else 'remote'})

        client = self.client if local else self.rmt_client

        lun_id, lun_type = self.get_lun_id_and_type(
            volume, constants.VOLUME_NOT_EXISTS_WARN, local)

        initiator_name = connector.get('host_nqn')
        host_name = connector.get('host')

        LOG.info('terminate_connection: initiator name: %(ini)s, LUN ID: %('
                 'lunid)s, lun type: %(lun_type)s, connector: %('
                 'connector)s.', {'ini': huawei_utils.mask_initiator_sensitive_info(initiator_name),
                                  'lunid': lun_id,
                                  'lun_type': lun_type,
                                  'connector': huawei_utils.mask_initiator_sensitive_info(
                                      connector, sensitive_keys=constants.SENSITIVE_INI_KEYS)})

        lungroup_id = None
        portgroup_id = None
        view_id = None

        host_id = huawei_utils.get_host_id(client, host_name)
        if host_id:
            mapping_view_name = constants.MAPPING_VIEW_PREFIX + host_id
            view_id = client.find_mapping_view(mapping_view_name)
            if view_id:
                lungroup_id = client.find_lungroup_from_map(view_id)
                portgroup_id = client.get_portgroup_by_view(view_id)

        if lun_id and lungroup_id:
            lungroup_ids = client.get_lungroupids_by_lunid(lun_id, lun_type)
            if lungroup_id in lungroup_ids:
                client.remove_lun_from_lungroup(lungroup_id, lun_id, lun_type)
            else:
                LOG.warning("LUN is not in lungroup. LUN ID: %(lun_id)s. "
                            "Lungroup id: %(lungroup_id)s.",
                            {"lun_id": lun_id, "lungroup_id": lungroup_id})
        if self.configuration.retain_storage_mapping:
            return

        mapping_param = {
            'host_id': host_id, 'initiator_name': initiator_name,
            'lungroup_id': lungroup_id, 'view_id': view_id,
            'portgroup_id': portgroup_id
        }
        self._delete_storage_mapping(client, mapping_param)

    def _delete_storage_mapping(self, client, mapping_param):
        left_lun_num = -1
        lungroup_id = mapping_param.get('lungroup_id')
        view_id = mapping_param.get('view_id')
        portgroup_id = mapping_param.get('portgroup_id')
        initiator_name = mapping_param.get('initiator_name')
        host_id = mapping_param.get('host_id')
        if lungroup_id:
            left_lun_num = client.get_obj_count_from_lungroup(lungroup_id)
        if view_id and (int(left_lun_num) <= 0):
            if portgroup_id and client.is_portgroup_associated_to_view(
                    view_id, portgroup_id):
                client.delete_portgroup_mapping_view(view_id, portgroup_id)

            if client.lungroup_associated(view_id, lungroup_id):
                client.delete_lungroup_mapping_view(view_id, lungroup_id)

            client.delete_lungroup(lungroup_id)

            if client.is_roce_initiator_associated_to_host(
                    initiator_name, host_id):
                client.remove_roce_initiator_from_host(initiator_name, host_id)

            hostgroup_name = constants.HOSTGROUP_PREFIX + host_id
            hostgroup_id = client.find_hostgroup(hostgroup_name)
            if hostgroup_id:
                if client.hostgroup_associated(view_id, hostgroup_id):
                    client.delete_hostgoup_mapping_view(view_id, hostgroup_id)
                client.remove_host_from_hostgroup(hostgroup_id, host_id)
                client.delete_hostgroup(hostgroup_id)
            client.remove_host(host_id)

            client.delete_mapping_view(view_id)

    def _check_roce_params(self, volume, connector):
        if not volume or not connector:
            msg = _(
                '%(param)s is none.'
                % {'param': 'volume' if not volume else 'connector'})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not volume.id:
            msg = _(
                'volume param is error. volume is %(volume)s.'
                % {'volume': volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not connector.get('host_nqn') or not connector.get('host'):
            msg = _(
                'connector param is error. connector is %(connector)s.'
                % {'connector': huawei_utils.mask_initiator_sensitive_info(
                    connector, sensitive_keys=['host_nqn'])})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not self.is_dorado_v6:
            msg = _("Current storage doesn't support RoCE.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
