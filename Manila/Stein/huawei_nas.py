# Copyright (c) 2014 Huawei Technologies Co., Ltd.
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

import ipaddress
import json
import math
import os
import re
import tempfile

from oslo_config import cfg
from oslo_config import types
from oslo_log import log
import oslo_messaging as messaging
from oslo_utils import strutils
from oslo_utils import units

from manila.common import constants as common_constants
from manila import context as manila_context
from manila.data import utils as data_utils
from manila import exception
from manila.i18n import _
from manila import rpc
from manila.share import driver
from manila.share import utils as share_utils
from manila import utils

from manila.share.drivers.huawei import constants
from manila.share.drivers.huawei import helper
from manila.share.drivers.huawei import huawei_config
from manila.share.drivers.huawei import huawei_utils
from manila.share.drivers.huawei import hypermetro
from manila.share.drivers.huawei import manager
from manila.share.drivers.huawei import replication
from manila.share.drivers.huawei import rpcapi
from manila.share.drivers.huawei import smartx

huawei_opts = [
    cfg.StrOpt('manila_huawei_conf_file',
               default='/etc/manila/manila_huawei_conf.xml',
               help='The configuration file for the Manila Huawei driver.'),
    cfg.BoolOpt('local_replication',
                default=False,
                help='The replication type of backend Huawei storage.'),
    cfg.MultiOpt('metro_info',
                 item_type=types.Dict(),
                 secret=True,
                 help='Multi opt of dictionaries to represent a hypermetro '
                      'target device. This option may be specified multiple '
                      'times in a single config section to specify multiple '
                      'hypermetro target devices. Each entry takes the '
                      'standard dict config form: hypermetro_device = '
                      'key1:value1,key2:value2...'),
]

CONF = cfg.CONF
CONF.register_opts(huawei_opts)
LOG = log.getLogger(__name__)


class HuaweiNasDriver(driver.ShareDriver):
    def __init__(self, *args, **kwargs):
        super(HuaweiNasDriver, self).__init__((True, False), *args, **kwargs)
        self.configuration.append_config_values(huawei_opts)
        self.huawei_config = huawei_config.HuaweiConfig(self.configuration)

        self.helper = helper.RestHelper(
            self.configuration.nas_address, self.configuration.nas_user,
            self.configuration.nas_password)
        self.metro_domain = None
        self.remote_backend = None
        self.vstore_pair_id = None

        self.replica_mgr = replication.ReplicaPairManager(self.helper)
        self.metro_mgr = hypermetro.HyperPairManager(self.helper,
                                                     self.configuration)
        self.smart_qos = smartx.SmartQos(self.helper)
        self.smart_partition = smartx.SmartPartition(self.helper)
        self.smart_cache = smartx.SmartCache(self.helper)
        self.rpc_client = rpcapi.HuaweiAPI()
        self.feature_supports = {}

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""
        self._check_config()
        self._check_storage_pools()
        self._get_metro_info()

    def do_setup(self, context):
        self.helper.login()
        rpc_manager = manager.HuaweiManager(self, self.replica_mgr,
                                            self.metro_mgr)
        self._setup_rpc_server(rpc_manager.RPC_API_VERSION, [rpc_manager])

    def _setup_rpc_server(self, server_version, endpoints):
        host = "%s@%s" % (CONF.host, self.configuration.config_group)
        target = messaging.Target(topic=self.rpc_client.topic, server=host,
                                  version=server_version)
        self.rpc_server = rpc.get_server(target, endpoints)
        self.rpc_server.start()

    def _check_storage_pools(self):
        s_pools = []
        pools = self.helper.get_all_pools()
        for pool in pools:
            if pool.get('USAGETYPE') in (constants.FILE_SYSTEM_POOL_TYPE,
                                         constants.DORADO_V6_POOL_TYPE) or \
                    pool.get('NEWUSAGETYPE') in \
                    (constants.FILE_SYSTEM_POOL_TYPE,
                     constants.DORADO_V6_POOL_TYPE):
                s_pools.append(pool['NAME'])

        for pool_name in self.configuration.storage_pools:
            if pool_name not in s_pools:
                msg = _("Storage pool %s not exist.") % pool_name
                LOG.error(msg)
                raise exception.BadConfigurationException(reason=msg)

    def _check_config(self):
        if (not self.configuration.driver_handles_share_servers and
                not self.configuration.logical_ip and
                not self.configuration.dns):
            msg = _('driver_handles_share_servers or LogicalPortIP '
                    'or DNS must be set at least one.')
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        if self.configuration.snapshot_support \
                and self.configuration.replication_support:
            msg = _('SnapshotSupport and ReplicationSupport cannot both '
                    'be set to True.')
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

    def _get_metro_info(self):
        metro_infos = self.huawei_config.get_metro_info()
        metro_info = metro_infos[0] if metro_infos else {}
        if metro_info:
            self.metro_domain = metro_info.get("metro_domain")
            self.remote_backend = metro_info.get("remote_backend")
            local_vstore_name = metro_info.get("local_vStore_name")
            remote_vstore_name = metro_info.get("remote_vStore_name")
            self.metro_logic_ip = metro_info.get('metro_logic_ip')

            self.vstore_pair_id = huawei_utils.get_hypermetro_vstore_id(
                self.helper, self.metro_domain, local_vstore_name,
                remote_vstore_name)
            # check_remote_metro_info
            context = manila_context.get_admin_context()
            self.rpc_client.check_remote_metro_info(
                context, self.remote_backend, self.metro_domain,
                local_vstore_name, remote_vstore_name,
                self.vstore_pair_id)

    def _create_filesystem(self, share, pool_name,
                           share_fs_id=None, snapshot_id=None, context=None):
        opts = huawei_utils.get_share_extra_specs_params(
            share['share_type_id'])

        if ('LUNType' in opts and
                opts['LUNType'] == constants.ALLOC_TYPE_THICK_FLAG):
            if opts['dedupe'] or opts['compression']:
                msg = _('Thick filesystem cannot use dedupe or compression.')
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        if not (share_fs_id and snapshot_id):
            pool_info = self.helper.get_pool_by_name(pool_name)
            if not pool_info:
                msg = _("Pool %s to create FS not exist.") % pool_name
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

            params = {
                "NAME": huawei_utils.share_name(share['name']),
                "ALLOCTYPE": opts.get('LUNType',
                                      constants.ALLOC_TYPE_THIN_FLAG),
                "CAPACITY": huawei_utils.share_size(share['size']),
                "PARENTID": pool_info['ID'],
                "ENABLEDEDUP": opts['dedupe'],
                "ENABLECOMPRESSION": opts['compression'],
                "SNAPSHOTRESERVEPER": self.configuration.snapshot_reserve
            }
        else:
            params = {
                "NAME": huawei_utils.share_name(share['name']),
                "ALLOCTYPE": opts.get('LUNType',
                                      constants.ALLOC_TYPE_THIN_FLAG),
                "PARENTFILESYSTEMID": share_fs_id,
                "PARENTSNAPSHOTID": snapshot_id,
                "SNAPSHOTRESERVEPER": self.configuration.snapshot_reserve
            }
        if opts.get('sectorsize'):
            params["SECTORSIZE"] = int(opts['sectorsize']) * units.Ki
        elif hasattr(self.configuration, 'sector_size'):
            params["SECTORSIZE"] = int(self.configuration.sector_size
                                       ) * units.Ki

        if opts.get('controllername'):
            controller = self.helper.get_controller_id(opts['controllername'])
            if controller:
                params['OWNINGCONTROLLER'] = controller

        remote_vstore_id = None
        if opts.get('hypermetro'):
            vstore_info = self.helper.get_hypermetro_vstore_by_pair_id(
                self.vstore_pair_id)
            local_vstore_id = vstore_info.get('LOCALVSTOREID')
            remote_vstore_id = vstore_info.get('REMOTEVSTOREID')
            if local_vstore_id and remote_vstore_id:
                params['vstoreId'] = local_vstore_id

        fs_id = self.helper.create_filesystem(params)
        huawei_utils.wait_fs_online(
            self.helper, fs_id, self.configuration.wait_interval,
            self.configuration.timeout)

        try:
            if opts['qos']:
                self.smart_qos.add(opts['qos'], fs_id)
            if opts['huawei_smartpartition']:
                self.smart_partition.add(opts['partitionname'], fs_id)
            if opts['huawei_smartcache']:
                self.smart_cache.add(opts['cachename'], fs_id)
        except Exception:
            self._delete_filesystem(fs_id)
            LOG.exception('Failed to add smartx to filesystem %s.', fs_id)
            raise

        if opts.get('hypermetro'):
            try:
                params.update({'vstoreId': remote_vstore_id})
                remote_fs_id = self.rpc_client.create_remote_filesystem(
                    context, self.remote_backend, params)
            except Exception as err:
                self._delete_filesystem(fs_id)
                LOG.exception('Failed to create remote filesystem.'
                              ' reason: %s', err)
                raise

            try:
                self.metro_mgr.create_metro_pair(
                    self.metro_domain, fs_id, remote_fs_id,
                    self.vstore_pair_id)
            except Exception as err:
                self._delete_filesystem(fs_id, metro=False)
                params = {"ID": remote_fs_id}
                self.rpc_client.delete_remote_filesystem(
                    context, self.remote_backend, params)
                LOG.exception('Failed to create HyperMetro filesystem pair'
                              '%(fs_id)s. reason: %(err)s'
                              % {"fs_id": fs_id, "err": err})
                raise

        return fs_id

    def _get_remote_fs_id(self, fs_id, metro_info):
        if fs_id == metro_info['LOCALOBJID']:
            remote_fs_id = metro_info['REMOTEOBJID']
        elif fs_id == metro_info['REMOTEOBJID']:
            remote_fs_id = metro_info['LOCALOBJID']
        else:
            msg = (_("Filesystem %s is not belong to a HyperMetro "
                     "filesystem.") % fs_id)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        return remote_fs_id

    def _get_metro_id_from_fs_info(self, fs_info):
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            metro_id = json.loads(fs_info.get('HYPERMETROPAIRIDS'))
            if not metro_id:
                msg = _("Filesystem is a HyperMetro, but failed to get the "
                        "metro id")
                LOG.error(msg)
                raise exception.ShareResourceNotFound(reason=msg)
            metro_id = metro_id[0]
            return metro_id

    def _delete_metro_filesystem(self, context, fs_id, fs_info):
        metro_id = self._get_metro_id_from_fs_info(fs_info)
        if not metro_id:
            return
        metro_info = self.helper.get_hypermetro_pair_by_id(metro_id)
        remote_fs_id = self._get_remote_fs_id(fs_id, metro_info)

        try:
            self.metro_mgr.delete_metro_pair(metro_id=metro_id)
        except Exception as err:
            msg = (_("Failed to delete HyperMetro filesystem pair "
                     "%(metro_id)s. Reason: %(err)s")
                   % {"metro_id": metro_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        vstore_info = self.helper.get_hypermetro_vstore_by_pair_id(
            self.vstore_pair_id)
        try:
            remote_vstore_id = vstore_info.get('REMOTEVSTOREID')
            if remote_vstore_id:
                params = {"ID": remote_fs_id, 'vstoreId': remote_vstore_id}
            else:
                params = {"ID": remote_fs_id}
            self.rpc_client.delete_remote_filesystem(
                context, self.remote_backend, params)
        except Exception as err:
            msg = (_("Failed to delete remote filesystem %(fs_id)s. "
                     "Reason: %(err)s") % {"fs_id": remote_fs_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        try:
            local_vstore_id = vstore_info.get('LOCALVSTOREID')
            if local_vstore_id:
                params = {"ID": fs_id, 'vstoreId': local_vstore_id}
            else:
                params = {"ID": fs_id}
            self.helper.delete_filesystem(params)
        except Exception as err:
            msg = (_("Failed to delete local filesystem %(fs_id)s. "
                     "Reason: %(err)s") % {"fs_id": fs_id, "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

    def _delete_filesystem(self, fs_id, context=None, metro=True):
        fs_info = self.helper.get_fs_info_by_id(fs_id)
        if fs_info['IOCLASSID']:
            self.smart_qos.remove(fs_id, fs_info['IOCLASSID'])
        if fs_info['CACHEPARTITIONID']:
            self.smart_partition.remove(fs_id, fs_info['CACHEPARTITIONID'])
        if fs_info['SMARTCACHEPARTITIONID']:
            self.smart_cache.remove(fs_id, fs_info['SMARTCACHEPARTITIONID'])

        if json.loads(fs_info.get('HYPERMETROPAIRIDS')) and metro:
            self._delete_metro_filesystem(context, fs_id, fs_info)
            return

        params = {"ID": fs_id}
        self.helper.delete_filesystem(params)

    def _create_share(self, share, fs_id):
        share_name = share['name']
        share_proto = share['share_proto']

        fs_info = self.helper.get_fs_info_by_id(fs_id)
        vstore_id = fs_info.get('vstoreId')

        try:
            self.helper.create_share(share_name, fs_id, share_proto,
                                     vstore_id)
        except Exception:
            LOG.exception('Failed to create %(proto)s share for FS %(fs)s.',
                          {'proto': share_proto, 'fs': fs_id})
            raise

    def _get_export_location(self, share_name, share_proto,
                             share_server):
        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.warning('FS %s to delete not exist.', share_name)
            return
        elif json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            metro = True
        else:
            metro = False

        if share_server:
            if metro:
                vstore_id = fs_info.get('vstoreId')
                self.helper.modify_logical_port(
                    share_server['backend_details']['logical_port_id'],
                    vstore_id)
            ips = [share_server['backend_details']['ip']]
        else:
            if not metro:
                ips = self.configuration.logical_ip
            else:
                ips = [self.metro_logic_ip]

            dnses = self.configuration.dns
            if dnses:
                ips = dnses

        path_name = huawei_utils.share_name(share_name)
        if share_proto == 'NFS':
            locations = ['%s:/%s' % (ip, path_name) for ip in ips]
        elif share_proto == 'CIFS':
            share_info = self.helper.get_share_by_name(
                share_name, share_proto, fs_info.get('vstoreId'))
            path_name = huawei_utils.share_name(share_info.get('NAME'))
            locations = [r'\\%s\%s' % (ip, path_name) for ip in ips]
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        return locations

    def create_share(self, context, share, share_server=None):
        pool_name = share_utils.extract_host(share['host'], level='pool')
        if not pool_name:
            msg = _("Pool is not available in host %s.") % share['host']
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        fs_id = self._create_filesystem(share, pool_name, context=context)
        self._create_share(share, fs_id)
        return self._get_export_location(
            share['name'], share['share_proto'], share_server)

    def rpc_delete_share(self, context, share_name, share_proto):
        fs_info = self.helper.get_fs_info_by_name(share_name)
        vstore_id = fs_info.get('vstoreId')
        share_info = self.helper.get_share_by_name(
            share_name, share_proto, vstore_id)
        if share_info:
            self.helper.delete_share(
                share_info['ID'], share_proto, vstore_id)
        self._delete_filesystem(fs_info['ID'], context)

    def delete_share(self, context, share, share_server=None):
        share_name = share['name']
        share_proto = share['share_proto']
        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.warning('FS %s to delete not exist.', share_name)
            return
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            self.rpc_client.get_remote_fs_info(
                context, share_name, self.remote_backend)
            if self._check_is_active_client():
                self.rpc_delete_share(context, share_name, share_proto)
            else:
                self.rpc_client.delete_share(
                    context, share_name, share_proto, self.remote_backend)
        else:
            self.rpc_delete_share(context, share_name, share_proto)

    def _update_filesystem(self, fs_info, params):
        fs_id = fs_info.get('ID')
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            metro_id = self._get_metro_id_from_fs_info(fs_info)
            metro_info = self.helper.get_hypermetro_pair_by_id(metro_id)
            remote_fs_id = self._get_remote_fs_id(fs_id, metro_info)

            try:
                context = manila_context.get_admin_context()
                self.rpc_client.update_filesystem(context, self.remote_backend,
                                                  remote_fs_id, params)
            except Exception as err:
                msg = (_("Failed to update remote filesystem %(fs_id)s. "
                         "Reason: %(err)s") %
                       {"fs_id": remote_fs_id, "err": err})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        self.helper.update_filesystem(fs_id, params)

    def extend_share(self, share, new_size, share_server):
        share_name = share['name']
        share_proto = share['share_proto']
        fs_info = self.helper.get_fs_info_by_name(share_name)
        share_info = self.helper.get_share_by_name(
            share_name, share_proto, fs_info.get('vstoreId'))
        if not share_info:
            msg = _("share %s does not exist.") % share_name
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        fs_info = self.helper.get_fs_info_by_name(share_name)
        size = new_size * constants.CAPACITY_UNIT
        params = {"CAPACITY": size}
        self._update_filesystem(fs_info, params)

    def shrink_share(self, share, new_size, share_server):
        share_name = share['name']
        share_proto = share['share_proto']
        fs_info = self.helper.get_fs_info_by_name(share_name)
        share_info = self.helper.get_share_by_name(
            share_name, share_proto, fs_info.get('vstoreId'))
        if not share_info:
            msg = _("share %s does not exist.") % share_name
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        fs_info = self.helper.get_fs_info_by_name(share_name)
        fs_id = fs_info['ID']
        size = new_size * constants.CAPACITY_UNIT
        used_size = int(fs_info['MINSIZEFSCAPACITY'])
        if used_size > size:
            LOG.error('FS %(id)s already uses %(used)d capacity. '
                      'Cannot shrink to %(newsize)d.',
                      {'id': fs_id, 'used': used_size, 'newsize': size})
            raise exception.ShareShrinkingPossibleDataLoss(
                share_id=share['id'])

        params = {"CAPACITY": size}
        self._update_filesystem(fs_info, params)

    def create_snapshot(self, context, snapshot, share_server=None):
        fs_info = self.helper.get_fs_info_by_name(snapshot['share_name'])
        if not fs_info:
            msg = _("FS %s not exist.") % snapshot['share_name']
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        snapshot_id = self.helper.create_snapshot(
            fs_info['ID'], snapshot['name'])
        return {'provider_location': snapshot_id}

    def delete_snapshot(self, context, snapshot, share_server=None):
        provider_location = snapshot.get('provider_location')
        if provider_location and '@' in provider_location:
            snapshot_id = provider_location
        else:
            fs_info = self.helper.get_fs_info_by_name(
                snapshot['share_name'])
            if not fs_info:
                LOG.warning('Parent FS of snapshot %s to delete not exist.',
                            snapshot['id'])
                return
            snapshot_id = huawei_utils.snapshot_id(
                fs_info['ID'], snapshot['name'])
        self.helper.delete_snapshot(snapshot_id)

    def _update_storage_supports(self):
        feature_status = self.helper.get_feature_status()

        for f in ('SmartThin', 'SmartQoS', 'SmartPartition', 'SmartCache',
                  'HyperMetro', 'HyperReplication', 'HyperSnap'):
            self.feature_supports[f] = (feature_status.get(f) in
                                        constants.AVAILABLE_FEATURE_STATUS)

        self.feature_supports['SmartDedup'] = False
        self.feature_supports['SmartCompression'] = False

        for f in feature_status:
            if re.match('SmartDedup[\s\S]*FS', f):
                self.feature_supports['SmartDedup'] = (
                    feature_status[f] in constants.AVAILABLE_FEATURE_STATUS)
            if re.match('SmartCompression[\s\S]*FS', f):
                self.feature_supports['SmartCompression'] = (
                    feature_status[f] in constants.AVAILABLE_FEATURE_STATUS)

        LOG.info('Update feature support: %s.', self.feature_supports)

    def _update_share_stats(self):
        self.huawei_config.update_configs()
        self._update_storage_supports()

        backend_name = self.configuration.safe_get('share_backend_name')
        data = {
            'share_backend_name': backend_name or 'HUAWEI_NAS_Driver',
            'vendor_name': 'Huawei',
            'driver_version': '2.2.RC1',
            'storage_protocol': 'NFS_CIFS',
            'snapshot_support': (self.feature_supports['HyperSnap']
                                 and self.configuration.snapshot_support),
        }

        data['revert_to_snapshot_support'] = data['snapshot_support']

        # Huawei storage doesn't support snapshot replication, so driver can't
        # create replicated snapshot, this's not fit the requirement of Manila
        # replication feature.
        # To avoid this problem, we specify Huawei driver can't support
        # snapshot and replication both, as a workaround.
        if (not data['snapshot_support'] and
                self.feature_supports['HyperReplication'] and
                self.configuration.replication_support):
            data['replication_type'] = 'dr'

        def _get_capacity(pool_info):
            return {
                'TOTALCAPACITY': float(pool_info['USERTOTALCAPACITY']
                                       ) / constants.CAPACITY_UNIT,
                'FREECAPACITY': float(pool_info['USERFREECAPACITY']
                                      ) / constants.CAPACITY_UNIT,
                'CONSUMEDCAPACITY': float(pool_info['USERCONSUMEDCAPACITY']
                                          ) / constants.CAPACITY_UNIT,
                'PROVISIONEDCAPACITY': float(pool_info['TOTALFSCAPACITY']
                                             ) / constants.CAPACITY_UNIT,
            }

        def _get_disk_type(pool_info):
            pool_disk = []
            for i, x in enumerate(['ssd', 'sas', 'nl_sas']):
                if ('TIER%dCAPACITY' % i in pool_info and
                        pool_info['TIER%dCAPACITY' % i] != '0'):
                    pool_disk.append(x)

            if len(pool_disk) > 1:
                pool_disk = ['mix']

            return pool_disk[0] if pool_disk else None

        pools = []
        for pool_name in self.configuration.storage_pools:
            pool = {'pool_name': pool_name}

            pool_info = self.helper.get_pool_by_name(pool_name,
                                                     log_filter=True)
            if pool_info:
                capacity = _get_capacity(pool_info)
                pool['huawei_disk_type'] = _get_disk_type(pool_info)
            else:
                capacity = {}

            pool.update({
                'max_over_subscription_ratio': self.configuration.safe_get(
                    'max_over_subscription_ratio'),
                'total_capacity_gb': capacity.get('TOTALCAPACITY', 0.0),
                'free_capacity_gb': capacity.get('FREECAPACITY', 0.0),
                'provisioned_capacity_gb':
                    capacity.get('PROVISIONEDCAPACITY', 0.0),
                'allocated_capacity_gb': capacity.get('CONSUMEDCAPACITY', 0.0),
                'reserved_percentage': 0,
                'qos': [self.feature_supports['SmartQoS'], False],
                'huawei_smartcache':
                    [self.feature_supports['SmartCache'], False],
                'huawei_smartpartition':
                    [self.feature_supports['SmartPartition'], False],
                'dedupe': [self.feature_supports['SmartDedup'], False],
                'compression':
                    [self.feature_supports['SmartCompression'], False],
            })

            if self.configuration.nas_product != "Dorado":
                pool['thin_provisioning'] = \
                    [self.feature_supports['SmartThin'], False]
            else:
                pool['thin_provisioning'] = True

            if self.metro_domain and self._check_is_active_client():
                pool['hypermetro'] = self.feature_supports['HyperMetro']
            else:
                pool['hypermetro'] = False

            pools.append(pool)
        data['pools'] = pools
        super(HuaweiNasDriver, self)._update_share_stats(data)

    def _check_is_active_client(self):
        vstore_pair_info = self.helper.get_hypermetro_vstore_by_pair_id(
            self.vstore_pair_id)
        active_flag = vstore_pair_info.get('ACTIVEORPASSIVE')
        if active_flag == '0':
            return True
        return False

    def _get_access_for_share_copy(self, share):
        share_proto = share['share_proto']
        access = {'access_level': common_constants.ACCESS_LEVEL_RW}
        if share_proto == 'NFS':
            access['access_to'] = self.configuration.nfs_client_ip
            access['access_type'] = 'ip'
        else:
            access['access_to'] = self.configuration.cifs_client_name
            access['access_password'] = self.configuration.cifs_client_password
            access['access_type'] = 'user'

        LOG.info("Get access %(access)s for share %(share)s copy.",
                 {'access': access, 'share': share['name']})
        return access

    def create_share_from_snapshot(self, context, share, snapshot,
                                   share_server=None):
        share_fs_info = self.helper.get_fs_info_by_name(
            snapshot['share_name'])
        if not share_fs_info:
            LOG.error('share %s of snapshot is not existed.',
                      snapshot['share_name'])
            raise exception.StorageResourceNotFound(
                name=snapshot['share_name'])
        if json.loads(share_fs_info.get('HYPERMETROPAIRIDS')):
            msg = _("HyperMetro Pair Share does not support "
                    "create from snapshot")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        share_fs_id = share_fs_info['ID']
        snapshot_id = huawei_utils.snapshot_id(
            share_fs_id, snapshot['name'])

        done = True

        if snapshot['snapshot']['share_proto'] == share['share_proto']:
            try:
                location = self._create_from_snapshot_by_clone(
                    context, share, share_fs_id, snapshot_id, share_server)
            except Exception:
                LOG.warning('Create share by backend clone failed, '
                            'try host copy.')
                done = False
        else:
            LOG.info('Share protocol is inconsistent, will use host copy.')
            done = False

        if not done:
            location = self._create_from_snapshot_by_host(
                context, share, snapshot, share_server)

        return location

    def _create_from_snapshot_by_clone(self, context, share, share_fs_id,
                                       snapshot_id, share_server):
        fs_id = self._create_filesystem(share, None, share_fs_id,
                                        snapshot_id, context)
        fs_info = self.helper.get_fs_info_by_id(fs_id)
        clone_size = int(fs_info['CAPACITY'])
        new_size = int(share['size']) * units.Mi * 2

        try:
            if new_size != clone_size:
                param = {"CAPACITY": new_size}
                self.helper.update_filesystem(fs_id, param)

            self.helper.split_clone_fs(fs_id)

            def _split_done():
                fs_info = self.helper.get_fs_info_by_id(fs_id)
                return fs_info['ISCLONEFS'] != 'true'

            huawei_utils.wait_for_condition(_split_done, 5, 3600 * 24)
        except Exception:
            LOG.exception('Create clone FS %s error.', fs_id)
            self.delete_share(context, share)
            raise

        share_name = share['name']
        share_proto = share['share_proto']
        fs_info = self.helper.get_fs_info_by_name(share_name)
        share_info = self.helper.get_share_by_name(
            share_name, share_proto, fs_info.get('vstoreId'))
        if not share_info:
            self._create_share(share, fs_id)
        else:
            accesses = self.helper.get_all_share_access(
                share_info['ID'], share_proto)
            for i in accesses:
                self.helper.remove_access(i['ID'], share_proto)

        return self._get_export_location(share_name, share_proto, share_server)

    def _create_from_snapshot_by_host(self, context, share, snapshot,
                                      share_server=None):
        src_share_proto = snapshot['snapshot']['share_proto']
        src_share_name = snapshot['share_name']
        src_share = {'name': src_share_name,
                     'share_proto': src_share_proto}
        src_access = self._get_access_for_share_copy(src_share)
        try:
            self.allow_access(context, src_share, src_access)
        except Exception:
            LOG.exception('Failed to add access to src share %s for copy.',
                          src_share_name)
            raise

        dst_share = share
        dst_export_paths = self.create_share(context, dst_share, share_server)
        dst_access = self._get_access_for_share_copy(dst_share)
        try:
            self.allow_access(context, dst_share, dst_access)
        except Exception:
            LOG.exception('Failed to add access to dst share %s for copy.',
                          dst_share['name'])
            self.deny_access(context, src_share, src_access)
            raise

        src_mount_dir = tempfile.mkdtemp(prefix=constants.TMP_PATH_SRC_PREFIX)
        src_export_paths = self._get_export_location(
            src_share_name, src_share_proto, share_server)
        dst_mount_dir = tempfile.mkdtemp(prefix=constants.TMP_PATH_DST_PREFIX)

        try:
            self._copy_share_data(
                src_share_proto, src_access, src_export_paths, src_mount_dir,
                snapshot['name'], dst_share['share_proto'], dst_access,
                dst_export_paths, dst_mount_dir)
        except Exception:
            LOG.exception('Copy share data from %(src)s to %(dst)s error.',
                          {'src': src_export_paths, 'dst': dst_export_paths})
            self.delete_share(context, dst_share, share_server)
            raise
        finally:
            try:
                os.rmdir(src_mount_dir)
                os.rmdir(dst_mount_dir)
            except Exception:
                LOG.exception('Remove temp files error.')
            self.deny_access(context, src_share, src_access)
            self.deny_access(context, dst_share, dst_access)

        return dst_export_paths

    def _copy_share_data(self, src_share_proto, src_access, src_export_paths,
                         src_mount_dir, snapshot_name, dst_share_proto,
                         dst_access, dst_export_paths, dst_mount_dir):
        try:
            self._mount_share_to_host(src_share_proto, src_access,
                                      src_export_paths, src_mount_dir)
        except Exception:
            LOG.exception('Mount src share %s failed.', src_export_paths)
            raise

        try:
            self._mount_share_to_host(dst_share_proto, dst_access,
                                      dst_export_paths, dst_mount_dir)
        except Exception:
            LOG.exception('Mount dst share %s failed.', dst_export_paths)
            self._umount_share_from_host(src_mount_dir)
            raise

        src_path = '/'.join((src_mount_dir, '.snapshot',
                             huawei_utils.snapshot_name(snapshot_name)))
        try:
            self._copy_data(src_path, dst_mount_dir)
        finally:
            self._umount_share_from_host(src_mount_dir)
            self._umount_share_from_host(dst_mount_dir)

    def _copy_data(self, src_path, dst_path):
        LOG.info("Copy data from src %s to dst %s.", src_path, dst_path)

        copy = data_utils.Copy(src_path, dst_path, '')
        copy.run()
        if copy.get_progress()['total_progress'] != 100:
            msg = _('Copy data from src %(src)s to dst %(dst)s error.'
                    ) % {'src': src_path, 'dst': dst_path}
            LOG.error(msg)
            raise exception.ShareCopyDataException(reason=msg)

    def _umount_share_from_host(self, mount_dir):
        utils.execute('umount', mount_dir, run_as_root=True)

    def _mount_share_to_host(self, share_proto, access, export_paths,
                             mount_dir):
        LOG.info("Mount share %(share)s to dir %(dir)s.",
                 {'share': export_paths, 'dir': mount_dir})

        for path in export_paths:
            if share_proto == 'NFS':
                exe_args = ('mount', '-t', 'nfs', path, mount_dir)
            else:
                user = 'username=%s,password=%s' % (
                    access['access_to'], access['access_password'])
                exe_args = ('mount', '-t', 'cifs', path, mount_dir,
                            '-o', user)

            try:
                utils.execute(*exe_args, run_as_root=True)
            except Exception:
                LOG.exception('Mount share %s error', path)
                continue
            else:
                return

        msg = ("Cannot mount share %(share)s to dir %(dir)s.",
               {'share': export_paths, 'dir': mount_dir})
        LOG.error(msg)
        raise exception.ShareMountException(reason=msg)

    def rpc_deny_access(self, context, params):
        share_name = params['name']
        share_proto = params['share_proto']
        access_to = params['access_to']
        access_type = params['access_type']
        if (share_proto == 'NFS' and access_type not in ('ip', 'user')
                or share_proto == 'CIFS' and access_type != 'user'):
            LOG.warning('Access type invalid for %s share.', share_proto)
            return

        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.warning('FS %s to deny access not exist.', share_name)
            return

        vstore_id = fs_info.get('vstoreId')
        share_info = self.helper.get_share_by_name(
            share_name, share_proto, vstore_id)
        if not share_info:
            LOG.warning('Share %s not exist for denying access.', share_name)
            return

        access = self.helper.get_share_access(
            share_info['ID'], access_to, share_proto, vstore_id)
        if not access:
            LOG.warning('Access %(access)s not exist in share %(share)s.',
                        {'access': access_to, 'share': share_name})
            return

        self.helper.remove_access(access['ID'], share_proto, vstore_id)

    def deny_access(self, context, share, access, share_server=None):
        share_name = share['name']
        share_proto = share['share_proto']
        access_to, access_type, access_level = huawei_utils.get_access_info(
            access)
        params = {"name": share_name,
                  "share_proto": share_proto,
                  "access_to": access_to,
                  "access_type": access_type}
        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.warning("FS %s is not exist.", share['name'])
            return
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            self.rpc_client.get_remote_fs_info(
                context, share_name, self.remote_backend)
            if self._check_is_active_client():
                self.rpc_deny_access(context, params)
            else:
                self.rpc_client.deny_access(context, params,
                                            self.remote_backend)
        else:
            self.rpc_deny_access(context, params)

    def rpc_allow_access(self, context, params):
        share_name = params['name']
        share_proto = params['share_proto']
        access_to = params['access_to']
        access_type = params['access_type']
        access_level = params['access_level']
        share_type_id = params['share_type_id']
        if share_proto == 'NFS':
            if access_type not in ('user', 'ip'):
                msg = _('Only ip or user access types '
                        'are allowed for NFS share.')
                raise exception.InvalidShareAccess(reason=msg)
            if access_type == 'user':
                # Use 'user' type as netgroup for NFS.
                access_to = '@' + access_to

            if access_level == common_constants.ACCESS_LEVEL_RW:
                access_level = constants.ACCESS_NFS_RW
            else:
                access_level = constants.ACCESS_NFS_RO
        elif share_proto == 'CIFS':
            if access_type != 'user':
                msg = _('Only user access type is allowed for CIFS share.')
                raise exception.InvalidShareAccess(reason=msg)
            if access_level == common_constants.ACCESS_LEVEL_RW:
                access_level = constants.ACCESS_CIFS_FULLCONTROL
            else:
                access_level = constants.ACCESS_CIFS_RO

        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            msg = _("FS %s to allow access not exist.") % share_name
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        vstore_id = fs_info.get('vstoreId')
        share_info = self.helper.get_share_by_name(
            share_name, share_proto, vstore_id)
        if not share_info:
            msg = _("Share %s not exist.") % share_name
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        share_access = self.helper.get_share_access(
            share_info['ID'], access_to, share_proto, vstore_id)
        if share_access:
            if (('ACCESSVAL' in share_access and
                 share_access['ACCESSVAL'] != access_level)
                    or ('PERMISSION' in share_access
                        and share_access['PERMISSION'] != access_level)):
                self.helper.change_access(
                    share_access['ID'], share_proto, access_level,
                    vstore_id)
        else:
            self.helper.allow_access(
                share_info['ID'], access_to, share_proto, access_level,
                share_type_id, vstore_id)

    def allow_access(self, context, share, access, share_server=None):
        share_name = share['name']
        share_proto = share['share_proto']
        access_to, access_type, access_level = huawei_utils.get_access_info(
            access)
        params = {"name": share_name,
                  "share_proto": share_proto,
                  "access_to": access_to,
                  "access_type": access_type,
                  "access_level": access_level,
                  "share_type_id": share.get("share_type_id")}
        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            msg = _("FS %s to allow access not exist.") % share_name
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)
        if json.loads(fs_info.get('HYPERMETROPAIRIDS')):
            self.rpc_client.get_remote_fs_info(
                context, share_name, self.remote_backend)
            if self._check_is_active_client():
                self.rpc_allow_access(context, params)
            else:
                self.rpc_client.allow_access(
                    context, params, self.remote_backend)
        else:
            self.rpc_allow_access(context, params)

    def update_access(self, context, share, access_rules, add_rules,
                      delete_rules, share_server=None):
        def _access_handler(rules, handler):
            for access in rules:
                try:
                    handler(context, share, access, share_server)
                except Exception:
                    LOG.exception(
                        'Failed to %(handler)s access %(access)s for share '
                        '%(share)s.',
                        {'handler': handler.__name__,
                         'access': huawei_utils.get_access_info(access),
                         'share': share['name']})
                    raise

        if not add_rules and not delete_rules:
            _access_handler(access_rules, self.allow_access)
        else:
            _access_handler(delete_rules, self.deny_access)
            _access_handler(add_rules, self.allow_access)

    def get_pool(self, share):
        fs_info = self.helper.get_fs_info_by_name(share['name'])
        if fs_info:
            return fs_info['PARENTNAME']

    def manage_existing(self, share, driver_options):
        share_proto = share['share_proto']

        old_export_location = share['export_locations'][0]['path']
        old_share_ip, old_share_name = huawei_utils.get_share_by_location(
            old_export_location, share_proto)
        if not old_share_name:
            msg = _('Export location %s is invalid.') % old_export_location
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        if old_share_ip not in self.configuration.logical_ip \
                and old_share_ip not in self.configuration.dns:
            msg = _('IP %s inconsistent with logical IP.') % old_share_ip
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
        fs_info = self.helper.get_fs_info_by_name(old_share_name)
        share_info = self.helper.get_share_by_name(
            old_share_name, share_proto, fs_info.get('vstoreId'))
        if not share_info:
            msg = _("Share %s not exist.") % old_share_name
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        fs_id = share_info['FSID']
        fs_info = self.helper.get_fs_info_by_id(fs_id)
        if (fs_info['HEALTHSTATUS'] != constants.STATUS_FS_HEALTH or
                fs_info['RUNNINGSTATUS'] != constants.STATUS_FS_RUNNING):
            msg = _("FS %s status is abnormal.") % fs_id
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        if (json.loads(fs_info['REMOTEREPLICATIONIDS']) or
                json.loads(fs_info['HYPERMETROPAIRIDS'])):
            msg = _("FS %s has been associated to other feature, "
                    "cannot manage it.") % fs_id
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        pool_name = share_utils.extract_host(share['host'], level='pool')
        if pool_name and pool_name != fs_info['PARENTNAME']:
            msg = _("FS %(id)s pool is inconsistent with %(pool)s."
                    ) % {'id': fs_id, 'pool': pool_name}
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        opts = huawei_utils.get_share_extra_specs_params(
            share['share_type_id'])
        if 'LUNType' in opts and fs_info['ALLOCTYPE'] != opts['LUNType']:
            msg = _("FS %(id)s type is inconsistent with %(type)s."
                    ) % {'id': fs_id, 'type': opts['LUNType']}
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        if (fs_info['ALLOCTYPE'] == constants.ALLOC_TYPE_THICK_FLAG and
                (opts['compression'] or opts['dedupe'])):
            msg = _('Dedupe or compression cannot be set for thick FS.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        share_params = {'DESCRIPTION': share['name']}
        self.helper.update_share(share_info['ID'], share_proto, share_params)
        share_size = self._retype_filesystem(opts, fs_info, share['name'])

        locations = self._get_export_location(share['name'], share_proto, None)
        return {'size': share_size, 'export_locations': locations}

    def _retype_filesystem(self, new_opts, fs_info, new_share_name):
        fs_id = fs_info['ID']

        if new_opts['huawei_smartpartition']:
            if fs_info['CACHEPARTITIONID']:
                self.smart_partition.update(fs_id, new_opts['partitionname'],
                                            fs_info['CACHEPARTITIONID'])
            else:
                self.smart_partition.add(new_opts['partitionname'], fs_id)
        elif fs_info['CACHEPARTITIONID']:
            self.smart_partition.remove(fs_id, fs_info['CACHEPARTITIONID'])

        if new_opts['huawei_smartcache']:
            if fs_info['SMARTCACHEPARTITIONID']:
                self.smart_cache.update(fs_id, new_opts['cachename'],
                                        fs_info['SMARTCACHEPARTITIONID'])
            else:
                self.smart_cache.add(new_opts['cachename'], fs_id)
        elif fs_info['SMARTCACHEPARTITIONID']:
            self.smart_cache.remove(fs_id, fs_info['SMARTCACHEPARTITIONID'])

        if new_opts['qos']:
            if fs_info['IOCLASSID']:
                self.smart_qos.update(fs_id, new_opts['qos'],
                                      fs_info['IOCLASSID'])
            else:
                self.smart_qos.add(new_opts['qos'], fs_id)
        elif fs_info['IOCLASSID']:
            self.smart_qos.remove(fs_id, fs_info['IOCLASSID'])

        fs_param = {"NAME": huawei_utils.share_name(new_share_name),
                    "DESCRIPTION": new_share_name,
                    }

        compression = strutils.bool_from_string(fs_info['ENABLECOMPRESSION'])
        if new_opts['compression'] and not compression:
            fs_param["ENABLECOMPRESSION"] = True
        elif compression:
            fs_param["ENABLECOMPRESSION"] = False

        dedupe = strutils.bool_from_string(fs_info['ENABLEDEDUP'])
        if new_opts['dedupe'] and not dedupe:
            fs_param["ENABLEDEDUP"] = True
        elif dedupe:
            fs_param["ENABLEDEDUP"] = False

        cur_size = int(fs_info['CAPACITY']) / constants.CAPACITY_UNIT
        new_size = math.ceil(float(fs_info['CAPACITY']) /
                             constants.CAPACITY_UNIT)
        if cur_size != new_size:
            fs_param["CAPACITY"] = new_size * constants.CAPACITY_UNIT

        if new_opts['sectorsize']:
            sectorsize = int(new_opts['sectorsize']) * units.Ki
            if sectorsize != int(fs_info['SECTORSIZE']):
                fs_param['SECTORSIZE'] = sectorsize

        self.helper.update_filesystem(fs_id, fs_param)
        return new_size

    def unmanage(self, share):
        pass

    def manage_existing_snapshot(self, snapshot, driver_options):
        fs_info = self.helper.get_fs_info_by_name(snapshot['share_name'])
        if not fs_info:
            msg = _("Parent FS %(fs)s of snapshot %(snap)s not exist."
                    ) % {'snap': snapshot['id'],
                         'fs': snapshot['share_name']}
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        snapshot_id = fs_info['ID'] + "@" + snapshot['provider_location']
        snapshot_info = self.helper.get_snapshot_by_id(snapshot_id)
        if snapshot_info['HEALTHSTATUS'] != constants.STATUS_SNAPSHOT_HEALTH:
            msg = _("Snapshot %s is abnormal, cannot import.") % snapshot_id
            LOG.error(msg)
            raise exception.ManageInvalidShareSnapshot(reason=msg)

        snapshot_name = huawei_utils.snapshot_name(snapshot['name'])
        self.helper.rename_snapshot(snapshot_id, snapshot_name)
        snapshot_id = huawei_utils.snapshot_id(
            fs_info['ID'], snapshot_name)
        return {'provider_location': snapshot_id}

    def get_network_allocations_number(self):
        if self.configuration.driver_handles_share_servers:
            return 1
        else:
            return 0

    def _setup_server(self, network_info, metadata=None):
        LOG.info('To setup server: %s.', network_info)
        network_type = network_info['network_type']
        if network_type not in constants.VALID_NETWORK_TYPE:
            msg = _('Network type %s is invalid.') % network_type
            LOG.error(msg)
            raise exception.NetworkBadConfigurationException(reason=msg)

        vlan_tag = network_info['segmentation_id'] or 0
        ip = network_info['network_allocations'][0]['ip_address']
        ip_addr = ipaddress.ip_address(ip)
        subnet = utils.cidr_to_netmask(network_info['cidr'])

        ad, ldap = self._get_security_service(
            network_info['security_services'])

        ad_created = False
        ldap_created = False
        if ad:
            self._configure_ad(ad)
            ad_created = True
        if ldap:
            self._configure_ldap(ldap)
            ldap_created = True

        try:
            vlan_id, logical_port_id = self._create_logical_port(
                vlan_tag, ip, ip_addr.version, subnet)
        except exception.ManilaException:
            if ad_created:
                self.helper.delete_ad_config(ad['user'], ad['password'])
                self.helper.set_dns_ip_address([])
            if ldap_created:
                self.helper.delete_ldap_config()
            raise

        server_details = {'ip': ip,
                          'logical_port_id': logical_port_id,
                          }
        if vlan_id:
            server_details['vlan_id'] = vlan_id
        return server_details

    def _get_security_service(self, security_services):
        active_directory = None
        ldap = None
        for ss in security_services:
            if ss['type'] == 'active_directory':
                active_directory = ss
            elif ss['type'] == 'ldap':
                ldap = ss
        return active_directory, ldap

    def _configure_ad(self, active_directory):
        dns_ip = active_directory['dns_ip']
        user = active_directory['user']
        password = active_directory['password']
        domain = active_directory['domain']
        if not dns_ip or not user or not password or not domain:
            msg = (_("(%(dns_ip)s, %(user)s, %(password)s, %(domain)s) of "
                     "active_directory invalid.")
                   % {"dns_ip": dns_ip, "user": user,
                      "password": password, "domain": domain})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        # Check DNS server exists or not.
        ip_address = self.helper.get_dns_ip_address()
        if ip_address:
            msg = _("DNS server %s has already been configured."
                    ) % ip_address
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        # Check AD config exists or not.
        ad_config = self.helper.get_ad_config()
        if ad_config:
            msg = _("AD domain %s has already been configured."
                    ) % ad_config['FULLDOMAINNAME']
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        # Set DNS server ip.
        self.helper.set_dns_ip_address([dns_ip])

        def _check_ad_status():
            ad = self.helper.get_ad_config()
            if not ad or ad['DOMAINSTATUS'] == constants.AD_JOIN_FAILED:
                msg = _('AD domain status is failed.')
                LOG.error(msg)
                raise exception.ShareBackendException(msg=msg)
            return ad['DOMAINSTATUS'] == constants.AD_JOIN_DOMAIN

        # Set AD config.
        try:
            self.helper.add_ad_config(user, password, domain)
            huawei_utils.wait_for_condition(
                _check_ad_status, self.configuration.wait_interval,
                self.configuration.timeout)
        except exception.ManilaException:
            self.helper.set_dns_ip_address([])
            msg = _('Failed to add AD config.')
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

    def _configure_ldap(self, ldap):
        server = ldap['server']
        domain = ldap['domain']
        if not server or not domain:
            msg = (_("(%(server)s, %(domain)s) of ldap invalid.")
                   % {"server": server, "domain": domain})
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        # Check LDAP config exists or not.
        ldap_info = self.helper.get_ldap_config()
        if ldap_info:
            err_msg = _("LDAP domain (%s) has already been configured."
                        ) % ldap_info['LDAPSERVER']
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        if len(server.split(',')) > 3:
            msg = _("Server IPs of ldap greater than 3.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        # Set LDAP config.
        self.helper.add_ldap_config(server, domain)

    def _create_logical_port(self, vlan_tag, ip, ip_type, subnet):
        vlan_id = None
        if vlan_tag:
            vlans = self.helper.get_vlan_by_tag(vlan_tag)
            if vlans:
                vlan_id = vlans[0]['ID']
            else:
                port, port_type = self._get_optimal_port()
                vlan_id = self.helper.create_vlan(
                    port['id'], port_type, vlan_tag)
            home_port_id = vlan_id
            home_port_type = constants.PORT_TYPE_VLAN
        else:
            port, port_type = self._get_optimal_port()
            home_port_id = port['id']
            home_port_type = port_type

        logical_port = self.helper.get_logical_port_by_ip(ip, ip_type)
        if not logical_port:
            data = {"HOMEPORTID": home_port_id,
                    "HOMEPORTTYPE": home_port_type,
                    "NAME": ip,
                    "OPERATIONALSTATUS": True,
                    "SUPPORTPROTOCOL": 3,
                    }
            if ip_type == 4:
                data.update({"ADDRESSFAMILY": 0,
                             "IPV4ADDR": ip,
                             "IPV4MASK": subnet,
                             })
            else:
                data.update({"ADDRESSFAMILY": 1,
                             "IPV6ADDR": ip,
                             "IPV6MASK": subnet,
                             })
            logical_port_id = self.helper.create_logical_port(data)
        else:
            logical_port_id = logical_port['ID']

        return vlan_id, logical_port_id

    def _get_optimal_port(self):
        eth_ports, bond_ports = self._get_valid_ports(self.configuration.ports)
        logical_ports = self.helper.get_all_logical_port()
        sorted_eths = self._sorted_ports(eth_ports, logical_ports)
        sorted_bonds = self._sorted_ports(bond_ports, logical_ports)

        if sorted_eths and sorted_bonds:
            if sorted_eths[0][1] >= sorted_bonds[0][1]:
                return sorted_bonds[0][0], constants.PORT_TYPE_BOND
            else:
                return sorted_eths[0][0], constants.PORT_TYPE_ETH
        elif sorted_eths:
            return sorted_eths[0][0], constants.PORT_TYPE_ETH
        elif sorted_bonds:
            return sorted_bonds[0][0], constants.PORT_TYPE_BOND
        else:
            msg = _("Cannot find optimal port.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

    def _get_valid_ports(self, config_ports):
        eth_ports = self.helper.get_all_eth_port()
        bond_ports = self.helper.get_all_bond_port()

        def _filter_eth_port(port):
            return (constants.PORT_LINKUP == port['RUNNINGSTATUS']
                    and not port['IPV4ADDR']
                    and not port['IPV6ADDR']
                    and not port['BONDNAME']
                    and (not config_ports or port['LOCATION'] in config_ports)
                    )

        def _filter_bond_port(port):
            if (constants.PORT_LINKUP != port['RUNNINGSTATUS'] or
                    (config_ports and port['NAME'] not in config_ports)):
                return False
            port_ids = json.loads(port['PORTIDLIST'])
            for eth in eth_ports:
                if eth['ID'] in port_ids and (
                        eth['IPV4ADDR'] or eth['IPV6ADDR']):
                    return False
            return True

        valid_eth_ports = [{'id': eth['ID'], 'name': eth['LOCATION']}
                           for eth in eth_ports if _filter_eth_port(eth)]
        valid_bond_ports = [{'id': bond['ID'], 'name': bond['NAME']}
                            for bond in bond_ports if _filter_bond_port(bond)]
        return valid_eth_ports, valid_bond_ports

    def _sorted_ports(self, port_list, logical_ports):
        def _get_port_weight(port):
            weight = 0
            for logical in logical_ports:
                if logical['HOMEPORTTYPE'] == constants.PORT_TYPE_VLAN:
                    pos = logical['HOMEPORTNAME'].rfind('.')
                    if logical['HOMEPORTNAME'][:pos] == port['name']:
                        weight += 1
                elif logical['HOMEPORTNAME'] == port['name']:
                    weight += 1
            return weight

        sorted_ports = []
        for port in port_list:
            port_weight = _get_port_weight(port)
            sorted_ports.append((port, port_weight))

        return sorted(sorted_ports, key=lambda i: i[1])

    def _teardown_server(self, server_details, security_services=None):
        if 'logical_port_id' in server_details:
            self.helper.delete_logical_port(
                server_details['logical_port_id'])

        if 'vlan_id' in server_details:
            self.helper.delete_vlan(server_details['vlan_id'])

        if not security_services:
            return

        ad, ldap = self._get_security_service(security_services)
        if ad:
            ip_address = self.helper.get_dns_ip_address()
            if ip_address and ip_address[0] == ad['dns_ip']:
                self.helper.set_dns_ip_address([])

            ad_config = self.helper.get_ad_config()
            if ad_config and ad_config['FULLDOMAINNAME'] == ad['domain']:
                self.helper.delete_ad_config(ad['user'], ad['password'])

        if ldap:
            ldap_info = self.helper.get_ldap_config()
            if (ldap_info and ldap_info['LDAPSERVER'] == ldap['server']
                    and ldap_info['BASEDN'] == ldap['domain']):
                self.helper.delete_ldap_config()

    def ensure_share(self, context, share, share_server=None):
        share_proto = share['share_proto']
        share_name = share['name']
        fs_info = self.helper.get_fs_info_by_name(share_name)
        if not fs_info:
            LOG.error('FS %s not exist while ensuring.', share_name)
            raise exception.ShareResourceNotFound(share_id=share['id'])

        if (fs_info['HEALTHSTATUS'] != constants.STATUS_FS_HEALTH or
                fs_info['RUNNINGSTATUS'] != constants.STATUS_FS_RUNNING):
            msg = _('FS %s status is abnormal.') % share_name
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

        share_info = self.helper.get_share_by_name(
            share_name, share_proto, fs_info.get('vstoreId'))
        if not share_info:
            LOG.error('Share %s not exist while ensuring.', share_name)
            raise exception.ShareResourceNotFound(share_id=share['id'])

        return self._get_export_location(share_name, share_proto, share_server)

    def create_replica(self, context, replica_list, new_replica,
                       access_rules, replica_snapshots, share_server=None):
        """Create a new share, and create a remote replication pair."""
        location = self.create_share(context, new_replica, share_server)

        try:
            for access in access_rules:
                self.allow_access(context, new_replica, access)
        except Exception:
            LOG.exception('Failed to allow access to new replica %s.',
                          new_replica['name'])
            self.delete_share(context, new_replica, share_server)
            raise

        # create a replication pair.
        # replication pair only can be created by master node,
        # so here is a remote call to trigger master node to
        # start the creating progress.
        try:
            active_replica = share_utils.get_active_replica(replica_list)
            remote_device_wwn = self.helper.get_array_wwn()
            replica_fs = self.helper.get_fs_info_by_name(
                new_replica['name'])

            (local_pair_id, replica_pair_id) = \
                self.rpc_client.create_replica_pair(
                context,
                active_replica['host'],
                local_share_info={'name': active_replica['name']},
                remote_device_wwn=remote_device_wwn,
                remote_fs_id=replica_fs['ID'],
                local_replication=self.configuration.local_replication
            )
        except Exception:
            LOG.exception('Failed to create a replication pair '
                          'with host %s.', active_replica['host'])
            self.delete_share(context, new_replica, share_server)
            raise

        # Get the state of the new created replica
        replica_state = self.replica_mgr.get_replica_state(local_pair_id)
        replica_ref = {
            'export_locations': location,
            'replica_state': replica_state,
        }

        return replica_ref

    def update_replica_state(self, context, replica_list, replica,
                             access_rules, replica_snapshots,
                             share_server=None):
        replica_name = replica['name']
        replica_pair_id = huawei_utils.get_replica_pair_id(
            self.helper, replica_name)
        if not replica_pair_id:
            LOG.error("No replication pair for replica %s.", replica_name)
            return common_constants.STATUS_ERROR

        self.replica_mgr.update_replication_pair_state(replica_pair_id)
        return self.replica_mgr.get_replica_state(replica_pair_id)

    def promote_replica(self, context, replica_list, replica, access_rules,
                        share_server=None):
        replica_name = replica['name']
        replica_pair_id = huawei_utils.get_replica_pair_id(
            self.helper, replica_name)
        if not replica_pair_id:
            msg = _("No replication pair for replica %s.") % replica_name
            LOG.error(msg)
            raise exception.ReplicationException(reason=msg)

        try:
            self.replica_mgr.switch_over(replica_pair_id)
        except Exception:
            LOG.exception('Failed to promote replica %s.', replica_name)
            raise

        old_active_replica = share_utils.get_active_replica(replica_list)
        new_active_update = {
            'id': replica['id'],
            'replica_state': common_constants.REPLICA_STATE_ACTIVE,
        }

        # get replica state for new secondary after switch over
        replica_state = self.replica_mgr.get_replica_state(replica_pair_id)
        old_active_update = {
            'id': old_active_replica['id'],
            'replica_state': replica_state,
        }

        return [old_active_update, new_active_update]

    def delete_replica(self, context, replica_list, replica_snapshots,
                       replica, share_server=None):
        replica_name = replica['name']
        replica_pair_id = huawei_utils.get_replica_pair_id(
            self.helper, replica_name)
        if not replica_pair_id:
            LOG.warning("No replication pair for replica %s, "
                        "continue to delete it.", replica_name)
        else:
            self.replica_mgr.delete_replication_pair(replica_pair_id)

        try:
            self.delete_share(context, replica, share_server)
        except Exception:
            LOG.exception('Failed to delete replica %s.', replica_name)
            raise

    def revert_to_snapshot(self, context, snapshot, share_access_rules,
                           snapshot_access_rules, share_server=None):
        fs_info = self.helper.get_fs_info_by_name(snapshot['share_name'])
        if not fs_info:
            msg = _("FS %s not exist.") % snapshot['share_name']
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

        snap_id = huawei_utils.snapshot_id(fs_info['ID'], snapshot['name'])
        self.helper.rollback_snapshot(snap_id)
