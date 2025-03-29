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
import time

import oslo_messaging as messaging

from manila import rpc
from manila.share import utils
from manila.share.drivers.huawei import constants


class HuaweiAPI(object):
    """Client side of the huawei storage rpc API.

    API version history:

        1.0  - Initial version.
    """

    BASE_RPC_API_VERSION = '1.0'

    def __init__(self):
        self.topic = 'huawei_storage'
        target = messaging.Target(topic=self.topic,
                                  version=self.BASE_RPC_API_VERSION)
        self.client = rpc.get_client(target, version_cap='1.0')

    @staticmethod
    def _retry_rpc_call(call_times, interval, call_context_call, context,
                        rpc_fun_name, **kwargs):
        while call_times:
            try:
                return call_context_call(context, rpc_fun_name, **kwargs)
            except Exception:
                call_times -= 1
                if not call_times:
                    raise
                time.sleep(interval)
                continue

    def create_replica_pair(
            self, context, host, local_share_info, remote_device_wwn,
            remote_fs_id, local_replication):
        new_host = utils.extract_host(host)
        call_context = self.client.prepare(server=new_host, version='1.0')
        return call_context.call(
            context, 'create_replica_pair',
            local_share_info=local_share_info,
            remote_device_wwn=remote_device_wwn,
            remote_fs_id=remote_fs_id,
            local_replication=local_replication,
        )

    def update_replica_filesystem(self, context, host, replica_fs_id, params):
        new_host = utils.extract_host(host)
        call_context = self.client.prepare(server=new_host, version='1.0')
        return call_context.call(
            context, 'update_replica_filesystem',
            replica_fs_id=replica_fs_id,
            params=params
        )

    def create_remote_filesystem(self, context, host, params):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err

        try:
            return call_context.call(context, 'create_remote_filesystem',
                                     params=params)
        except Exception:
            try:
                return call_context.call(context, 'create_remote_filesystem',
                                         params=params)
            except Exception as err:
                raise err

    def delete_remote_filesystem(self, context, host, params):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(context, 'delete_remote_filesystem',
                              params=params)
        except Exception:
            try:
                call_context.call(context, 'delete_remote_filesystem',
                                  params=params)
            except Exception as err:
                raise err

    def delete_share(self, context, share_name, share_proto, host):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(context,
                              'delete_share',
                              share_name=share_name,
                              share_proto=share_proto)
        except Exception:
            try:
                call_context.call(context,
                                  'delete_share',
                                  share_name=share_name,
                                  share_proto=share_proto)
            except Exception as err:
                raise err

    def deny_access(self, context, params, host):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(context,
                              'deny_access',
                              params=params)
        except Exception:
            try:
                call_context.call(context,
                                  'deny_access',
                                  params=params)
            except Exception as err:
                raise err

    def allow_access(self, context, params, host):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(context,
                              'allow_access',
                              params=params)
        except Exception:
            try:
                call_context.call(context,
                                  'allow_access',
                                  params=params)
            except Exception as err:
                raise err

    def update_filesystem(self, context, host, fs_id, params):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(context, 'update_filesystem', fs_id=fs_id,
                              params=params)
        except Exception:
            try:
                call_context.call(context, 'update_filesystem', fs_id=fs_id,
                                  params=params)
            except Exception as err:
                raise err

    def check_remote_metro_info(self, context, host, domain_name,
                                local_vstore, remote_vstore, vstore_pair_id):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(
                context, 'check_remote_metro_info', domain_name=domain_name,
                local_vstore=remote_vstore, remote_vstore=local_vstore,
                vstore_pair_id=vstore_pair_id)
        except Exception:
            try:
                call_context.call(
                    context, 'check_remote_metro_info',
                    domain_name=domain_name,
                    local_vstore=local_vstore,
                    remote_vstore=remote_vstore,
                    vstore_pair_id=vstore_pair_id)
            except Exception as err:
                raise err

    def get_remote_fs_info(self, context, share_name, host):
        try:
            call_context = self.client.prepare(server=host, version='1.0')
        except Exception as err:
            raise err
        try:
            call_context.call(
                context, 'get_remote_fs_info',
                share_name=share_name
            )
        except Exception:
            try:
                call_context.call(
                    context, 'get_remote_fs_info',
                    share_name=share_name)
            except Exception as err:
                raise err

    def create_replica_snapshot(
            self, context, host, replica_share_name,
            active_snapshot_name, replica_snapshot_name):
        new_host = utils.extract_host(host)
        call_context = self.client.prepare(server=new_host, version='1.0')
        return call_context.call(
            context, 'create_replica_snapshot',
            replica_share_name=replica_share_name,
            active_snapshot_name=active_snapshot_name,
            replica_snapshot_name=replica_snapshot_name,
        )

    def delete_replica_snapshot(
            self, context, host, replica_share_name, replica_snapshot_name):
        new_host = utils.extract_host(host)
        call_context = self.client.prepare(server=new_host, version='1.0')
        return call_context.call(
            context, 'delete_replica_snapshot',
            replica_share_name=replica_share_name,
            replica_snapshot_name=replica_snapshot_name,
        )

    def create_hypermetro_snapshot(self, context, share_name, snapshot_name,
                                   host):
        call_context = self.client.prepare(server=host, version='1.0')
        return self._retry_rpc_call(constants.RPC_CALL_TIMES,
                                    constants.RPC_CALL_INTERVAL,
                                    call_context.call, context,
                                    "create_hypermetro_snapshot",
                                    share_name=share_name,
                                    snapshot_name=snapshot_name)

    def delete_hypermetro_snapshot(self, context, share_name, snapshot_name,
                                   host):
        call_context = self.client.prepare(server=host, version='1.0')
        return self._retry_rpc_call(constants.RPC_CALL_TIMES,
                                    constants.RPC_CALL_INTERVAL,
                                    call_context.call, context,
                                    "delete_hypermetro_snapshot",
                                    share_name=share_name,
                                    snapshot_name=snapshot_name)

    def revert_to_hypermetro_snapshot(self, context, share_name, snapshot_name,
                                      host):
        call_context = self.client.prepare(server=host, version='1.0')
        return self._retry_rpc_call(constants.RPC_CALL_TIMES,
                                    constants.RPC_CALL_INTERVAL,
                                    call_context.call, context,
                                    "revert_to_hypermetro_snapshot",
                                    share_name=share_name,
                                    snapshot_name=snapshot_name)

    def create_share_from_hypermetro_snapshot(self, context, share,
                                              snapshot, share_server, host):
        call_context = self.client.prepare(server=host, version='1.0')
        return self._retry_rpc_call(constants.RPC_CALL_TIMES,
                                    constants.RPC_CALL_INTERVAL,
                                    call_context.call, context,
                                    "create_share_from_hypermetro_snapshot",
                                    share=share,
                                    snapshot=snapshot,
                                    share_server=share_server)
