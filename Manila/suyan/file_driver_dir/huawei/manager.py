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


class HuaweiManager(object):
    """Manages the huawei storage backend rpc."""

    RPC_API_VERSION = '1.0'

    @property
    def target(self):
        """This property is used by oslo_messaging."""
        if not hasattr(self, '_target'):
            import oslo_messaging as messaging
            self._target = messaging.Target(version=self.RPC_API_VERSION)
        return self._target

    def __init__(self, driver, replica_mgr, metro_mgr):
        self.driver = driver
        self.replica_mgr = replica_mgr
        self.metro_mgr = metro_mgr
        self._target = None

    def create_replica_pair(
            self, ctx, local_share_info, remote_device_wwn, remote_fs_id,
            local_replication):
        """Create replication pair."""
        return self.replica_mgr.create(
            local_share_info, remote_device_wwn, remote_fs_id,
            local_replication)

    def create_replica_snapshot(
            self, ctx, replica_share_name,
            active_snapshot_name, replica_snapshot_name):
        """Create replication snapshot."""
        return self.driver.rpc_update_snapshot(
            replica_share_name, active_snapshot_name, replica_snapshot_name)

    def delete_replica_snapshot(self, ctx, replica_share_name,
                                replica_snapshot_name):
        """Delete replication snapshot."""
        return self.driver.rpc_delete_snapshot(
            replica_share_name, replica_snapshot_name)

    def update_replica_filesystem(self, context, replica_fs_id, params):
        return self.driver.update_replica_filesystem(replica_fs_id, params)

    def create_remote_filesystem(self, context, params):
        return self.metro_mgr.create_remote_filesystem(params)

    def delete_remote_filesystem(self, context, params):
        return self.metro_mgr.delete_remote_filesystem(params)

    def update_filesystem(self, context, fs_id, params):
        return self.metro_mgr.update_filesystem(fs_id, params)

    def check_remote_metro_info(self, context, domain_name, local_vstore,
                                remote_vstore, vstore_pair_id):
        return self.metro_mgr.check_remote_metro_info(
            domain_name, local_vstore, remote_vstore, vstore_pair_id)

    def delete_share(self, context, share_name, share_proto):
        self.driver.rpc_delete_share(context, share_name, share_proto)

    def deny_access(self, context, params):
        self.driver.rpc_deny_access(context, params)

    def allow_access(self, context, params):
        self.driver.rpc_allow_access(context, params)

    def get_remote_fs_info(self, context, share_name):
        self.metro_mgr.get_remote_fs_info(share_name)

    def create_hypermetro_snapshot(self, context, share_name, snapshot_name):
        return self.driver.rpc_create_hypermetro_snapshot(context, share_name,
                                                          snapshot_name)

    def delete_hypermetro_snapshot(self, context, share_name, snapshot_name):
        return self.driver.rpc_delete_hypermetro_snapshot(context, share_name,
                                                          snapshot_name)

    def revert_to_hypermetro_snapshot(self, context, share_name,
                                      snapshot_name):
        self.driver.rpc_revert_to_hypermetro_snapshot(context, share_name,
                                                      snapshot_name)

    def create_share_from_hypermetro_snapshot(self, context, share, snapshot,
                                              share_server):
        return self.driver.rpc_create_share_from_hypermetro_snapshot(
            context, share, snapshot, share_server)
