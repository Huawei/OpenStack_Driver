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


class HuaweiV3Manager(object):
    """Manages the huawei V3 backend rpc."""

    RPC_API_VERSION = '1.1'

    @property
    def target(self):
        """This property is used by oslo_messaging."""
        if not hasattr(self, '_target'):
            import oslo_messaging as messaging
            self._target = messaging.Target(version=self.RPC_API_VERSION)
        return self._target

    def __init__(self, connection, replica_mgr):
        self.connection = connection
        self.replica_mgr = replica_mgr

    def create_replica_pair(self, ctx,
                            local_share_info,
                            remote_device_wwn,
                            remote_fs_id):
        """Create replication pair."""
        return self.replica_mgr.create(local_share_info,
                                       remote_device_wwn,
                                       remote_fs_id)

    def clear_share_access(self, ctx, share):
        """Clear share access."""
        return self.connection.clear_access(share)
