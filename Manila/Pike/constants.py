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

STATUS_ETH_RUNNING = "10"
STATUS_FS_HEALTH = "1"
STATUS_FS_RUNNING = "27"
STATUS_FSSNAPSHOT_HEALTH = '1'
STATUS_JOIN_DOMAIN = '1'
STATUS_EXIT_DOMAIN = '0'
STATUS_SERVICE_RUNNING = "2"
STATUS_QOS_ACTIVE = '2'
STATUS_QOS_INACTIVATED = '45'

DEFAULT_WAIT_INTERVAL = 3
DEFAULT_TIMEOUT = 60
PWD_EXPIRED_OR_INITIAL = (3, 4)

MAX_FS_NUM_IN_QOS = 64
MSG_SNAPSHOT_NOT_FOUND = 1073754118
IP_ALLOCATIONS_DHSS_FALSE = 0
IP_ALLOCATIONS_DHSS_TRUE = 1
SOCKET_TIMEOUT = 52
LOGIN_SOCKET_TIMEOUT = 4
QOS_NAME_PREFIX = 'OpenStack_'
SYSTEM_NAME_PREFIX = "Array-"
MIN_ARRAY_VERSION_FOR_QOS = 'V300R003C00'
TMP_PATH_SRC_PREFIX = "huawei_manila_tmp_path_src_"
TMP_PATH_DST_PREFIX = "huawei_manila_tmp_path_dst_"

ACCESS_NFS_RW = "1"
ACCESS_NFS_RO = "0"
ACCESS_CIFS_FULLCONTROL = "1"
ACCESS_CIFS_RO = "0"

ERROR_CONNECT_TO_SERVER = -403
ERROR_UNAUTHORIZED_TO_SERVER = -401
ERROR_LOGICAL_PORT_EXIST = 1073813505
ERROR_USER_OR_GROUP_NOT_EXIST = 1077939723
ERROR_REPLICATION_PAIR_NOT_EXIST = 1077937923
ERROR_HYPERMETRO_NOT_EXIST = 1077674242

PORT_TYPE_ETH = '1'
PORT_TYPE_BOND = '7'
PORT_TYPE_VLAN = '8'

SORT_BY_VLAN = 1
SORT_BY_LOGICAL = 2

ALLOC_TYPE_THIN_FLAG = "1"
ALLOC_TYPE_THICK_FLAG = "0"

ALLOC_TYPE_THIN = "Thin"
ALLOC_TYPE_THICK = "Thick"
THIN_PROVISIONING = "true"
THICK_PROVISIONING = "false"

FILE_SYSTEM_POOL_TYPE = '2'
DORADO_V6_POOL_TYPE = '0'

OPTS_QOS_VALUE = {
    'maxiops': None,
    'miniops': None,
    'minbandwidth': None,
    'maxbandwidth': None,
    'latency': None,
    'iotype': None
}

QOS_LOWER_LIMIT = ['MINIOPS', 'LATENCY', 'MINBANDWIDTH']
QOS_UPPER_LIMIT = ['MAXIOPS', 'MAXBANDWIDTH']

OPTS_CAPABILITIES = {
    'dedupe': False,
    'compression': False,
    'huawei_smartcache': False,
    'huawei_smartpartition': False,
    'huawei_controller': False,
    'thin_provisioning': None,
    'qos': False,
    'huawei_sectorsize': None,
    'huawei_share_privilege': False,
    'hypermetro': False,
}

OPTS_VALUE = {
    'cachename': None,
    'partitionname': None,
    'sectorsize': None,
    'controllername': None,
    'sync': None,
    'allsquash': None,
    'rootsquash': None,
    'secure': None,
}

OPTS_PRIVILEGE_VALUE = {
    'sync': None,
    'allsquash': None,
    'rootsquash': None,
    'secure': None
}


OPTS_VALUE.update(OPTS_QOS_VALUE)

OPTS_ASSOCIATE = {
    'huawei_smartcache': 'cachename',
    'huawei_smartpartition': 'partitionname',
    'huawei_sectorsize': 'sectorsize',
    'qos': OPTS_QOS_VALUE,
    'huawei_controller': 'controllername',
    'huawei_share_privilege': OPTS_PRIVILEGE_VALUE,
}

VALID_SECTOR_SIZES = ('4', '8', '16', '32', '64')

LOCAL_RES_TYPES = (FILE_SYSTEM_TYPE,) = ('40',)

REPLICA_MODELS = (REPLICA_SYNC_MODEL,
                  REPLICA_ASYNC_MODEL) = ('1', '2')

REPLICATION_TYPES = (REMOTE_REPLICATION, LOCAL_REPLICATION) = ('0', '1')

REPLICA_SPEED_MODELS = (REPLICA_SPEED_LOW,
                        REPLICA_SPEED_MEDIUM,
                        REPLICA_SPEED_HIGH,
                        REPLICA_SPEED_HIGHEST) = ('1', '2', '3', '4')

REPLICA_HEALTH_STATUSES = (REPLICA_HEALTH_STATUS_NORMAL,
                           REPLICA_HEALTH_STATUS_FAULT,
                           REPLICA_HEALTH_STATUS_INVALID) = ('1', '2', '14')

REPLICA_DATA_STATUSES = (
    REPLICA_DATA_STATUS_SYNCHRONIZED,
    REPLICA_DATA_STATUS_COMPLETE,
    REPLICA_DATA_STATUS_INCOMPLETE) = ('1', '2', '5')

REPLICA_DATA_STATUS_IN_SYNC = (
    REPLICA_DATA_STATUS_SYNCHRONIZED,
    REPLICA_DATA_STATUS_COMPLETE)

REPLICA_RUNNING_STATUSES = (
    REPLICA_RUNNING_STATUS_NORMAL,
    REPLICA_RUNNING_STATUS_SYNCING,
    REPLICA_RUNNING_STATUS_SPLITTED,
    REPLICA_RUNNING_STATUS_TO_RECOVER,
    REPLICA_RUNNING_STATUS_INTERRUPTED,
    REPLICA_RUNNING_STATUS_INVALID) = (
    '1', '23', '26', '33', '34', '35')

REPLICA_SECONDARY_ACCESS_RIGHTS = (
    REPLICA_SECONDARY_ACCESS_DENIED,
    REPLICA_SECONDARY_RO,
    REPLICA_SECONDARY_RW) = ('1', '2', '3')

METRO_RUNNING_STATUSES = (
    METRO_RUNNING_STATUS_NORMAL,
    METRO_RUNNING_STATUS_SYNCING,
    METRO_RUNNING_STATUS_INVALID,
    METRO_RUNNING_STATUS_PAUSE,
    METRO_RUNNING_STATUS_FORCED_START,
    METRO_RUNNING_STATUS_ERROR,
    METRO_RUNNING_STATUS_TO_BE_SYNC) = (
    '1', '23', '35', '41', '93', '94', '100')

HUAWEI_UNIFIED_DRIVER_REGISTRY = {
    'V3': 'manila.share.drivers.huawei.v3.connection.V3StorageConnection',
    'V5': 'manila.share.drivers.huawei.v3.connection.V3StorageConnection',
    'Dorado': 'manila.share.drivers.huawei.v3.connection.V3StorageConnection',
}

VALID_PRODUCT = ['V3', 'V5', 'Dorado']
