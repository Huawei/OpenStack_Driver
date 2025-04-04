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

STATUS_HEALTH = '1'
STATUS_ACTIVE = '43'
STATUS_RUNNING = '10'
STATUS_VOLUME_READY = '27'
STATUS_LUNCOPY_READY = '40'
STATUS_QOS_ACTIVE = '2'
STATUS_QOS_INACTIVATED = '45'
STATUS_SNAPSHOT_INACTIVE = '45'
STATUS_SNAPSHOT_ACTIVE = '43'
LUN_TYPE = '11'
SNAPSHOT_TYPE = '27'

BLOCK_STORAGE_POOL_TYPE = '1'
DORADO_V6_POOL_TYPE = '0'
FILE_SYSTEM_POOL_TYPE = '2'

HOSTGROUP_PREFIX = 'OpenStack_HostGroup_'
LUNGROUP_PREFIX = 'OpenStack_LunGroup_'
MAPPING_VIEW_PREFIX = 'OpenStack_Mapping_View_'
PORTGROUP_PREFIX = 'OpenStack_PortGroup_'
QOS_NAME_PREFIX = 'OpenStack_'
SENSITIVE_KEYS = ['auth_password']
SENSITIVE_INI_KEYS = ['initiator', 'wwpns', 'host_nqn', 'wwnns', 'nqn']

PORTGROUP_DESCRIP_PREFIX = "Please do NOT modify this. Engine ID: "
FC_PORT_CONNECTED = '10'
FC_INIT_ONLINE = '27'
PARENT_TYPE_HOST = 21
FC_PORT_MODE_FABRIC = '0'
CAPACITY_UNIT = 1024 * 1024 * 2
DEFAULT_WAIT_TIMEOUT = 3600 * 24 * 30
DEFAULT_WAIT_INTERVAL = 5

MIGRATION_WAIT_INTERVAL = 5
MIGRATION_FAULT = '74'
MIGRATION_COMPLETE = '76'

# ROCE INITIATOR CONSTANTS
NVME_ROCE_INITIATOR_TYPE = '57870'
ADDRESS_FAMILY_IPV4 = '0'

ERROR_CONNECT_TO_SERVER = -403
ERROR_UNAUTHORIZED_TO_SERVER = -401
ERROR_BAD_STATUS_LINE = -400
ERROR_DEVICE_COMMUNICATE = 4294967297
HTTP_ERROR_NOT_FOUND = 404
SOCKET_TIMEOUT = 52
ERROR_VOLUME_ALREADY_EXIST = 1077948993
LOGIN_SOCKET_TIMEOUT = 32
DEFAULT_SEMAPHORE = 20
ERROR_VOLUME_NOT_EXIST = 1077939726
ERROR_LUN_NOT_EXIST = 1077936859
ERROR_SNAPSHOT_NOT_EXIST = 1077937880
FC_INITIATOR_NOT_EXIST = 1077948996
INITIATOR_NOT_EXIST = 1077948996
HYPERMETROPAIR_NOT_EXIST = 1077674242
REPLICATIONPAIR_NOT_EXIST = 1077937923
REPLICG_IS_EMPTY = 1077937960
ERROR_VOLUME_TIMEOUT = 1077949001
ERROR_PARAMETER_ERROR = 50331651
GET_VOLUME_WAIT_INTERVAL = 30
CREATE_HYPERMETRO_TIMEOUT = 1077949006
HYPERMETRO_ALREADY_EXIST = 1077674256
CLONE_PAIR_SYNC_COMPLETE = 1073798176
CLONE_PAIR_SYNC_NOT_EXIST = 1073798172
HOST_ALREADY_IN_HOSTGROUP = 1077937501
OBJECT_ALREADY_EXIST = 1077948997
LUN_ALREADY_IN_LUNGROUP = 1077948997
HOSTGROUP_ALREADY_IN_MAPPINGVIEW = 1073804556
LUNGROUP_ALREADY_IN_MAPPINGVIEW = 1073804560
HOST_NOT_EXIST = 1077937498

RELOGIN_ERROR_PASS = [ERROR_VOLUME_NOT_EXIST]
RELOGIN_ERROR_CODE = (
    ERROR_CONNECT_TO_SERVER, ERROR_UNAUTHORIZED_TO_SERVER,
    ERROR_BAD_STATUS_LINE, ERROR_DEVICE_COMMUNICATE
)
RUNNING_NORMAL = '1'
RUNNING_SYNC = '23'
RUNNING_STOP = '41'
RUNNING_TO_BE_SYNC = '100'
METRO_SYNC_NORMAL = (RUNNING_NORMAL, RUNNING_SYNC, RUNNING_TO_BE_SYNC)
HEALTH_NORMAL = '1'

NO_SPLITMIRROR_LICENSE = 1077950233
NO_MIGRATION_LICENSE = 1073806606

THICK_LUNTYPE = 0
THIN_LUNTYPE = 1
MAX_NAME_LENGTH = 31
MAX_VOL_DESCRIPTION = 170
PORT_NUM_PER_CONTR = 2
MAX_QUERY_COUNT = 100

OS_TYPE = {
    'Linux': '0',
    'Windows': '1',
    'Solaris': '2',
    'HP-UX': '3',
    'AIX': '4',
    'XenServer': '5',
    'Mac OS X': '6',
    'VMware ESX': '7'
}

LOWER_LIMIT_KEYS = ['MINIOPS', 'LATENCY', 'MINBANDWIDTH']
UPPER_LIMIT_KEYS = ['MAXIOPS', 'MAXBANDWIDTH']
PWD_EXPIRED_OR_INITIAL = (3, 4)

DEFAULT_REPLICA_WAIT_INTERVAL = 1
DEFAULT_REPLICA_WAIT_TIMEOUT = 20

REPLICA_SYNC_MODEL = '1'
REPLICA_ASYNC_MODEL = '2'
REPLICA_SPEED = '2'
REPLICA_PERIOD = '3600'
REPLICA_SECOND_RO = '2'
REPLICA_SECOND_RW = '3'
REPLICG_PERIOD = '60'

REPLICA_RUNNING_STATUS_KEY = 'RUNNINGSTATUS'
REPLICA_RUNNING_STATUS_INITIAL_SYNC = '21'
REPLICA_RUNNING_STATUS_SYNC = '23'
REPLICA_RUNNING_STATUS_SYNCED = '24'
REPLICA_RUNNING_STATUS_NORMAL = '1'
REPLICA_RUNNING_STATUS_SPLIT = '26'
REPLICA_RUNNING_STATUS_ERRUPTED = '34'
REPLICA_RUNNING_STATUS_INVALID = '35'

REPLICA_HEALTH_STATUS_KEY = 'HEALTHSTATUS'
REPLICA_HEALTH_STATUS_NORMAL = '1'

REPLICA_LOCAL_DATA_STATUS_KEY = 'PRIRESDATASTATUS'
REPLICA_REMOTE_DATA_STATUS_KEY = 'SECRESDATASTATUS'
REPLICA_DATA_SYNC_KEY = 'ISDATASYNC'
REPLICA_DATA_STATUS_SYNCED = '1'
REPLICA_DATA_STATUS_COMPLETE = '2'
REPLICA_DATA_STATUS_INCOMPLETE = '3'

SNAPSHOT_NOT_EXISTS_WARN = 'warning'
SNAPSHOT_NOT_EXISTS_RAISE = 'raise'

LUN_TYPE_MAP = {'Thick': THICK_LUNTYPE, 'Thin': THIN_LUNTYPE}

VALID_PRODUCT = ['V3', 'V5', '18000', 'Dorado', 'V6']
VALID_PROTOCOL = ['FC', 'iSCSI', 'nvmeof']
VALID_WRITE_TYPE = ['1', '2']
VOLUME_NOT_EXISTS_WARN = 'warning'
VOLUME_NOT_EXISTS_RAISE = 'raise'
DORADO_V6_AND_V6_PRODUCT = ('Dorado', 'V6')

LUN_COPY_SPEED_TYPES = (
    LUN_COPY_SPEED_LOW,
    LUN_COPY_SPEED_MEDIUM,
    LUN_COPY_SPEED_HIGH,
    LUN_COPY_SPEED_HIGHEST
) = ('1', '2', '3', '4')

HYPER_SYNC_SPEED_TYPES = (
    HYPER_SYNC_SPEED_LOW,
    HYPER_SYNC_SPEED_MEDIUM,
    HYPER_SYNC_SPEED_HIGH,
    HYPER_SYNC_SPEED_HIGHEST
) = ('1', '2', '3', '4')

REPLICA_SYNC_SPEED_TYPES = (
    REPLICA_SYNC_SPEED_LOW,
    REPLICA_SYNC_SPEED_MEDIUM,
    REPLICA_SYNC_SPEED_HIGH,
    REPLICA_SYNC_SPEED_HIGHEST
) = ('1', '2', '3', '4')

REPLICG_STATUS_NORMAL = '1'
REPLICG_STATUS_SYNCING = '23'
REPLICG_STATUS_TO_BE_RECOVERD = '33'
REPLICG_STATUS_INTERRUPTED = '34'
REPLICG_STATUS_SPLITED = '26'
REPLICG_STATUS_INVALID = '35'
REPLICG_HEALTH_NORMAL = '1'

OPTIMAL_MULTIPATH_NUM = 16

AVAILABLE_FEATURE_STATUS = (1, 2)
DEDUP_FEATURES = (
    'SmartDedupe (for LUN)',
    'SmartDedupe (for LUNsAndFS)',
    'SmartDedupe & SmartCompression (for LUN)',
    'Effective Capacity'
)
COMPRESSION_FEATURES = (
    'SmartCompression (for LUN)',
    'SmartCompression (for LUNsAndFS)',
    'SmartDedupe & SmartCompression (for LUN)',
    'Effective Capacity'
)

FEATURES_DICTS = {
    "SmartPartition": "cachepartition",
    "SmartCache": "smartcachepartition",
    "SmartQoS": "ioclass",
    "HyperCopy": "LUNCOPY",
    "SmartThin": None,
    "HyperMetro": "HyperMetroPair",
}

DEFAULT_CLONE_MODE = "luncopy"

HYPERMETRO_WAIT_INTERVAL = 5
CLONE_STATUS_HEALTH = '0'
CLONE_STATUS_COMPLETE = (CLONE_COMPLETE,) = ('2',)
CLONE_PAIR_NOT_EXIST = "1073798147"
SUPPORT_CLONE_PAIR_VERSION = "V600R003C00"

DEFAULT_MINIMUM_FC_INITIATOR_ONLINE = 0

SNAPSHOT_HEALTH_STATUS = (
    SNAPSHOT_HEALTH_STATUS_NORMAL,
    SNAPSHOT_HEALTH_STATUS_FAULTY) = ('1', '2')
SNAPSHOT_RUNNING_STATUS = (
    SNAPSHOT_RUNNING_STATUS_ACTIVATED,
    SNAPSHOT_RUNNING_STATUS_ROLLINGBACK) = ('43', '44')
SNAPSHOT_ROLLBACK_PROGRESS_FINISH = '100'
SNAPSHOT_ROLLBACK_SPEED_TYPES = (
    SNAPSHOT_ROLLBACK_SPEED_LOW,
    SNAPSHOT_ROLLBACK_SPEED_MEDIUM,
    SNAPSHOT_ROLLBACK_SPEED_HIGH,
    SNAPSHOT_ROLLBACK_SPEED_HIGHEST
) = ('1', '2', '3', '4')

# Duplicate Field Summary
TRUE = 'true'
POLICY = 'policy'
CACHENAME = 'cachename'
PARTITIONNAME = 'partitionname'
DATA = 'data'
TARGETIP = 'TargetIP'
HOSTNAME = 'HostName'
TARGETPORTGROUP = 'TargetPortGroup'
IPV4ADDR = 'IPV4ADDR'
IPV6ADDR = 'IPV6ADDR'
ALUA = 'ALUA'
CHAPINFO = 'CHAPinfo'
NAME = 'name'
LUNCONFIGEDCAPACITY = 'LUNCONFIGEDCAPACITY'
ERROR = 'error'
CODE = 'code'
DESCRIPTION = 'description'
REPLICATION_TYPE = 'replication_type'
WORKLOADTYPEID = 'WORKLOADTYPEID'
RUNNINGSTATUS = 'RUNNINGSTATUS'
ID = 'id'
ID_UPPER = 'ID'
HUAWEI_LUN_ID = 'huawei_lun_id'
TARGET_WWN = 'target_wwn'
TARGET_IQNS = 'target_iqns'
TARGET_PORTALS = 'target_portals'
TARGET_LUNS = 'target_luns'
HOST = 'host'
OLD_STATUS = 'old_status'
STATUS = 'status'
NEWSIZE = 'newsize'
CAPABILITIES = 'capabilities'
FASTCLONE = 'fastclone'
APPLICATION_TYPE = 'application_type'
DEDUP = 'dedup'
COMPRESSION = 'compression'
HYPERMETRO = 'hypermetro'
THICK_PROVISIONING_SUPPORT = 'thick_provisioning_support'
HUAWEI_APPLICATION_TYPE = 'huawei_application_type'
SMARTTIER = 'smarttier'
ISCSI_DEFAULT_TARGET_IP = 'iscsi_default_target_ip'
IN_BAND_OR_NOT = 'in_band_or_not'
METRO_SYNC_COMPLETED = 'metro_sync_completed'
THIN = 'Thin'
INITIATOR_TARGET_MAP = 'initiator_target_map'
URL = 'url'
