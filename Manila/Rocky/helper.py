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

import base64
import json
import requests
import six
import time

from oslo_log import log

from manila import exception
from manila.i18n import _
from manila.share.drivers.huawei import constants
from manila.share.drivers.huawei import huawei_utils
from manila import utils

LOG = log.getLogger(__name__)


def _error_code(result):
    return result['error']['code']


def _assert_result(result, format_str, *args):
    if _error_code(result) != 0:
        args += (result,)
        msg = (format_str + '\nresult: %s.') % args
        LOG.error(msg)
        raise exception.ShareBackendException(msg=msg)


class RestHelper(object):
    """Helper class for Huawei OceanStor V3 storage system."""

    def __init__(self, nas_address, nas_username, nas_password):
        self.nas_address = nas_address
        self.nas_username = nas_username
        self.nas_password = nas_password
        self.url = None
        self.session = None

        LOG.warning("Suppressing requests library SSL Warnings")
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning)
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecurePlatformWarning)

    def init_http_head(self):
        self.url = None
        self.session = requests.Session()
        self.session.headers.update({
            "Connection": "keep-alive",
            "Content-Type": "application/json"})
        self.session.verify = False

    def do_call(self, postfix_url, method, data=None,
                timeout=constants.SOCKET_TIMEOUT, **kwargs):
        url = self.url + postfix_url
        kwargs['timeout'] = timeout
        if data:
            kwargs['data'] = json.dumps(data)

        log_filter = kwargs.pop('log_filter', False)
        if not log_filter:
            LOG.info('Request URL: %(url)s\n'
                     'Call Method: %(method)s\n'
                     'Request Data: %(data)s',
                     {'url': url,
                      'method': method,
                      'data': data})

        func = getattr(self.session, method.lower())

        try:
            res = func(url, **kwargs)
        except Exception as exc:
            LOG.error('Bad response from server: %(url)s. '
                      'Error: %(err)s.',
                      {'url': url, 'err': six.text_type(exc)})
            return {"error": {"code": constants.ERROR_CONNECT_TO_SERVER,
                              "description": "Connect server error"}}

        try:
            res.raise_for_status()
        except requests.HTTPError as exc:
            return {"error": {"code": exc.response.status_code,
                              "description": six.text_type(exc)}}

        res_json = res.json()

        if not log_filter:
            LOG.info('Response Data: %s', res_json)
        return res_json

    def _get_user_info(self):
        if self.nas_username.startswith('!$$$'):
            username = base64.b64decode(self.nas_username[4:])
        else:
            username = self.nas_username

        if self.nas_password.startswith('!$$$'):
            password = base64.b64decode(self.nas_password[4:])
        else:
            password = self.nas_password

        return username, password

    def login(self):
        username, password = self._get_user_info()
        for item_url in self.nas_address:
            data = {"username": username,
                    "password": password,
                    "scope": "0"}
            self.init_http_head()
            self.url = item_url

            LOG.info('Try to login %s.', item_url)
            result = self.do_call(
                "xx/sessions", 'POST', data, constants.LOGIN_SOCKET_TIMEOUT,
                log_filter=True)
            if _error_code(result) != 0:
                LOG.error("Login %s failed, try another.", item_url)
                continue

            LOG.info('Login %s success.', item_url)
            self.url = item_url + result['data']['deviceid']
            self.session.headers['iBaseToken'] = result['data']['iBaseToken']
            if (result['data']['accountstate']
                    in constants.PWD_EXPIRED_OR_INITIAL):
                self.logout()
                msg = _("Password has expired or initial, "
                        "please change the password.")
                LOG.error(msg)
                raise exception.ShareBackendException(msg=msg)

            break
        else:
            msg = _("All url login fail.")
            LOG.error(msg)
            raise exception.ShareBackendException(msg=msg)

    def logout(self):
        url = "/sessions"
        if self.url:
            result = self.do_call(url, "DELETE")
            _assert_result(result, 'Logout session error.')

    @utils.synchronized('huawei_manila')
    def call(self, url, method, data=None, **kwargs):
        result = self.do_call(url, method, data, **kwargs)
        if _error_code(result) in (constants.ERROR_CONNECT_TO_SERVER,
                                   constants.ERROR_UNAUTHORIZED_TO_SERVER):
            LOG.error("Can't open the recent url, relogin.")
            self.login()
            result = self.do_call(url, method, data, **kwargs)
        return result

    def create_filesystem(self, fs_param):
        url = "/filesystem"
        result = self.call(url, 'POST', fs_param)
        _assert_result(result, 'Create filesystem %s error.', fs_param)
        return result['data']['ID']

    def create_share(self, share_name, fs_id, share_proto, vstore_id=None):
        share_path = huawei_utils.share_path(share_name)
        data = {
            "DESCRIPTION": share_name,
            "FSID": fs_id,
            "SHAREPATH": share_path,
        }

        if share_proto == 'NFS':
            url = "/NFSHARE"
        elif share_proto == 'CIFS':
            url = "/CIFSHARE"
            data["NAME"] = huawei_utils.share_name(share_name)
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        if vstore_id:
            data['vstoreId'] = vstore_id

        result = self.call(url, "POST", data)
        _assert_result(result, 'Create share for %s error.', share_name)
        return result['data']['ID']

    def update_share(self, share_id, share_proto, params, vstore_id=None):
        if share_proto == 'NFS':
            url = "/NFSHARE/%s" % share_id
        elif share_proto == 'CIFS':
            url = "/CIFSHARE/%s" % share_id
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        data = params
        if vstore_id:
            data['vstoreId'] = vstore_id
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Update share %s error.', share_id)

    def delete_share(self, share_id, share_proto, vstore_id=None):
        if share_proto == 'NFS':
            url = "/NFSHARE/%s" % share_id
        elif share_proto == 'CIFS':
            url = "/CIFSHARE/%s" % share_id
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        data = {'vstoreId': vstore_id} if vstore_id else None
        result = self.call(url, "DELETE", data)
        if _error_code(result) == constants.SHARE_NOT_EXIST:
            LOG.warning('Share %s to delete not exist.', share_id)
            return
        _assert_result(result, 'Delete share %s error.', share_id)

    def delete_filesystem(self, fs_id):
        url = "/filesystem/" + fs_id
        result = self.call(url, "DELETE")
        if _error_code(result) == constants.FILESYSTEM_NOT_EXIST:
            LOG.warning('FS %s to delete not exist.', fs_id)
            return
        _assert_result(result, 'Delete filesystem %s error.', fs_id)

    def get_all_pools(self):
        url = "/storagepool"
        result = self.call(url, "GET")
        _assert_result(result, "Query resource pool error.")
        return result['data']

    def get_pool_by_name(self, name, log_filter=False):
        url = "/storagepool?filter=NAME::%s" % name
        result = self.call(url, "GET", log_filter=log_filter)
        _assert_result(result, "Get pool %s error.", name)
        if 'data' in result and result['data']:
            return result['data'][0]

    def remove_access(self, access_id, share_proto, vstore_id=None):
        if share_proto == 'NFS':
            url = "/NFS_SHARE_AUTH_CLIENT/%s" % access_id
        elif share_proto == 'CIFS':
            url = "/CIFS_SHARE_AUTH_CLIENT/%s" % access_id
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        data = {'vstoreId': vstore_id} if vstore_id else None
        result = self.call(url, "DELETE", data)
        _assert_result(result, 'Delete access %s error.', access_id)

    def get_share_access(self, share_id, access_to, share_proto,
                         vstore_id=None):
        # Huawei array uses * to represent IP addresses of all clients
        if access_to == '0.0.0.0/0':
            access_to = '*'

        accesses = self.get_all_share_access(share_id, share_proto, vstore_id)
        for access in accesses:
            if access['NAME'] == access_to:
                return access

    def _get_share_access_count(self, share_id, share_proto, vstore_id=None):
        if share_proto == 'NFS':
            url = "/NFS_SHARE_AUTH_CLIENT"
        elif share_proto == 'CIFS':
            url = "/CIFS_SHARE_AUTH_CLIENT"
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        url += "/count?filter=PARENTID::%s" % share_id
        data = {'vstoreId': vstore_id} if vstore_id else None
        result = self.call(url, "GET", data)

        _assert_result(result, 'Get access count of share %s error.', share_id)
        return int(result['data']['COUNT'])

    def _get_share_access_by_range(self, share_id, share_proto, range, vstore_id=None):
        if share_proto == 'NFS':
            url = "/NFS_SHARE_AUTH_CLIENT"
        elif share_proto == 'CIFS':
            url = "/CIFS_SHARE_AUTH_CLIENT"
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        url += "?filter=PARENTID::%s" % share_id
        url += "&range=[%s-%s]" % range
        data = {'vstoreId': vstore_id} if vstore_id else None
        result = self.call(url, "GET", data)
        _assert_result(result, 'Get accesses of share %s error.', share_id)
        return result.get('data', [])

    def get_all_share_access(self, share_id, share_proto, vstore_id=None):
        count = self._get_share_access_count(share_id, share_proto, vstore_id)
        if count <= 0:
            return []

        all_accesses = []
        for i in range(count // 100 + 1):
            query_range = i * 100, (i + 1) * 100
            accesses = self._get_share_access_by_range(
                share_id, share_proto, query_range, vstore_id)
            all_accesses.extend(accesses)

        return all_accesses

    def change_access(self, access_id, share_proto, access_level,
                      vstore_id=None):
        if share_proto == 'NFS':
            url = "/NFS_SHARE_AUTH_CLIENT/%s" % access_id
            access = {"ACCESSVAL": access_level}
        elif share_proto == 'CIFS':
            url = "/CIFS_SHARE_AUTH_CLIENT/" + access_id
            access = {"PERMISSION": access_level}
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        if vstore_id:
            access['vstoreId'] = vstore_id
        result = self.call(url, "PUT", access)
        _assert_result(result, 'Change access %s level to %s error.',
                       access_id, access_level)

    def allow_access(self, share_id, access_to, share_proto, access_level,
                     share_type_id=None, vstore_id=None):
        if share_proto == 'NFS':
            self._allow_nfs_access(
                share_id, access_to, access_level, share_type_id, vstore_id)
        elif share_proto == 'CIFS':
            self._allow_cifs_access(
                share_id, access_to, access_level, vstore_id)
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

    def _allow_nfs_access(self, share_id, access_to, access_level,
                          share_type_id, vstore_id):
        # Huawei array uses * to represent IP addresses of all clients
        if access_to == '0.0.0.0/0':
            access_to = '*'
        access = {
            "NAME": access_to,
            "PARENTID": share_id,
            "ACCESSVAL": access_level,
            "SYNC": "0",
            "ALLSQUASH": "1",
            "ROOTSQUASH": "0",
        }

        if share_type_id:
            opts = huawei_utils.get_share_privilege(share_type_id)
            access.update(opts)

        if vstore_id:
            access['vstoreId'] = vstore_id

        result = self.call("/NFS_SHARE_AUTH_CLIENT", "POST", access)
        _assert_result(result, 'Allow NFS access %s error.', access)

    def _allow_cifs_access(self, share_id, access_to, access_level, vstore_id):
        access = {
            "NAME": access_to,
            "PARENTID": share_id,
            "PERMISSION": access_level,
            "DOMAINTYPE": '2' if '\\' not in access_to else '0',
        }
        if vstore_id:
            access['vstoreId'] = vstore_id

        result = self.call("/CIFS_SHARE_AUTH_CLIENT", "POST", access)
        if _error_code(result) == constants.ERROR_USER_OR_GROUP_NOT_EXIST:
            # If add user access failed, try to add group access.
            access['NAME'] = '@' + access_to
            result = self.call("/CIFS_SHARE_AUTH_CLIENT", "POST", access)
        _assert_result(result, 'Allow CIFS access %s error.', access)

    def get_snapshot_by_id(self, snap_id):
        url = "/FSSNAPSHOT/" + snap_id
        result = self.call(url, "GET")
        _assert_result(result, 'Get snapshot by id %s error.', snap_id)
        return result['data']

    def delete_snapshot(self, snap_id):
        url = "/FSSNAPSHOT/%s" % snap_id
        result = self.call(url, "DELETE")
        if _error_code(result) == constants.SNAPSHOT_NOT_EXIST:
            LOG.warning('Snapshot %s to delete not exist.', snap_id)
            return
        _assert_result(result, 'Delete snapshot %s error.', snap_id)

    def create_snapshot(self, fs_id, snapshot_name):
        data = {
            "PARENTTYPE": "40",
            "PARENTID": fs_id,
            "NAME": huawei_utils.snapshot_name(snapshot_name),
        }
        result = self.call("/FSSNAPSHOT", "POST", data)
        _assert_result(result, 'Create snapshot %s error.', data)
        return result['data']['ID']

    def get_share_by_name(self, share_name, share_proto, vstore_id=None):
        if share_proto == 'NFS':
            share_path = huawei_utils.share_path(share_name)
            url = "/NFSHARE?filter=SHAREPATH::%s&range=[0-100]" % share_path
        elif share_proto == 'CIFS':
            cifs_share = huawei_utils.share_name(share_name)
            url = "/CIFSHARE?filter=NAME:%s&range=[0-100]" % cifs_share
        else:
            msg = _('Invalid NAS protocol %s.') % share_proto
            raise exception.InvalidInput(reason=msg)

        data = {'vstoreId': vstore_id} if vstore_id else None
        result = self.call(url, "GET", data)
        if _error_code(result) == constants.SHARE_PATH_INVALID:
            LOG.warning('Share %s not exist.', share_name)
            return

        _assert_result(result, 'Get share by name %s error.', share_name)

        # for CIFS, if didn't get share by NAME, try DESCRIPTION
        if share_proto == 'CIFS' and not result.get('data'):
            url = "/CIFSHARE?filter=DESCRIPTION:%s&range=[0-100]" % share_name
            result = self.call(url, "GET", data)

        if result.get('data'):
            return result['data'][0]

    def get_fs_info_by_name(self, name):
        url = "/filesystem?filter=NAME::%s" % huawei_utils.share_name(name)
        result = self.call(url, "GET")
        _assert_result(result, 'Get filesystem by name %s error.', name)
        if 'data' in result and result['data']:
            return result['data'][0]

    def get_fs_info_by_id(self, fs_id):
        url = "/filesystem/%s" % fs_id
        result = self.call(url, "GET")
        _assert_result(result, "Get filesystem by id %s error.", fs_id)
        return result['data']

    def update_filesystem(self, fs_id, params):
        url = "/filesystem/%s" % fs_id
        result = self.call(url, "PUT", params)
        _assert_result(result, "Update filesystem %s by %s error.",
                       fs_id, params)

    def get_partition_id_by_name(self, name):
        url = "/cachepartition?filter=NAME::%s" % name
        result = self.call(url, "GET")
        _assert_result(result, 'Get partition by name %s error.', name)
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def get_partition_info_by_id(self, partitionid):
        url = '/cachepartition/' + partitionid
        result = self.call(url, "GET")
        _assert_result(result, 'Get partition by id %s error.', partitionid)
        return result['data']

    def add_fs_to_partition(self, fs_id, partition_id):
        url = "/smartPartition/addFs"
        data = {"ID": partition_id,
                "ASSOCIATEOBJTYPE": 40,
                "ASSOCIATEOBJID": fs_id,
                }
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Add FS %s to partition %s error.',
                       fs_id, partition_id)

    def remove_fs_from_partition(self, fs_id, partition_id):
        url = "/smartPartition/removeFs"
        data = {"ID": partition_id,
                "ASSOCIATEOBJTYPE": 40,
                "ASSOCIATEOBJID": fs_id,
                }
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Remove FS %s from partition %s error.',
                       fs_id, partition_id)

    def rename_snapshot(self, snapshot_id, new_name):
        url = "/FSSNAPSHOT/%s" % snapshot_id
        data = {"NAME": huawei_utils.snapshot_name(new_name)}
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Rename snapshot %s error.', snapshot_id)

    def get_cache_id_by_name(self, name):
        url = "/SMARTCACHEPARTITION?filter=NAME::%s" % name
        result = self.call(url, "GET")
        _assert_result(result, 'Get cache by name %s error.', name)
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def get_cache_info_by_id(self, cacheid):
        url = "/SMARTCACHEPARTITION/" + cacheid
        result = self.call(url, "GET")
        _assert_result(result, 'Get smartcache by id %s error.', cacheid)
        return result['data']

    def add_fs_to_cache(self, fs_id, cache_id):
        url = "/SMARTCACHEPARTITION/CREATE_ASSOCIATE"
        data = {"ID": cache_id,
                "ASSOCIATEOBJTYPE": 40,
                "ASSOCIATEOBJID": fs_id,
                }
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Add FS %s to cache %s error.',
                       fs_id, cache_id)

    def remove_fs_from_cache(self, fs_id, cache_id):
        url = "/SMARTCACHEPARTITION/REMOVE_ASSOCIATE"
        data = {"ID": cache_id,
                "ASSOCIATEOBJTYPE": 40,
                "ASSOCIATEOBJID": fs_id,
                }
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Remove FS %s from cache %s error.',
                       fs_id, cache_id)

    def get_all_qos(self):
        url = "/ioclass"
        result = self.call(url, "GET")
        _assert_result(result, 'Get all QoS error.')
        return result.get('data', [])

    def update_qos_fs(self, qos_id, new_fs_list):
        url = "/ioclass/%s" % qos_id
        data = {"FSLIST": new_fs_list}
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Associate FS %s to Qos %s error.',
                       new_fs_list, qos_id)

    def create_qos(self, qos, fs_id):
        localtime = time.strftime('%Y%m%d%H%M%S', time.localtime())
        qos_name = constants.QOS_NAME_PREFIX + fs_id + '_' + localtime
        data = {"NAME": qos_name,
                "FSLIST": [fs_id],
                "CLASSTYPE": "1",
                "SCHEDULEPOLICY": "1",
                "SCHEDULESTARTTIME": "1410969600",
                "STARTTIME": "00:00",
                "DURATION": "86400",
                }
        data.update(qos)
        result = self.call("/ioclass", 'POST', data)
        _assert_result(result, 'Create QoS %s error.', data)
        return result['data']['ID']

    def activate_deactivate_qos(self, qos_id, enable_status):
        url = "/ioclass/active"
        data = {"ID": qos_id,
                "ENABLESTATUS": enable_status}
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Activate or deactivate QoS %s error.', qos_id)

    def delete_qos(self, qos_id):
        url = "/ioclass/%s" % qos_id
        result = self.call(url, 'DELETE')
        _assert_result(result, 'Delete QoS %s error.', qos_id)

    def get_qos_info(self, qos_id):
        url = "/ioclass/%s" % qos_id
        result = self.call(url, "GET")
        _assert_result(result, 'Get QoS info by id %s error.', qos_id)
        return result['data']

    def get_all_eth_port(self):
        result = self.call("/eth_port", 'GET')
        _assert_result(result, 'Get all eth port error.')
        return result.get('data', [])

    def get_all_bond_port(self):
        result = self.call("/bond_port", 'GET')
        _assert_result(result, 'Get all bond port error.')
        return result.get('data', [])

    def get_all_vlan(self):
        result = self.call("/vlan", 'GET')
        _assert_result(result, 'Get all vlan error.')
        return result.get('data', [])

    def get_vlan_by_tag(self, vlan_tag):
        url = "/vlan?filter=TAG::%s" % vlan_tag
        result = self.call(url, 'GET')
        _assert_result(result, 'Get vlan by tag %s error.', vlan_tag)
        return result.get('data', [])

    def create_vlan(self, port_id, port_type, vlan_tag):
        data = {"PORTID": port_id,
                "PORTTYPE": port_type,
                "TAG": vlan_tag,
                }
        result = self.call("/vlan", "POST", data)
        _assert_result(result, 'Create vlan %s error.', data)
        return result['data']['ID']

    def delete_vlan(self, vlan_id):
        url = "/vlan/%s" % vlan_id
        result = self.call(url, 'DELETE')
        if _error_code(result) == constants.OBJECT_NOT_EXIST:
            LOG.warning('vlan %s to delete not exist.', vlan_id)
            return
        _assert_result(result, 'Delete vlan %s error.', vlan_id)

    def get_logical_port_by_ip(self, ip, ip_type):
        if ip_type == 4:
            url = "/LIF?filter=IPV4ADDR::%s" % ip
        else:
            url = "/LIF?filter=IPV6ADDR::%s" % ip
        result = self.call(url, 'GET')
        _assert_result(result, 'Get logical port by IP %s error.', ip)
        if 'data' in result and result['data']:
            return result['data'][0]

    def create_logical_port(self, params):
        result = self.call("/LIF", 'POST', params)
        _assert_result(result, 'Create logical port %s error.', params)
        return result['data']['ID']

    def get_all_logical_port(self):
        result = self.call("/LIF", 'GET')
        _assert_result(result, 'Get all logical port error.')
        return result.get('data', [])

    def delete_logical_port(self, logical_port_id):
        url = "/LIF/%s" % logical_port_id
        result = self.call(url, 'DELETE')
        if _error_code(result) == constants.OBJECT_NOT_EXIST:
            LOG.warning('Logical port %s to delete not exist.',
                        logical_port_id)
            return
        _assert_result(result, 'Delete logical port %s error.',
                       logical_port_id)

    def set_dns_ip_address(self, dns_ip_list):
        if len(dns_ip_list) > 3:
            msg = _('3 IPs can be set to DNS most.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        dns_info = {"ADDRESS": json.dumps(dns_ip_list)}
        result = self.call("/DNS_Server", 'PUT', dns_info)
        _assert_result(result, 'Set DNS ip address %s error.', dns_ip_list)

    def get_dns_ip_address(self):
        result = self.call("/DNS_Server", 'GET')
        _assert_result(result, 'Get DNS ip address error.')
        if 'data' in result:
            return json.loads(result['data']['ADDRESS'])

    def add_ad_config(self, user, password, domain):
        info = {"DOMAINSTATUS": 1,
                "ADMINNAME": user,
                "ADMINPWD": password,
                "FULLDOMAINNAME": domain,
                }
        result = self.call("/AD_CONFIG", 'PUT', info)
        _assert_result(result, 'Add AD config %s error.', info)

    def delete_ad_config(self, user, password):
        info = {"DOMAINSTATUS": 0,
                "ADMINNAME": user,
                "ADMINPWD": password,
                }
        result = self.call("/AD_CONFIG", 'PUT', info)
        _assert_result(result, 'Delete AD config %s error.', info)

    def get_ad_config(self):
        result = self.call("/AD_CONFIG", 'GET')
        _assert_result(result, 'Get AD config error.')
        return result.get('data')

    def add_ldap_config(self, server, domain):
        info = {"BASEDN": domain,
                "LDAPSERVER": server,
                "PORTNUM": 389,
                "TRANSFERTYPE": "1",
                }
        result = self.call("/LDAP_CONFIG", 'PUT', info)
        _assert_result(result, 'Add LDAP config %s error.', info)

    def delete_ldap_config(self):
        result = self.call("/LDAP_CONFIG", 'DELETE')
        if _error_code(result) == constants.AD_DOMAIN_NOT_EXIST:
            LOG.warning('LDAP config not exist while deleting.')
            return
        _assert_result(result, 'Delete LDAP config error.')

    def get_ldap_config(self):
        result = self.call("/LDAP_CONFIG", 'GET')
        _assert_result(result, 'Get LDAP config error.')
        return result.get('data')

    def get_array_wwn(self):
        result = self.call("/system/", "GET")
        _assert_result(result, 'Get array info error.')
        return result['data']['wwn']

    def get_remote_device_by_wwn(self, wwn):
        result = self.call("/remote_device", "GET")
        _assert_result(result, 'Get all remote devices error.')
        for device in result.get('data', []):
            if device.get('WWN') == wwn:
                return device

    def create_replication_pair(self, params):
        result = self.call("/REPLICATIONPAIR", "POST", params)
        _assert_result(result, 'Create replication pair %s error.', params)
        return result['data']

    def split_replication_pair(self, pair_id):
        data = {"ID": pair_id}
        result = self.call('/REPLICATIONPAIR/split', "PUT", data)
        _assert_result(result, 'Split replication pair %s error.', pair_id)

    def switch_replication_pair(self, pair_id):
        data = {"ID": pair_id}
        result = self.call('/REPLICATIONPAIR/switch', "PUT", data)
        _assert_result(result, 'Switch replication pair %s error.', pair_id)

    def delete_replication_pair(self, pair_id):
        url = "/REPLICATIONPAIR/%s" % pair_id
        result = self.call(url, "DELETE")
        if _error_code(result) == constants.REPLICATION_PAIR_NOT_EXIST:
            LOG.warning('Replication pair %s to delete not exist.', pair_id)
            return
        _assert_result(result, 'Delete replication pair %s error.', pair_id)

    def sync_replication_pair(self, pair_id):
        data = {"ID": pair_id}
        result = self.call("/REPLICATIONPAIR/sync", "PUT", data)
        _assert_result(result, 'Sync replication pair %s error.', pair_id)

    def cancel_pair_secondary_write_lock(self, pair_id):
        url = "/REPLICATIONPAIR/CANCEL_SECODARY_WRITE_LOCK"
        data = {"ID": pair_id}
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Cancel replication pair %s secondary '
                               'write lock error.', pair_id)

    def set_pair_secondary_write_lock(self, pair_id):
        url = "/REPLICATIONPAIR/SET_SECODARY_WRITE_LOCK"
        data = {"ID": pair_id}
        result = self.call(url, "PUT", data)
        _assert_result(result, 'Set replication pair %s secondary '
                               'write lock error.', pair_id)

    def get_replication_pair_by_id(self, pair_id):
        url = "/REPLICATIONPAIR/%s" % pair_id
        result = self.call(url, "GET")
        _assert_result(result, 'Get replication pair by id %s error.', pair_id)
        return result.get('data', {})

    def get_replication_pair_by_localres_name(self, local_res):
        url = "/REPLICATIONPAIR?filter=LOCALRESNAME::" + local_res
        result = self.call(url, "GET")
        _assert_result(result, 'Get replication pair by local resource '
                               'name %s error.', local_res)
        return result.get('data')

    def get_feature_status(self):
        result = self.call('/license/feature', 'GET', log_filter=True)
        if result['error']['code'] != 0:
            LOG.warning('Query feature information failed.')
            return {}

        status = {}
        for feature in result.get('data', []):
            status.update(feature)

        return status

    def get_vstore_pair(self, pair_id):
        url = '/vstore_pair/%s' % pair_id
        result = self.call(url, 'GET')
        _assert_result(result, 'Get vstore pair info %s error.', pair_id)
        return result['data']

    def rollback_snapshot(self, snap_id):
        data = {"ID": snap_id}
        result = self.call("/FSSNAPSHOT/ROLLBACK_FSSNAPSHOT", "PUT", data)
        _assert_result(result, 'Failed to rollback snapshot %s.', snap_id)

    def get_controller_id(self, controller_name):
        result = self.call('/controller', 'GET')
        _assert_result(result, 'Get controllers error.')

        for con in result.get('data', []):
            if con.get('LOCATION') == controller_name:
                return con['ID']

    def split_clone_fs(self, fs_id):
        data = {"ID": fs_id,
                "SPLITENABLE": True,
                "SPLITSPEED": 4,
                }
        result = self.call("/filesystem_split_switch", "PUT", data)
        _assert_result(result, 'Split clone fs %s error.', fs_id)
