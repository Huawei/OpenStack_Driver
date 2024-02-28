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

import json
import netaddr
import re
import requests
import six
import threading
import time

from oslo_concurrency import lockutils
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import strutils
from requests.adapters import HTTPAdapter

from cinder import exception
from cinder.i18n import _
from cinder import utils
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import huawei_utils

LOG = logging.getLogger(__name__)


class HostNameIgnoringAdapter(HTTPAdapter):
    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = False
        return super(HostNameIgnoringAdapter, self).cert_verify(
            conn, url, verify, cert)


class RestClient(object):
    """Common class for Huawei OceanStor storage system."""

    def __init__(self, configuration, san_address, san_user, san_password,
                 **kwargs):
        self.configuration = configuration
        self.san_address = san_address
        self.san_user = san_user
        self.san_password = san_password
        self.vstore_name = kwargs.get('vstore_name', None)
        self.storage_pools = kwargs.get('storage_pools',
                                        self.configuration.storage_pools)
        self.iscsi_info = kwargs.get('iscsi_info',
                                     self.configuration.iscsi_info)
        self.fc_info = kwargs.get('fc_info', self.configuration.fc_info)
        self.roce_info = kwargs.get('roce_info', self.configuration.roce_info)
        self.iscsi_default_target_ip = kwargs.get(
            'iscsi_default_target_ip',
            self.configuration.iscsi_default_target_ip)
        self.metro_domain = kwargs.get('metro_domain', None)
        self.metro_sync_completed = strutils.bool_from_string(
            kwargs.get('metro_sync_completed'))
        self.semaphore = threading.Semaphore(20)
        self.call_lock = lockutils.ReaderWriterLock()
        self.session = None
        self.url = None
        self.ssl_cert_verify = self.configuration.ssl_cert_verify
        self.ssl_cert_path = self.configuration.ssl_cert_path
        self.in_band_or_not = kwargs.get('in_band_or_not',
                                         self.configuration.in_band_or_not)
        self.storage_sn = kwargs.get('storage_sn',
                                     self.configuration.storage_sn)
        self.is_dorado_v6 = False

        if self.in_band_or_not and not self.storage_sn:
            msg = _("'Storagesn' is must be set if 'InBandOrNot' is True,"
                    " Please Check Your config")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not self.ssl_cert_verify and hasattr(requests, 'packages'):
            LOG.warning("Suppressing requests library SSL Warnings")
            requests.packages.urllib3.disable_warnings(
                requests.packages.urllib3.exceptions.InsecureRequestWarning)
            requests.packages.urllib3.disable_warnings(
                requests.packages.urllib3.exceptions.InsecurePlatformWarning)

    def init_http_head(self):
        self.url = None
        self.session = requests.Session()
        session_headers = {
            "Connection": "keep-alive",
            "Content-Type": "application/json"}
        if self.in_band_or_not:
            session_headers["IBA-Target-Array"] = self.storage_sn
        self.session.headers.update(session_headers)

        self.session.verify = self.ssl_cert_path if self.ssl_cert_verify else False

    def do_call(self, url=None, data=None, method=None,
                calltimeout=constants.SOCKET_TIMEOUT, filter_flag=False):
        """Send requests to Huawei storage server.

        Send HTTPS call, get response in JSON.
        Convert response into Python Object and return it.
        """
        if self.url:
            url = self.url + url

        kwargs = {'timeout': calltimeout}
        if data:
            kwargs['data'] = json.dumps(data)

        if method in (None, 'POST'):
            func = self.session.post
        elif method in ('PUT',):
            func = self.session.put
        elif method in ('GET',):
            func = self.session.get
        elif method in ('DELETE',):
            func = self.session.delete
        else:
            msg = _("Request method %s is invalid.") % method
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self.semaphore.acquire()

        try:
            res = func(url, **kwargs)
        except Exception as exc:
            if "BadStatusLine" in six.text_type(exc):
                return {"error": {
                    "code": constants.ERROR_BAD_STATUS_LINE,
                    "description": "BadStatusLine."}}
            LOG.exception('Bad response from server: %(url)s.'
                          ' Error: %(err)s',
                          {'url': url, 'err': six.text_type(exc)})
            return {"error": {"code": constants.ERROR_CONNECT_TO_SERVER,
                              "description": "Connect to server error."}
                    }
        finally:
            self.semaphore.release()

        try:
            res.raise_for_status()
        except requests.HTTPError as exc:
            return {"error": {"code": exc.response.status_code,
                              "description": six.text_type(exc)}
                    }

        res_json = res.json()
        if not filter_flag:
            LOG.info('\nRequest URL: %(url)s\n'
                     'Call Method: %(method)s\n'
                     'Request Data: %(data)s\n'
                     'Response Data:%(res)s',
                     {'url': url,
                      'method': method,
                      'data': data,
                      'res': res_json})

        return res_json

    def login(self):
        """Login Huawei storage array."""
        device_id = None
        for item_url in self.san_address:
            url = item_url + "xx/sessions"
            data = {"username": self.san_user,
                    "password": self.san_password,
                    "scope": "0"}
            if self.vstore_name:
                data['vstorename'] = self.vstore_name
            self.init_http_head()
            self.session.mount(item_url.lower(), HostNameIgnoringAdapter())
            result = self.do_call(url, data,
                                  calltimeout=constants.LOGIN_SOCKET_TIMEOUT,
                                  filter_flag=True)

            if (result['error']['code'] != 0) or ("data" not in result):
                LOG.error("Login error. URL: %(url)s\n"
                          "Reason: %(reason)s.",
                          {"url": item_url, "reason": result})
                continue

            LOG.info('Login success: %(url)s', {'url': item_url})
            device_id = result['data']['deviceid']
            self.device_id = device_id
            self.url = item_url + device_id
            self.session.headers['iBaseToken'] = result['data']['iBaseToken']
            if (result['data']['accountstate']
                    in constants.PWD_EXPIRED_OR_INITIAL):
                self.logout()
                msg = _("Password has expired or initial, "
                        "please change the password.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            break

        if device_id is None:
            msg = _("Failed to login with all rest URLs.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return device_id

    def try_login(self):
        try:
            self.login()
        except Exception as err:
            LOG.warning('Login failed. Error: %s.', err)

    def relogin(self, old_token):
        """Relogin Huawei storage array

        When batch apporation failed
        """
        old_url = self.url
        if self.url is None:
            try:
                self.login()
            except Exception as err:
                LOG.error("Relogin failed. Error: %s.", err)
                return False
            LOG.info('Relogin: \n'
                     'Replace URL: \n'
                     'Old URL: %(old_url)s\n,'
                     'New URL: %(new_url)s\n.',
                     {'old_url': old_url,
                      'new_url': self.url})
        elif old_token == self.session.headers['iBaseToken']:
            try:
                self.logout()
            except Exception as err:
                LOG.warning('Logout failed. Error: %s.', err)

            try:
                self.login()
            except Exception as err:
                LOG.error("Relogin failed. Error: %s.", err)
                return False
            LOG.info('First logout then login: \n'
                     'Replace URL: \n'
                     'Old URL: %(old_url)s\n,'
                     'New URL: %(new_url)s\n.',
                     {'old_url': old_url,
                      'new_url': self.url})
        else:
            LOG.info('Relogin has been successed by other thread.')
        return True

    def call(self, url, data=None, method=None, filter_flag=False):
        """Send requests to server.

        If fail, try another RestURL.
        """
        with self.call_lock.read_lock():
            if self.url:
                old_token = self.session.headers.get('iBaseToken')
                result = self.do_call(url, data, method,
                                      filter_flag=filter_flag)
            else:
                old_token = None
                result = {"error": {
                    "code": constants.ERROR_UNAUTHORIZED_TO_SERVER,
                    "description": "unauthorized."}}

        error_code = result['error']['code']
        if error_code in constants.RELOGIN_ERROR_CODE:
            LOG.error("Can't open the recent url, relogin.")
            with self.call_lock.write_lock():
                relogin_result = self.relogin(old_token)
            if relogin_result:
                with self.call_lock.read_lock():
                    result = self.do_call(url, data, method,
                                          filter_flag=filter_flag)
                if result['error']['code'] in constants.RELOGIN_ERROR_PASS:
                    LOG.warning('This operation maybe successed first time')
                    result['error']['code'] = 0
                elif result['error']['code'] == 0:
                    LOG.info('Successed in the second time.')
                else:
                    LOG.info('Failed in the second time, Reason: %s', result)
            else:
                LOG.error('Relogin failed, no need to send again.')
        return result

    def logout(self):
        """Logout the session."""
        url = "/sessions"
        if self.url:
            result = self.do_call(url, None, "DELETE")
            self._assert_rest_result(result, _('Logout session error.'))

    def _assert_rest_result(self, result, err_str):
        if result['error']['code'] != 0:
            msg = (_('%(err)s\nresult: %(res)s.') % {'err': err_str,
                                                     'res': result})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _assert_initiator_exist(self, result, err_str):
        if result['error']['code'] != constants.INITIATOR_NOT_EXIST:
            msg = (_('%(err)s\nresult: %(res)s.') % {'err': err_str,
                                                     'res': result})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _assert_data_in_result(self, result, msg):
        if 'data' not in result:
            err_msg = _('%s "data" is not in result.') % msg
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

    def _get_info_by_range(self, func, params=None):
        range_start = 0
        info_list = []
        while True:
            range_end = range_start + constants.MAX_QUERY_COUNT
            info = func(range_start, range_end, params)
            info_list += info
            if len(info) < constants.MAX_QUERY_COUNT:
                break

            range_start += constants.MAX_QUERY_COUNT
        return info_list

    def create_lun(self, lun_params):
        # Set the mirror switch always on
        lun_params['MIRRORPOLICY'] = '1'
        url = "/lun"
        result = self.call(url, lun_params)
        if result['error']['code'] == constants.ERROR_VOLUME_ALREADY_EXIST:
            lun_id = self.get_lun_id_by_name(lun_params['NAME'])
            if lun_id:
                return self.get_lun_info(lun_id)

        if result['error']['code'] == constants.ERROR_VOLUME_TIMEOUT:
            try_times = 2
            while try_times:
                time.sleep(constants.GET_VOLUME_WAIT_INTERVAL)
                LOG.info(("Create LUN TimeOut, try get lun info in %s "
                          "time"), 2 - try_times)
                lun_id = self.get_lun_id_by_name(lun_params['NAME'])
                if lun_id:
                    return self.get_lun_info(lun_id)
                else:
                    try_times -= 1

        msg = _('Create lun error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']

    def check_lun_exist(self, lun_id, lun_wwn=None):
        url = "/lun/" + lun_id
        result = self.call(url, None, "GET")
        error_code = result['error']['code']
        if error_code != 0:
            if error_code == constants.ERROR_LUN_NOT_EXIST:
                LOG.warning("Can't find LUN %s on the array.", lun_id)
                return False
            else:
                msg = (_("Check LUN exist error."))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if lun_wwn and result['data']['WWN'] != lun_wwn:
            LOG.debug("LUN ID %(id)s with WWN %(wwn)s does not exist on "
                      "the array.", {"id": lun_id, "wwn": lun_wwn})
            return False

        return True

    def delete_lun(self, lun_id):
        url = "/lun/" + lun_id
        data = {"TYPE": "11",
                "ID": lun_id}
        result = self.call(url, data, "DELETE")
        if result['error']['code'] == constants.ERROR_LUN_NOT_EXIST:
            return
        self._assert_rest_result(result, _('Delete lun error.'))

    def get_all_pools(self):
        url = "/storagepool"
        result = self.call(url, None, "GET", filter_flag=True)
        msg = _('Query resource pool error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)
        return result['data']

    def get_pool_by_name(self, pool_name):
        url = "/storagepool?filter=NAME::%s" % pool_name
        result = self.call(url, None, "GET")
        msg = _('Query pool info by name error.')
        self._assert_rest_result(result, msg)
        if result.get('data'):
            return result['data'][0]

    @staticmethod
    def _get_capacity_info(info, pool):
        info['CAPACITY'] = pool.get('DATASPACE', pool['USERFREECAPACITY'])
        info['TOTALCAPACITY'] = pool['USERTOTALCAPACITY']
        if 'totalSizeWithoutSnap' in pool:
            info['LUNCONFIGEDCAPACITY'] = pool['totalSizeWithoutSnap']
        elif 'LUNCONFIGEDCAPACITY' in pool:
            info['LUNCONFIGEDCAPACITY'] = pool['LUNCONFIGEDCAPACITY']
        return info

    def get_pool_info(self, pool_name=None, pools=None):
        info = {}
        if not pool_name:
            return info

        for pool in pools:
            if pool_name.strip() != pool.get('NAME'):
                continue

            if pool.get('USAGETYPE') == constants.FILE_SYSTEM_POOL_TYPE:
                break

            info['ID'] = pool.get('ID')
            info['TIER0CAPACITY'] = pool.get('TIER0CAPACITY')
            info['TIER1CAPACITY'] = pool.get('TIER1CAPACITY')
            info['TIER2CAPACITY'] = pool.get('TIER2CAPACITY')
            self._get_capacity_info(info, pool)

        return info

    def get_pool_id(self, pool_name):
        pools = self.get_all_pools()
        pool_info = self.get_pool_info(pool_name, pools)
        if not pool_info:
            # The following code is to keep compatibility with old version of
            # Huawei driver.
            for pool_name in self.storage_pools:
                pool_info = self.get_pool_info(pool_name, pools)
                if pool_info:
                    break

        if not pool_info:
            msg = _('Can not get pool info. pool: %s') % pool_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return pool_info['ID']

    def _get_id_from_result(self, result, name, key):
        if 'data' in result:
            for item in result['data']:
                if name == item.get(key):
                    return item['ID']

    def _get_result_id(self, result):
        if result.get('data'):
            for item in result['data']:
                return item['ID']
        return None

    def get_lun_id_by_name(self, name):
        if not name:
            return

        url = "/lun?filter=NAME::%s&range=[0-100]" % name
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get lun id by name error.'))

        return self._get_id_from_result(result, name, 'NAME')

    def activate_snapshot(self, snapshot_id):
        url = "/snapshot/activate"
        data = ({"SNAPSHOTLIST": snapshot_id}
                if type(snapshot_id) in (list, tuple)
                else {"SNAPSHOTLIST": [snapshot_id]})
        result = self.call(url, data)
        self._assert_rest_result(result, _('Activate snapshot error.'))

    def create_snapshot(self, lun_id, snapshot_name, snapshot_description):
        url = "/snapshot"
        data = {"TYPE": "27",
                "NAME": snapshot_name,
                "PARENTTYPE": "11",
                "DESCRIPTION": snapshot_description,
                "PARENTID": lun_id}
        result = self.call(url, data)
        if result['error']['code'] == constants.ERROR_VOLUME_ALREADY_EXIST:
            snapshot_id = self.get_snapshot_id_by_name(snapshot_name)
            if snapshot_id:
                return self.get_snapshot_info(snapshot_id)

        if result['error']['code'] == constants.ERROR_VOLUME_TIMEOUT:
            try_times = 2
            while try_times:
                time.sleep(constants.GET_VOLUME_WAIT_INTERVAL)
                LOG.info(_("Create SNAPSHOT TimeOut, try get snapshot "
                           "info in %s time"), 2 - try_times)
                snapshot_id = self.get_snapshot_id_by_name(snapshot_name)
                if snapshot_id:
                    return self.get_snapshot_info(snapshot_id)
                else:
                    try_times -= 1

        msg = _('Create snapshot error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']

    def get_lun_id(self, volume, volume_name):
        metadata = huawei_utils.get_lun_metadata(volume)
        lun_id = (metadata.get('huawei_lun_id') or
                  self.get_lun_id_by_name(volume_name))

        if not lun_id:
            msg = (_("Can't find lun info on the array. "
                     "volume: %(id)s, lun name: %(name)s.") %
                   {'id': volume.id, 'name': volume_name})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return lun_id

    def check_snapshot_exist(self, snapshot_id, snapshot_wwn=None):
        url = "/snapshot/%s" % snapshot_id
        result = self.call(url, None, "GET")
        error_code = result['error']['code']
        if error_code != 0:
            if error_code == constants.ERROR_SNAPSHOT_NOT_EXIST:
                return False
            else:
                msg = (_("Check snapshot exist error."))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
        if snapshot_wwn:
            if snapshot_wwn != result['data']['WWN']:
                return False

        return True

    def stop_snapshot(self, snapshot_id):
        url = "/snapshot/stop"
        stopdata = {"ID": snapshot_id}
        result = self.call(url, stopdata, "PUT")
        self._assert_rest_result(result, _('Stop snapshot error.'))

    def delete_snapshot(self, snapshotid):
        url = "/snapshot/%s" % snapshotid
        data = {"TYPE": "27", "ID": snapshotid}
        result = self.call(url, data, "DELETE")
        if result['error']['code'] == constants.ERROR_SNAPSHOT_NOT_EXIST:
            return
        self._assert_rest_result(result, _('Delete snapshot error.'))

    def get_snapshot_id_by_name(self, name):
        if not name:
            return

        url = "/snapshot?filter=NAME::%s&range=[0-100]" % name
        description = 'The snapshot license file is unavailable.'
        result = self.call(url, None, "GET")
        if 'error' in result:
            if description == result['error']['description']:
                return
            self._assert_rest_result(result, _('Get snapshot id error.'))

        return self._get_id_from_result(result, name, 'NAME')

    def create_luncopy(self, srclunid, tgtlunid, copyspeed):
        data = {"NAME": 'LUNCopy_%s_%s' % (srclunid, tgtlunid),
                "COPYSPEED": copyspeed,
                "SOURCELUN": ("INVALID;%s;INVALID;INVALID;INVALID"
                              % srclunid),
                "TARGETLUN": ("INVALID;%s;INVALID;INVALID;INVALID"
                              % tgtlunid),
                }
        result = self.call("/luncopy", data)

        msg = _('Create luncopy error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']['ID']

    def add_host_to_hostgroup(self, host_id):
        """Associate host to hostgroup.

        If hostgroup doesn't exist, create one.
        """
        hostgroup_name = constants.HOSTGROUP_PREFIX + host_id
        hostgroup_id = self.create_hostgroup_with_check(hostgroup_name)
        is_associated = self._is_host_associate_to_hostgroup(hostgroup_id,
                                                             host_id)
        if not is_associated:
            self._associate_host_to_hostgroup(hostgroup_id, host_id)

        return hostgroup_id

    def get_tgt_port_group(self, tgt_port_group):
        """Find target portgroup id by target port group name."""
        url = "/portgroup?filter=NAME::%s" % tgt_port_group
        result = self.call(url, None, "GET")

        msg = _('Find portgroup error.')
        self._assert_rest_result(result, msg)
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def _associate_portgroup_to_view(self, view_id, portgroup_id):
        url = "/MAPPINGVIEW/CREATE_ASSOCIATE"
        data = {"ASSOCIATEOBJTYPE": "257",
                "ASSOCIATEOBJID": portgroup_id,
                "TYPE": "245",
                "ID": view_id}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Associate portgroup to mapping '
                                 'view error.'))

    def _portgroup_associated(self, view_id, portgroup_id):
        url = ("/mappingview/associate?TYPE=245&"
               "ASSOCIATEOBJTYPE=257&ASSOCIATEOBJID=%s" % portgroup_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Check portgroup associate error.'))

        if self._get_id_from_result(result, view_id, 'ID'):
            return True
        return False

    def do_mapping(self, lun_info, hostgroup_id, host_id, portgroup_id=None,
                   lun_type=constants.LUN_TYPE, hypermetro_lun=False):
        """Add hostgroup and lungroup to mapping view."""
        lun_id = lun_info['ID']
        lungroup_name = constants.LUNGROUP_PREFIX + host_id
        mapping_view_name = constants.MAPPING_VIEW_PREFIX + host_id
        lungroup_id = self._find_lungroup(lungroup_name)
        view_id = self.find_mapping_view(mapping_view_name)
        map_info = {}

        LOG.info('do_mapping, lun_group: %(lun_group)s, '
                 'view_id: %(view_id)s, lun_id: %(lun_id)s.',
                 {'lun_group': lungroup_id,
                  'view_id': view_id,
                  'lun_id': lun_id})

        try:
            # Create lungroup and add LUN into to lungroup.
            if lungroup_id is None:
                lungroup_id = self._create_lungroup(lungroup_name)
            is_associated = self._is_lun_associated_to_lungroup(
                lungroup_id, lun_info, lun_type)
            is_associated_host = False
            if view_id and self.lungroup_associated(view_id, lungroup_id):
                is_associated_host = True
            if not is_associated:
                self.associate_lun_to_lungroup(lungroup_id, lun_id,
                                               lun_type, is_associated_host)
            if view_id is None:
                view_id = self._add_mapping_view(mapping_view_name)
                self._associate_hostgroup_to_view(view_id, hostgroup_id)
                self._associate_lungroup_to_view(view_id, lungroup_id)
                if portgroup_id:
                    self._associate_portgroup_to_view(view_id, portgroup_id)
            else:
                if not self.hostgroup_associated(view_id, hostgroup_id):
                    self._associate_hostgroup_to_view(view_id, hostgroup_id)
                if not self.lungroup_associated(view_id, lungroup_id):
                    self._associate_lungroup_to_view(view_id, lungroup_id)
                if portgroup_id:
                    if not self._portgroup_associated(view_id,
                                                      portgroup_id):
                        self._associate_portgroup_to_view(view_id,
                                                          portgroup_id)
            if hypermetro_lun:
                aval_luns = self.find_view_by_id(view_id)
                map_info["lun_id"] = lun_id
                map_info["view_id"] = view_id
                map_info["aval_luns"] = aval_luns
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(
                    'Error occurred when adding hostgroup and lungroup to '
                    'view. Remove lun from lungroup now.')
                self.remove_lun_from_lungroup(lungroup_id, lun_id, lun_type)
        return map_info

    def ensure_initiator_added(self, initiator_name, host_id, host_name):
        added = self._initiator_is_added_to_array(initiator_name)
        if not added:
            self._add_initiator_to_array(initiator_name)
        if not self.is_initiator_associated_to_host(initiator_name, host_id):
            self._associate_initiator_to_host(initiator_name, host_id,
                                              host_name)

        alua_info = self._find_alua_info(
            self.iscsi_info, initiator_name, host_name)

        LOG.info('Use ALUA %s when adding initiator to host.', alua_info)
        self._use_iscsi_alua(initiator_name, alua_info)

    def find_hostgroup(self, groupname):
        """Get the given hostgroup id."""
        url = "/hostgroup?filter=NAME::%s" % groupname
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get hostgroup information error.'))

        return self._get_result_id(result)

    def _find_lungroup(self, lungroup_name):
        """Get the given hostgroup id."""
        url = "/lungroup?filter=NAME::%s" % lungroup_name
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get lungroup information error.'))

        return self._get_result_id(result)

    def create_hostgroup_with_check(self, hostgroup_name):
        """Check if host exists on the array, or create it."""
        hostgroup_id = self.find_hostgroup(hostgroup_name)
        if hostgroup_id:
            LOG.info(
                'create_hostgroup_with_check. '
                'hostgroup name: %(name)s, '
                'hostgroup id: %(id)s',
                {'name': hostgroup_name,
                 'id': hostgroup_id})
            return hostgroup_id

        try:
            hostgroup_id = self._create_hostgroup(hostgroup_name)
        except Exception:
            LOG.info(
                'Failed to create hostgroup: %(name)s. '
                'Please check if it exists on the array.',
                {'name': hostgroup_name})
            hostgroup_id = self.find_hostgroup(hostgroup_name)
            if hostgroup_id is None:
                err_msg = (_(
                    'Failed to create hostgroup: %(name)s. '
                    'Check if it exists on the array.')
                    % {'name': hostgroup_name})
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)

        LOG.info(
            'create_hostgroup_with_check. '
            'Create hostgroup success. '
            'hostgroup name: %(name)s, '
            'hostgroup id: %(id)s',
            {'name': hostgroup_name,
             'id': hostgroup_id})
        return hostgroup_id

    def _create_hostgroup(self, hostgroup_name):
        url = "/hostgroup"
        data = {"TYPE": "14", "NAME": hostgroup_name}
        result = self.call(url, data)

        msg = _('Create hostgroup error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']['ID']

    def _create_lungroup(self, lungroup_name):
        url = "/lungroup"
        data = {"DESCRIPTION": lungroup_name,
                "APPTYPE": '0',
                "GROUPTYPE": '0',
                "NAME": lungroup_name}
        result = self.call(url, data)

        msg = _('Create lungroup error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']['ID']

    def delete_lungroup(self, lungroup_id):
        url = "/LUNGroup/" + lungroup_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Delete lungroup error.'))

    def lungroup_associated(self, view_id, lungroup_id):
        url = ("/mappingview/associate?TYPE=245&"
               "ASSOCIATEOBJTYPE=256&ASSOCIATEOBJID=%s" % lungroup_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Check lungroup associate error.'))

        if self._get_id_from_result(result, view_id, 'ID'):
            return True
        return False

    def hostgroup_associated(self, view_id, hostgroup_id):
        url = ("/mappingview/associate?TYPE=245&"
               "ASSOCIATEOBJTYPE=14&ASSOCIATEOBJID=%s" % hostgroup_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Check hostgroup associate error.'))

        if self._get_id_from_result(result, view_id, 'ID'):
            return True
        return False

    def get_host_lun_id(self, host_id, lun_info, lun_type=constants.LUN_TYPE):
        cmd_type = 'lun' if lun_type == constants.LUN_TYPE else 'snapshot'
        url = ("/%s/associate?TYPE=%s&ASSOCIATEOBJTYPE=21&ASSOCIATEOBJID=%s"
               "&filter=NAME::%s&selectFields=ID,NAME,ASSOCIATEMETADATA,WWN"
               % (cmd_type, lun_type, host_id, lun_info['NAME']))
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find host lun id error.'))

        host_lun_id = 1
        if 'data' in result:
            for item in result['data']:
                if lun_info['ID'] == item['ID']:
                    associate_data = item['ASSOCIATEMETADATA']
                    try:
                        hostassoinfo = json.loads(associate_data)
                        host_lun_id = hostassoinfo['HostLUNID']
                        break
                    except Exception as err:
                        LOG.error("JSON transfer data error. %s.", err)
                        raise
        return host_lun_id

    def get_host_id_by_name(self, host_name):
        """Get the given host ID."""
        url = "/host?filter=NAME::%s&range=[0-100]" % host_name
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find host in hostgroup error.'))

        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def add_host_with_check(self, host_name, is_dorado_v6, initiator):
        self.is_dorado_v6 = is_dorado_v6
        host_id = huawei_utils.get_host_id(self, host_name)
        new_alua_info = {}
        if self.is_dorado_v6:
            info = self.iscsi_info or self.fc_info or self.roce_info
            new_alua_info = self._find_new_alua_info(
                info, host_name, initiator)
        if host_id:
            LOG.info(
                'add_host_with_check. '
                'host name: %(name)s, '
                'host id: %(id)s',
                {'name': host_name,
                 'id': host_id})
            self._update_host(host_id, new_alua_info)
            return host_id

        encoded_name = huawei_utils.encode_host_name(host_name)

        try:
            host_id = self._add_host(encoded_name, host_name, new_alua_info)
        except Exception:
            LOG.info(
                'Failed to create host: %(name)s. '
                'Check if it exists on the array.',
                {'name': encoded_name})
            host_id = self.get_host_id_by_name(encoded_name)
            if not host_id:
                msg = _('Failed to create host: %(name)s. '
                        'Please check if it exists on the array.'
                        ) % {'name': encoded_name}
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        LOG.info(
            'add_host_with_check. '
            'create host success. '
            'host name: %(name)s, '
            'host id: %(id)s',
            {'name': encoded_name,
             'id': host_id})
        return host_id

    def _add_host(self, hostname, host_name_before_hash, info):
        """Add a new host."""
        url = "/host"
        data = {"TYPE": "21",
                "NAME": hostname,
                "OPERATIONSYSTEM": "0",
                "DESCRIPTION": host_name_before_hash}
        data.update(info)
        result = self.call(url, data)
        self._assert_rest_result(result, _('Add new host error.'))

        if 'data' in result:
            return result['data']['ID']

    def _update_host(self, host_id, data):
        """Update a host."""
        url = "/host/" + host_id
        result = self.call(url, data, "PUT")
        if result['error']['code'] == constants.HOST_NOT_EXIST:
            return
        self._assert_rest_result(result, _('Update host error.'))

    def _is_host_associate_to_hostgroup(self, hostgroup_id, host_id):
        """Check whether the host is associated to the hostgroup."""
        url = ("/host/associate?TYPE=21&"
               "ASSOCIATEOBJTYPE=14&ASSOCIATEOBJID=%s" % hostgroup_id)

        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Check hostgroup associate error.'))

        if self._get_id_from_result(result, host_id, 'ID'):
            return True

        return False

    def _is_lun_associated_to_lungroup(self, lungroup_id, lun_info,
                                       lun_type=constants.LUN_TYPE):
        """Check whether the lun is associated to the lungroup."""
        cmd_type = 'lun' if lun_type == constants.LUN_TYPE else 'snapshot'
        url = ("/%s/associate?TYPE=%s&ASSOCIATEOBJTYPE=256&ASSOCIATEOBJID=%s"
               "&filter=NAME::%s&selectFields=ID,NAME,ASSOCIATEMETADATA,WWN"
               % (cmd_type, lun_type, lungroup_id, lun_info['NAME']))

        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Check lungroup associate error.'))

        if self._get_id_from_result(result, lun_info['ID'], 'ID'):
            return True

        return False

    def _associate_host_to_hostgroup(self, hostgroup_id, host_id):
        url = "/hostgroup/associate"
        data = {"TYPE": "14",
                "ID": hostgroup_id,
                "ASSOCIATEOBJTYPE": "21",
                "ASSOCIATEOBJID": host_id}

        result = self.call(url, data)
        if result['error']['code'] == constants.HOST_ALREADY_IN_HOSTGROUP:
            return
        self._assert_rest_result(result, _('Associate host to hostgroup '
                                 'error.'))

    def associate_lun_to_lungroup(
            self, lungroup_id, lun_id, lun_type=constants.LUN_TYPE, is_associated_host=False):
        """Associate lun to lungroup."""
        url = "/lungroup/associate"
        data = {"ID": lungroup_id,
                "ASSOCIATEOBJTYPE": lun_type,
                "ASSOCIATEOBJID": lun_id}
        if is_associated_host and self.is_dorado_v6:
            data["startHostLunId"] = 1
        result = self.call(url, data)
        if result['error']['code'] == constants.LUN_ALREADY_IN_LUNGROUP:
            return
        self._assert_rest_result(result, _('Associate lun to lungroup error.'))

    def remove_lun_from_lungroup(self, lungroup_id, lun_id,
                                 lun_type=constants.LUN_TYPE):
        """Remove lun from lungroup."""
        url = ("/lungroup/associate?ID=%s&ASSOCIATEOBJTYPE=%s"
               "&ASSOCIATEOBJID=%s" % (lungroup_id, lun_type, lun_id))

        result = self.call(url, None, 'DELETE')
        self._assert_rest_result(
            result, _('Delete associated lun from lungroup error.'))

    def _initiator_is_added_to_array(self, ininame):
        """Check whether the initiator is already added on the array."""
        url = "/iscsi_initiator/%s" % ininame
        result = self.call(url, None, "GET")
        err_str = _('Check initiator added to array error.')

        if result['error']['code'] != 0:
            self._assert_initiator_exist(result, err_str)
            return False

        if result.get('data'):
            return True
        return False

    def is_initiator_associated_to_host(self, ininame, host_id):
        """Check whether the initiator is associated to the host."""
        url = "/iscsi_initiator/%s" % ininame
        result = self.call(url, None, "GET")
        err_str = _('Check initiator associated to host error.')

        if result['error']['code'] != 0:
            self._assert_initiator_exist(result, err_str)
            return True

        if result.get('data'):
            item = result.get('data')
            if item['ISFREE'] == "true":
                return False
            if item['PARENTID'] == host_id:
                return True
            else:
                msg = (_("Initiator %(ini)s has been added to host "
                         "%(host)s.") % {"ini": ininame,
                                         "host": item['PARENTNAME']})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
        return True

    def is_initiator_used_chap(self, ininame):
        """Check whether the initiator is associated to the host."""
        url = "/iscsi_initiator/%s" % ininame
        result = self.call(url, None, "GET")
        err_str = _('Check initiator associated to host error.')

        if result['error']['code'] != 0:
            self._assert_initiator_exist(result, err_str)
            return False

        if result.get('data'):
            item = result.get('data')
            if item['USECHAP'] == "true":
                return True
        return False

    def _add_initiator_to_array(self, initiator_name):
        """Add a new initiator to storage device."""
        url = "/iscsi_initiator"
        data = {"TYPE": "222",
                "ID": initiator_name,
                "USECHAP": "false"}
        result = self.call(url, data, "POST")
        self._assert_rest_result(result,
                                 _('Add initiator to array error.'))

    def _add_initiator_to_host(self, initiator_name, host_id):
        url = "/iscsi_initiator/" + initiator_name
        data = {"TYPE": "222",
                "ID": initiator_name,
                "USECHAP": "false",
                "PARENTTYPE": "21",
                "PARENTID": host_id}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result,
                                 _('Associate initiator to host error.'))

    def _associate_initiator_to_host(self, initiator_name, host_id, host_name):
        """Associate initiator with the host."""
        chapinfo = self.find_chap_info(
            self.iscsi_info, initiator_name, host_name)

        if chapinfo:
            LOG.info('Use CHAP when adding initiator to host.')
            self._use_chap(chapinfo, initiator_name, host_id)
        else:
            self._add_initiator_to_host(initiator_name, host_id)

    def find_chap_info(self, iscsi_info, initiator_name, host_name):
        """Find CHAP info from xml."""
        chapinfo = None
        find_initiator_flag = False
        for ini in iscsi_info:
            if ini.get("Name") == initiator_name:
                find_initiator_flag = True
                if 'CHAPinfo' in ini:
                    chapinfo = ini['CHAPinfo']
                    break

        tmp_chap_info = None
        if not find_initiator_flag:
            for info in iscsi_info:
                if info.get('HostName'):
                    if info.get('HostName') == '*':
                        tmp_chap_info = info.get('CHAPinfo')
                    elif re.search(info.get('HostName'), host_name):
                        chapinfo = info.get('CHAPinfo')
                        break

        if chapinfo is None and tmp_chap_info:
            chapinfo = tmp_chap_info
        return chapinfo

    @staticmethod
    def _find_name_info(config, initiator_name):
        for ini in config:
            if (ini.get('Name') == initiator_name or
                    (isinstance(initiator_name, list) and
                     ini.get('Name') in initiator_name)):
                return ini

    @staticmethod
    def _find_hostname_info(config, host_name):
        for info in config:
            if info.get('HostName') and (info.get('HostName') == '*' or
                                         re.search(info.get('HostName'),
                                                   host_name)):
                return info

    def _find_name_or_hostname_info(self, config, initiator_name, host_name):
        find_info = self._find_name_info(config, initiator_name)
        if not find_info:
            find_info = self._find_hostname_info(config, host_name)

        return find_info

    def _find_alua_info(self, config, initiator_name, host_name):
        """Find ALUA info from xml."""
        alua_info = {'ALUA': '0'}
        find_info = self._find_name_or_hostname_info(
            config, initiator_name, host_name)

        if find_info:
            if 'ACCESSMODE' in find_info and self.is_dorado_v6:
                return alua_info
            if 'ALUA' in find_info:
                alua_info['ALUA'] = find_info['ALUA']

            if alua_info['ALUA'] not in ('0', '1'):
                msg = _(
                    'Invalid ALUA value. ALUA value must be 1 or 0.')
                LOG.error(msg)
                raise exception.InvalidInput(msg)

            if alua_info['ALUA'] == '1':
                for k in ('FAILOVERMODE', 'SPECIALMODETYPE', 'PATHTYPE'):
                    if k in find_info:
                        alua_info[k] = find_info[k]

        return alua_info

    def _find_new_alua_info(self, config, host_name, initiator):
        """Find new ALUA info from xml."""
        alua_info = {'accessMode': '0'}
        find_info = self._find_name_or_hostname_info(
            config, initiator, host_name)

        if (find_info and find_info.get('ACCESSMODE') and
                find_info.get('HYPERMETROPATHOPTIMIZED')):
            alua_info.update({
                'accessMode': find_info['ACCESSMODE'],
                'hyperMetroPathOptimized':
                    find_info['HYPERMETROPATHOPTIMIZED']
            })

        return alua_info

    def _use_chap(self, chapinfo, initiator_name, host_id):
        """Use CHAP when adding initiator to host."""
        (chap_username, chap_password) = chapinfo.split(";")

        url = "/iscsi_initiator/" + initiator_name
        data = {"TYPE": "222",
                "USECHAP": "true",
                "CHAPNAME": chap_username,
                "CHAPPASSWORD": chap_password,
                "ID": initiator_name,
                "PARENTTYPE": "21",
                "PARENTID": host_id}
        result = self.call(url, data, "PUT", filter_flag=True)
        msg = _('Use CHAP to associate initiator to host error. '
                'Please check the CHAP username and password.')
        self._assert_rest_result(result, msg)

    def _use_iscsi_alua(self, initiator_name, alua_info):
        """Use ALUA when adding initiator to host."""
        url = "/iscsi_initiator"
        data = {"ID": initiator_name,
                'MULTIPATHTYPE': alua_info.pop('ALUA')}
        data.update(alua_info)

        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, _('Use ALUA to associate initiator to host error.'))

    def remove_chap(self, initiator_name):
        """Remove CHAP when terminate connection."""
        url = "/iscsi_initiator"
        data = {"USECHAP": "false",
                "MULTIPATHTYPE": "0",
                "ID": initiator_name}
        result = self.call(url, data, "PUT")

        self._assert_rest_result(result, _('Remove CHAP error.'))

    def find_mapping_view(self, name):
        """Find mapping view."""
        if not name:
            return None
        url = "/mappingview?filter=NAME::%s" % name
        result = self.call(url, None, "GET")

        msg = _('Find mapping view error.')
        self._assert_rest_result(result, msg)

        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def _add_mapping_view(self, name):
        if not name:
            return None
        url = "/mappingview"
        data = {"NAME": name, "TYPE": "245"}
        result = self.call(url, data)
        self._assert_rest_result(result, _('Add mapping view error.'))

        return result['data']['ID']

    def _associate_hostgroup_to_view(self, view_id, hostgroup_id):
        url = "/MAPPINGVIEW/CREATE_ASSOCIATE"
        data = {"ASSOCIATEOBJTYPE": "14",
                "ASSOCIATEOBJID": hostgroup_id,
                "TYPE": "245",
                "ID": view_id}
        result = self.call(url, data, "PUT")
        if (result['error']['code'] ==
                constants.HOSTGROUP_ALREADY_IN_MAPPINGVIEW):
            return
        self._assert_rest_result(result, _('Associate host to mapping view '
                                 'error.'))

    def _associate_lungroup_to_view(self, view_id, lungroup_id):
        url = "/MAPPINGVIEW/CREATE_ASSOCIATE"
        data = {"ASSOCIATEOBJTYPE": "256",
                "ASSOCIATEOBJID": lungroup_id,
                "TYPE": "245",
                "ID": view_id}

        result = self.call(url, data, "PUT")
        if (result['error']['code'] ==
                constants.LUNGROUP_ALREADY_IN_MAPPINGVIEW):
            return
        self._assert_rest_result(
            result, _('Associate lungroup to mapping view error.'))

    def delete_lungroup_mapping_view(self, view_id, lungroup_id):
        """Remove lungroup associate from the mapping view."""
        url = "/mappingview/REMOVE_ASSOCIATE"
        data = {"ASSOCIATEOBJTYPE": "256",
                "ASSOCIATEOBJID": lungroup_id,
                "TYPE": "245",
                "ID": view_id}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Delete lungroup from mapping view '
                                 'error.'))

    def delete_hostgoup_mapping_view(self, view_id, hostgroup_id):
        """Remove hostgroup associate from the mapping view."""
        url = "/mappingview/REMOVE_ASSOCIATE"
        data = {"ASSOCIATEOBJTYPE": "14",
                "ASSOCIATEOBJID": hostgroup_id,
                "TYPE": "245",
                "ID": view_id}

        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, _('Delete hostgroup from mapping view error.'))

    def delete_portgroup_mapping_view(self, view_id, portgroup_id):
        """Remove portgroup associate from the mapping view."""
        url = "/mappingview/REMOVE_ASSOCIATE"
        data = {"ASSOCIATEOBJTYPE": "257",
                "ASSOCIATEOBJID": portgroup_id,
                "TYPE": "245",
                "ID": view_id}

        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, _('Delete portgroup from mapping view error.'))

    def delete_mapping_view(self, view_id):
        """Remove mapping view from the storage."""
        url = "/mappingview/" + view_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Delete mapping view error.'))

    def get_obj_count_from_lungroup(self, lungroup_id):
        """Get all objects count associated to the lungroup."""
        lun_count = self._get_obj_count_from_lungroup_by_type(
            lungroup_id, constants.LUN_TYPE)
        snapshot_count = self._get_obj_count_from_lungroup_by_type(
            lungroup_id, constants.SNAPSHOT_TYPE)
        return int(lun_count) + int(snapshot_count)

    def _get_obj_count_from_lungroup_by_type(self, lungroup_id,
                                             lun_type=constants.LUN_TYPE):
        cmd_type = 'lun' if lun_type == constants.LUN_TYPE else 'snapshot'
        lunnum = 0
        if not lungroup_id:
            return lunnum

        url = ("/%s/count?TYPE=%s&ASSOCIATEOBJTYPE=256&"
               "ASSOCIATEOBJID=%s" % (cmd_type, lun_type, lungroup_id))
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find obj number error.'))
        if 'data' in result:
            lunnum = int(result['data']['COUNT'])
        return lunnum

    def is_portgroup_associated_to_view(self, view_id, portgroup_id):
        """Check whether the port group is associated to the mapping view."""
        url = ("/portgroup/associate?ASSOCIATEOBJTYPE=245&"
               "ASSOCIATEOBJID=%s&range=[0-8191]" % view_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find portgroup from mapping view '
                                 'error.'))

        if self._get_id_from_result(result, portgroup_id, 'ID'):
            return True
        return False

    def find_lungroup_from_map(self, view_id):
        """Get lungroup from the given map"""
        url = ("/mappingview/associate/lungroup?TYPE=256&"
               "ASSOCIATEOBJTYPE=245&ASSOCIATEOBJID=%s" % view_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find lun group from mapping view '
                                 'error.'))
        lungroup_id = None
        if 'data' in result:
            # One map can have only one lungroup.
            for item in result['data']:
                lungroup_id = item['ID']

        return lungroup_id

    def start_luncopy(self, luncopy_id):
        """Start a LUNcopy."""
        url = "/LUNCOPY/start"
        data = {"TYPE": "219", "ID": luncopy_id}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Start LUNcopy error.'))

    def _get_capacity(self, pool_name, result):
        """Get free capacity and total capacity of the pool."""
        pool_info = self.get_pool_info(pool_name, result)
        pool_capacity = {'total_capacity': 0.0,
                         'free_capacity': 0.0,
                         }

        if pool_info:
            total = float(pool_info['TOTALCAPACITY']) / constants.CAPACITY_UNIT
            free = float(pool_info['CAPACITY']) / constants.CAPACITY_UNIT
            pool_capacity['total_capacity'] = total
            pool_capacity['free_capacity'] = free

            configed_capacity = pool_info.get('LUNCONFIGEDCAPACITY')
            if configed_capacity:
                provisioned = float(
                    configed_capacity) / constants.CAPACITY_UNIT
                pool_capacity['provisioned_capacity'] = provisioned

        return pool_capacity

    def _get_disk_type(self, pool_name, result):
        """Get disk type of the pool."""
        pool_info = self.get_pool_info(pool_name, result)
        if not pool_info:
            return None

        pool_disk = []
        for i, x in enumerate(['ssd', 'sas', 'nl_sas']):
            if (pool_info['TIER%dCAPACITY' % i] and
                    pool_info['TIER%dCAPACITY' % i] != '0'):
                pool_disk.append(x)

        if len(pool_disk) > 1:
            pool_disk = ['mix']

        return pool_disk[0] if pool_disk else None

    def _get_smarttier(self, disk_type):
        return disk_type is not None and disk_type == 'mix'

    def get_luncopy_info(self, luncopy_id):
        """Get LUNcopy information."""
        url = "/LUNCOPY/%s" % luncopy_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get LUNcopy information error.'))

        luncopyinfo = {}
        if 'data' in result:
            luncopyinfo['name'] = result['data']['NAME']
            luncopyinfo['id'] = result['data']['ID']
            luncopyinfo['state'] = result['data']['HEALTHSTATUS']
            luncopyinfo['status'] = result['data']['RUNNINGSTATUS']
        return luncopyinfo

    def delete_luncopy(self, luncopy_id):
        """Delete a LUNcopy."""
        url = "/LUNCOPY/%s" % luncopy_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Delete LUNcopy error.'))

    def get_init_targ_map(self, wwns):
        init_targ_map = {}
        tgt_port_wwns = []
        for wwn in wwns:
            tgtwwpns = self.get_fc_target_wwpns(wwn)
            if not tgtwwpns:
                continue

            init_targ_map[wwn] = tgtwwpns
            for tgtwwpn in tgtwwpns:
                if tgtwwpn not in tgt_port_wwns:
                    tgt_port_wwns.append(tgtwwpn)

        if not tgt_port_wwns:
            err_msg = (_('Get FC target wwpns error, tgt_port_wwns: '
                         '%(tgt_port_wwns)s, init_targ_map: %(init_targ_map)s')
                       % {'tgt_port_wwns': tgt_port_wwns,
                          'init_targ_map': init_targ_map})
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        return (tgt_port_wwns, init_targ_map)

    def _get_free_wwns(self, start, end, params):
        url = ("/fc_initiator?ISFREE=true&range=[%(start)s-%(end)s]"
               % {"start": six.text_type(start), "end": six.text_type(end)})
        result = self.call(url, None, "GET")
        msg = _('Get connected free FC wwn error.')
        self._assert_rest_result(result, msg)
        return result.get('data', [])

    def get_online_free_wwns(self):
        """Get online free WWNs.

        If no new ports connected, return an empty list.
        """
        wwns_list = self._get_info_by_range(self._get_free_wwns)

        wwns = []
        for item in wwns_list:
            if item['RUNNINGSTATUS'] == constants.FC_INIT_ONLINE:
                wwns.append(item['ID'])

        return wwns

    def _use_fc_alua(self, wwn, alua_info):
        url = "/fc_initiator/" + wwn
        data = {"ID": wwn,
                "MULTIPATHTYPE": alua_info.pop('ALUA')}
        data.update(alua_info)

        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Set ALUA for fc initiator error.'))

    def add_fc_port_to_host(self, host_id, wwn):
        """Add a FC port to the host."""
        url = "/fc_initiator/" + wwn
        data = {"ID": wwn,
                "PARENTTYPE": 21,
                "PARENTID": host_id}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Add FC port to host error.'))

    def get_fc_target_wwpns(self, wwn):
        url = ("/host_link?INITIATOR_TYPE=223&INITIATOR_PORT_WWN=" + wwn)
        result = self.call(url, None, "GET")

        msg = _('Get FC target wwpn error.')
        self._assert_rest_result(result, msg)

        fc_wwpns = []
        if "data" in result:
            for item in result['data']:
                if wwn == item['INITIATOR_PORT_WWN']:
                    fc_wwpns.append(item['TARGET_PORT_WWN'])

        return fc_wwpns

    def get_fc_init_info(self, wwn):
        """Get wwn info by wwn_id and judge is error need to be raised"""
        url = "/fc_initiator/" + wwn
        result = self.call(url, None, "GET")
        error_code = result['error']['code']

        if error_code != 0:
            if error_code not in (constants.FC_INITIATOR_NOT_EXIST,
                                  constants.ERROR_PARAMETER_ERROR):
                msg = (_('Get fc initiator %(initiator)s on array error. '
                         'result: %(res)s.') % {'initiator': wwn,
                                                'res': result})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                return {}

        return result.get('data', {})

    def update_volume_stats(self):
        data = {}
        data['pools'] = []
        result = self.get_all_pools()
        for pool_name in self.storage_pools:
            capacity = self._get_capacity(pool_name, result)
            disk_type = self._get_disk_type(pool_name, result)
            tier_support = self._get_smarttier(disk_type)
            pool = {}
            pool.update(dict(
                location_info=self.device_id,
                pool_name=pool_name,
                total_capacity_gb=capacity['total_capacity'],
                free_capacity_gb=capacity['free_capacity'],
                reserved_percentage=self.configuration.safe_get(
                    'reserved_percentage'),
                max_over_subscription_ratio=self.configuration.safe_get(
                    'max_over_subscription_ratio'),
                smarttier=tier_support
            ))
            if capacity.get('provisioned_capacity') is not None:
                pool['provisioned_capacity_gb'] = capacity[
                    'provisioned_capacity']
            if disk_type:
                pool['disk_type'] = disk_type

            data['pools'].append(pool)
        return data

    def check_storage_pools(self):
        result = self.get_all_pools()
        s_pools = []
        for pool in result:
            if 'USAGETYPE' in pool:
                if pool['USAGETYPE'] in (constants.BLOCK_STORAGE_POOL_TYPE,
                                         constants.DORADO_V6_POOL_TYPE):
                    s_pools.append(pool['NAME'])
            else:
                s_pools.append(pool['NAME'])
        for pool_name in self.storage_pools:
            if pool_name not in s_pools:
                err_msg = (_('Block storage pool %s does not exist on '
                             'the array.') % pool_name)
                LOG.error(err_msg)
                raise exception.VolumeBackendAPIException(data=err_msg)

    def _update_qos_policy_lunlist(self, lun_list, policy_id):
        url = "/ioclass/" + policy_id
        data = {"TYPE": "230",
                "ID": policy_id,
                "LUNLIST": lun_list}

        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Update QoS policy error.'))

    def _get_tgt_ip_from_portgroup(self, portgroup_id):
        target_ips = []
        url = ("/eth_port/associate?TYPE=213&ASSOCIATEOBJTYPE=257"
               "&ASSOCIATEOBJID=%s" % portgroup_id)
        result = self.call(url, None, "GET")

        msg = _('Get target IP error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        if 'data' in result:
            for item in result['data']:
                if ((item['IPV4ADDR'] or item['IPV6ADDR'])
                   and item['HEALTHSTATUS'] == constants.STATUS_HEALTH
                   and item['RUNNINGSTATUS'] == constants.STATUS_RUNNING):
                    if item['IPV4ADDR']:
                        target_ips.append(item['IPV4ADDR'])
                    if item['IPV6ADDR']:
                        target_ips.append(item['IPV6ADDR'])
        LOG.info('_get_tgt_ip_from_portgroup: Get ip: %s.', target_ips)

        return target_ips

    def find_portgroup_info(self, initiator_name, host_name):
        portgroup = None
        find_initiator_flag = False
        for ini in self.iscsi_info:
            if ini.get("Name") == initiator_name:
                find_initiator_flag = True
                if 'TargetPortGroup' in ini:
                    portgroup = ini['TargetPortGroup']
                    break

        tmp_portgroup = None
        if not find_initiator_flag:
            for info in self.iscsi_info:
                if info.get('HostName'):
                    if info.get('HostName') == '*':
                        tmp_portgroup = info.get('TargetPortGroup')
                    elif re.search(info.get('HostName'), host_name):
                        portgroup = info.get('TargetPortGroup')
                        break

        if portgroup is None and tmp_portgroup:
            portgroup = tmp_portgroup

        return portgroup

    def get_iscsi_params(self, connector):
        """Get target iSCSI params, including iqn, IP."""
        initiator = connector['initiator']
        multipath = connector.get('multipath', False)
        host_name = connector['host']
        target_ips = []
        target_iqns = []
        temp_tgt_ips = []
        portgroup_id = None

        portgroup = self.find_portgroup_info(initiator, host_name)

        if portgroup:
            portgroup_id = self.get_tgt_port_group(portgroup)
            tgt_ips = self._get_tgt_ip_from_portgroup(portgroup_id)
            temp_tgt_ips = self.convert_ip_to_normalized_format(tgt_ips)
            port_ips = self._get_tgt_port_ip_from_rest()
            valid_tgt_ips = self.convert_ip_to_normalized_format(port_ips)

            for ip in temp_tgt_ips:
                if ip in valid_tgt_ips:
                    target_ips.append(ip)

            if not target_ips and multipath:
                msg = (_(
                    'get_iscsi_params: No valid port in portgroup. '
                    'portgroup_id: %(id)s, please check it on storage.')
                    % {'id': portgroup_id})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if not target_ips:
            target_ips = self._get_target_ip(initiator, host_name)

        # Deal with the remote tgt ip.
        if 'remote_target_ip' in connector:
            target_ips.append(connector['remote_target_ip'])
        LOG.info('Get the default ip: %s.', target_ips)
        target_ips = self.convert_ip_to_normalized_format(target_ips)

        for ip in target_ips:
            target_iqn = self._get_tgt_iqn_from_rest(ip)
            if target_iqn:
                target_iqns.append(target_iqn)

        if not target_iqns:
            err_msg = (_(
                'Get iSCSI target iqn error, please check the target IP '
                'configured on array.'))
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        target_ips = []
        for iqn in target_iqns:
            ip = iqn.split(':', 5)[5]
            if netaddr.IPAddress(ip).version == 6:
                ip = '[' + ip + ']'
            target_ips.append(ip)

        return (target_iqns, target_ips, portgroup_id)

    def convert_ip_to_normalized_format(self, target_ips):
        format_ips = []
        for ip in target_ips:
            format_ip = netaddr.IPAddress(ip)
            if format_ip.version == 6:
                ip = str(format_ip.format(dialect=netaddr.ipv6_compact))
            format_ips.append(ip)
        return format_ips

    def _get_target_ip(self, initiator, host_name):
        target_ips = []
        find_initiator_flag = False
        for ini in self.iscsi_info:
            if ini.get("Name") == initiator:
                find_initiator_flag = True
                if ini.get('TargetIP'):
                    target_ips = [ip.strip() for ip in ini['TargetIP'].split()
                                  if ip.strip()]

        tmp_target_ips = []
        if not find_initiator_flag:
            for info in self.iscsi_info:
                if info.get('HostName'):
                    if info.get('HostName') == '*':
                        tmp_target_ips = [ip.strip() for ip in info.get(
                            'TargetIP').split() if ip.strip()]
                    elif re.search(info.get('HostName'), host_name):
                        if info.get('TargetIP'):
                            target_ips = [ip.strip() for ip in info.get(
                                'TargetIP').split() if ip.strip()]
                            break

        if not target_ips and tmp_target_ips:
            target_ips = tmp_target_ips

        # If not specify target IP for some initiators, use default IP.
        if not target_ips:
            default_target_ips = self.iscsi_default_target_ip
            if default_target_ips:
                target_ips.append(default_target_ips[0])

            else:
                msg = (_(
                    'get_iscsi_params: Failed to get target IP '
                    'for initiator %(ini)s, please check config file.')
                    % {'ini': initiator})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        return target_ips

    def _get_tgt_port_ip_from_rest(self):
        url = "/iscsi_tgt_port"
        result = self.call(url, None, "GET")
        info_list = []
        target_ips = []
        if result['error']['code'] != 0:
            LOG.warning("Can't find target port info from rest.")
            return target_ips

        elif not result['data']:
            msg = (_(
                "Can't find valid IP from rest, please check it on storage."))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if 'data' in result:
            for item in result['data']:
                info_list.append(item['ID'])

        if not info_list:
            LOG.warning("Can't find target port info from rest.")
            return target_ips

        for info in info_list:
            iqn_info = info.split(',', 1)[0]
            target_ip = iqn_info.split(':', 5)[5]
            target_ips.append(target_ip)
        return target_ips

    def _get_tgt_iqn_from_rest(self, target_ip):
        url = "/iscsi_tgt_port"
        result = self.call(url, None, "GET")

        target_iqn = None
        if result['error']['code'] != 0:
            LOG.warning("Can't find target iqn from rest.")
            return target_iqn

        if 'data' in result:
            for item in result['data']:
                iqn_info = item['ID'].split(',', 1)[0]
                ip = iqn_info.split(':', 5)[5]
                format_ip = netaddr.IPAddress(ip)
                if format_ip.version == 6:
                    ip = str(format_ip.format(dialect=netaddr.ipv6_compact))
                if target_ip == ip:
                    target_iqn = item['ID']
                    break

        if not target_iqn:
            LOG.warning("Can't find target iqn from rest.")
            return target_iqn

        split_list = target_iqn.split(",")
        target_iqn_before = split_list[0]

        split_list_new = target_iqn_before.split("+")
        target_iqn = split_list_new[1]

        return target_iqn

    def create_qos_policy(self, qos, lun_id):
        # Get local time.
        localtime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        # Package QoS name.
        qos_name = constants.QOS_NAME_PREFIX + lun_id + '_' + localtime

        data = {"TYPE": "230",
                "NAME": qos_name,
                "LUNLIST": ["%s" % lun_id],
                "CLASSTYPE": "1",
                "SCHEDULEPOLICY": "2",
                "SCHEDULESTARTTIME": "1410969600",
                "STARTTIME": "08:00",
                "DURATION": "86400",
                "CYCLESET": "[1,2,3,4,5,6,0]",
                }
        data.update(qos)
        url = "/ioclass/"

        result = self.call(url, data)
        self._assert_rest_result(result, _('Create QoS policy error.'))

        return result['data']['ID']

    def delete_qos_policy(self, qos_id):
        """Delete a QoS policy."""
        url = "/ioclass/" + qos_id
        data = {"TYPE": "230", "ID": qos_id}

        result = self.call(url, data, 'DELETE')
        self._assert_rest_result(result, _('Delete QoS policy error.'))

    def activate_deactivate_qos(self, qos_id, enablestatus):
        """Activate or deactivate QoS.

        enablestatus: true (activate)
        enbalestatus: false (deactivate)
        """
        url = "/ioclass/active/" + qos_id
        data = {"TYPE": 230,
                "ID": qos_id,
                "ENABLESTATUS": enablestatus}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, _('Activate or deactivate QoS error.'))

    def get_qos_info(self, qos_id):
        """Get QoS information."""
        url = "/ioclass/" + qos_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get QoS information error.'))

        return result['data']

    def get_lun_list_in_qos(self, qos_id, qos_info):
        """Get the lun list in QoS."""
        lun_list = []
        lun_string = qos_info['LUNLIST'][1:-1]

        for lun in lun_string.split(","):
            str = lun[1:-1]
            lun_list.append(str)

        return lun_list

    def remove_lun_from_qos(self, lun_id, lun_list, qos_id):
        """Remove lun from QoS."""
        lun_list = [i for i in lun_list if i != lun_id]
        url = "/ioclass/" + qos_id
        data = {"LUNLIST": lun_list,
                "TYPE": 230,
                "ID": qos_id}
        result = self.call(url, data, "PUT")

        msg = _('Remove lun from QoS error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

    def change_lun_priority(self, lun_id):
        """Change lun priority to high."""
        url = "/lun/" + lun_id
        data = {"TYPE": "11",
                "ID": lun_id,
                "IOPRIORITY": "3"}

        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Change lun priority error.'))

    def change_lun_smarttier(self, lunid, smarttier_policy):
        """Change lun smarttier policy."""
        url = "/lun/" + lunid
        data = {"TYPE": "11",
                "ID": lunid,
                "DATATRANSFERPOLICY": smarttier_policy}

        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, _('Change lun smarttier policy error.'))

    def get_qosid_by_lunid(self, lun_id):
        """Get QoS id by lun id."""
        url = "/lun/" + lun_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get QoS id by lun id error.'))

        return result['data']['IOCLASSID']

    def get_lungroupids_by_lunid(self, lun_id, lun_type=constants.LUN_TYPE):
        """Get lungroup ids by lun id."""
        url = ("/lungroup/associate?TYPE=256"
               "&ASSOCIATEOBJTYPE=%s&ASSOCIATEOBJID=%s" % (lun_type, lun_id))

        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get lungroup id by lun id error.'))

        lungroup_ids = []
        if 'data' in result:
            for item in result['data']:
                lungroup_ids.append(item['ID'])

        return lungroup_ids

    def get_lun_info(self, lun_id, lun_type=constants.LUN_TYPE):
        cmd_type = 'lun' if lun_type == constants.LUN_TYPE else 'snapshot'
        url = ("/%s/%s" % (cmd_type, lun_id))
        result = self.call(url, None, "GET")

        msg = _('Get volume error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']

    def get_snapshot_info(self, snapshot_id):
        url = "/snapshot/" + snapshot_id
        result = self.call(url, None, "GET")

        msg = _('Get snapshot error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']

    def extend_lun(self, lun_id, new_volume_size):
        url = "/lun/expand"
        data = {"TYPE": 11, "ID": lun_id,
                "CAPACITY": new_volume_size}
        result = self.call(url, data, 'PUT')

        msg = _('Extend volume error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result['data']

    def create_lun_migration(self, src_id, dst_id, speed=2):
        url = "/LUN_MIGRATION"
        data = {"TYPE": '253',
                "PARENTID": src_id,
                "TARGETLUNID": dst_id,
                "SPEED": speed,
                "WORKMODE": 0}

        result = self.call(url, data, "POST")
        msg = _('Create lun migration error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

    def _get_lun_migration_task(self, start, end, params):
        url = ("/LUN_MIGRATION?range=[%(start)s-%(end)s]"
               % {"start": six.text_type(start), "end": six.text_type(end)})
        result = self.call(url, None, "GET")
        msg = _('Get migration task error.')
        self._assert_rest_result(result, msg)

        return result.get('data', [])

    def get_lun_migration_task(self):
        lun_migration_task = self._get_info_by_range(self._get_lun_migration_task)

        return lun_migration_task

    def delete_lun_migration(self, src_id, dst_id):
        url = '/LUN_MIGRATION/' + src_id
        result = self.call(url, None, "DELETE")
        msg = _('Delete lun migration error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

    def get_partition_id_by_name(self, name):
        url = "/cachepartition"
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get partition by name error.'))

        return self._get_id_from_result(result, name, 'NAME')

    def get_partition_info_by_id(self, partition_id):

        url = '/cachepartition/' + partition_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result,
                                 _('Get partition by partition id error.'))

        return result['data']

    def add_lun_to_partition(self, lun_id, partition_id):
        url = "/lun/associate/cachepartition"
        data = {"ID": partition_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id}
        result = self.call(url, data, "POST")
        self._assert_rest_result(result, _('Add lun to partition error.'))

    def remove_lun_from_partition(self, lun_id, partition_id):
        url = ('/lun/associate/cachepartition?ID=' + partition_id
               + '&ASSOCIATEOBJTYPE=11&ASSOCIATEOBJID=' + lun_id)

        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Remove lun from partition error.'))

    def get_cache_id_by_name(self, name):
        url = "/SMARTCACHEPARTITION"
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get cache by name error.'))

        return self._get_id_from_result(result, name, 'NAME')

    def get_cache_info_by_id(self, cacheid):
        url = "/SMARTCACHEPARTITION/" + cacheid
        data = {"TYPE": "273",
                "ID": cacheid}

        result = self.call(url, data, "GET")
        self._assert_rest_result(
            result, _('Get smartcache by cache id error.'))

        return result['data']

    def remove_lun_from_cache(self, lun_id, cache_id):
        url = "/SMARTCACHEPARTITION/REMOVE_ASSOCIATE"
        data = {"ID": cache_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id,
                "TYPE": 273}

        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Remove lun from cache error.'))

    def get_qos(self):
        url = "/ioclass"
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get QoS information error.'))
        return result

    def add_lun_to_qos(self, qos_id, lun_id, lun_list):
        """Add lun to QoS."""
        url = "/ioclass/" + qos_id
        new_lun_list = []
        lun_list_string = lun_list[1:-1]
        for lun_string in lun_list_string.split(","):
            tmp_lun_id = lun_string[1:-1]
            if '' != tmp_lun_id and tmp_lun_id != lun_id:
                new_lun_list.append(tmp_lun_id)

        new_lun_list.append(lun_id)

        data = {"LUNLIST": new_lun_list,
                "TYPE": 230,
                "ID": qos_id}
        result = self.call(url, data, "PUT")
        msg = _('Associate lun to QoS error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

    def add_lun_to_cache(self, lun_id, cache_id):
        url = "/SMARTCACHEPARTITION/CREATE_ASSOCIATE"
        data = {"ID": cache_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id,
                "TYPE": 273}
        result = self.call(url, data, "PUT")

        self._assert_rest_result(result, _('Add lun to cache error.'))

    def get_array_info(self):
        url = "/system/"
        result = self.call(url, None, "GET", filter_flag=True)
        self._assert_rest_result(result, _('Get array info error.'))
        return result.get('data', None)

    def remove_host(self, host_id):
        url = "/host/%s" % host_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Remove host from array error.'))

    def delete_hostgroup(self, hostgroup_id):
        url = "/hostgroup/%s" % hostgroup_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Delete hostgroup error.'))

    def remove_host_from_hostgroup(self, hostgroup_id, host_id):
        url_subfix001 = "/host/associate?TYPE=14&ID=%s" % hostgroup_id
        url_subfix002 = "&ASSOCIATEOBJTYPE=21&ASSOCIATEOBJID=%s" % host_id
        url = url_subfix001 + url_subfix002
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result,
                                 _('Remove host from hostgroup error.'))

    def remove_iscsi_from_host(self, initiator):
        url = "/iscsi_initiator/remove_iscsi_from_host"
        data = {"TYPE": '222',
                "ID": initiator}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Remove iscsi from host error.'))

    def get_host_online_fc_initiators(self, host_id):
        url = "/fc_initiator?PARENTTYPE=21&PARENTID=%s" % host_id
        result = self.call(url, None, "GET")

        initiators = []
        if 'data' in result:
            for item in result['data']:
                if (('PARENTID' in item) and (item['PARENTID'] == host_id)
                   and (item['RUNNINGSTATUS'] == constants.FC_INIT_ONLINE)):
                    initiators.append(item['ID'])

        return initiators

    def get_host_fc_initiators(self, host_id):
        url = "/fc_initiator?PARENTTYPE=21&PARENTID=%s" % host_id
        result = self.call(url, None, "GET")

        initiators = []
        if 'data' in result:
            for item in result['data']:
                if (('PARENTID' in item) and (item['PARENTID'] == host_id)):
                    initiators.append(item['ID'])

        return initiators

    def get_host_iscsi_initiators(self, host_id):
        url = "/iscsi_initiator?PARENTTYPE=21&PARENTID=%s" % host_id
        result = self.call(url, None, "GET")

        initiators = []
        if 'data' in result:
            for item in result['data']:
                if (('PARENTID' in item) and (item['PARENTID'] == host_id)):
                    initiators.append(item['ID'])

        return initiators

    def update_lun(self, lun_id, data):
        url = "/lun/" + lun_id
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Update lun properties error.'))

    def rename_lun(self, lun_id, new_name, description=None):
        url = "/lun/" + lun_id
        data = {"NAME": new_name}
        if description:
            data.update({"DESCRIPTION": description})
        result = self.call(url, data, "PUT")
        msg = _('Rename lun on array error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

    def rename_snapshot(self, snapshot_id, new_name, description=None):
        url = "/snapshot/" + snapshot_id
        data = {"NAME": new_name}
        if description:
            data.update({"DESCRIPTION": description})
        result = self.call(url, data, "PUT")
        msg = _('Rename snapshot on array error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

    def remove_fc_from_host(self, initiator):
        url = '/fc_initiator/remove_fc_from_host'
        data = {"TYPE": '223',
                "ID": initiator}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Remove fc from host error.'))

    def check_fc_initiators_exist_in_host(self, host_id):
        url = "/fc_initiator?PARENTID=%s&range=[0-1]" % host_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get host initiators info failed.'))
        if 'data' in result:
            return True

        return False

    def _fc_initiator_is_added_to_array(self, ininame):
        """Check whether the fc initiator is already added on the array."""
        url = "/fc_initiator/" + ininame
        result = self.call(url, None, "GET")
        err_str = _('Get fc initiator %s on array error.' % ininame)

        if result['error']['code'] != 0:
            self._assert_initiator_exist(result, err_str)
            return False

        return True

    def _add_fc_initiator_to_array(self, ininame):
        """Add a fc initiator to storage device."""
        url = '/fc_initiator/'
        data = {"TYPE": '223',
                "ID": ininame}
        result = self.call(url, data)
        self._assert_rest_result(result, _('Add fc initiator to array error.'))

    def ensure_fc_initiator_added(self, initiator_name, host_id, host_name):
        added = self._fc_initiator_is_added_to_array(initiator_name)
        if not added:
            self._add_fc_initiator_to_array(initiator_name)
        # Just add, no need to check whether have been added.
        self.add_fc_port_to_host(host_id, initiator_name)

        alua_info = self._find_alua_info(
            self.fc_info, initiator_name, host_name)

        LOG.info('Use ALUA %s when adding initiator to host.', alua_info)
        self._use_fc_alua(initiator_name, alua_info)

    def get_fc_ports(self):
        url = '/fc_port'
        result = self.call(url, None, "GET")
        msg = _('Get FC ports from array error.')
        self._assert_rest_result(result, msg)

        return result.get('data', [])

    def get_fc_initiator_count(self):
        url = '/fc_initiator/count'
        result = self.call(url, None, "GET")

        msg = _('Get fc initiator count error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return int(result['data']['COUNT'])

    def get_fc_initiator_on_array(self):
        count = self.get_fc_initiator_count()
        if count <= 0:
            return []

        fc_initiators = []
        for i in range((count - 1) / constants.MAX_QUERY_COUNT + 1):
            url = '/fc_initiator?range=[%d-%d]' % (
                i * constants.MAX_QUERY_COUNT,
                (i + 1) * constants.MAX_QUERY_COUNT)
            result = self.call(url, None, "GET")

            msg = _('Get FC initiators from array error.')
            self._assert_rest_result(result, msg)

            if 'data' in result:
                for item in result['data']:
                    fc_initiators.append(item['ID'])

        return fc_initiators

    def _get_hypermetro_domain(self, start, end, params):
        url = ("/HyperMetroDomain?range=[%(start)s-%(end)s]"
               % {"start": str(start), "end": str(end)})
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, "Get hyper domain info error.")
        return result.get('data', [])

    def get_hyper_domain_id(self, domain_name):
        domain_list = self._get_info_by_range(self._get_hypermetro_domain)
        for item in domain_list:
            if domain_name == item.get('NAME'):
                return item.get("ID")
        return None

    def create_hypermetro(self, hcp_param):
        url = "/HyperMetroPair"
        result = self.call(url, hcp_param, "POST")
        if result['error']['code'] == constants.HYPERMETRO_ALREADY_EXIST:
            hypermetro_info = self.get_hypermetro_by_lun_id(
                hcp_param["LOCALOBJID"])
            if hypermetro_info:
                return hypermetro_info

        if result['error']['code'] == constants.CREATE_HYPERMETRO_TIMEOUT:
            try_times = 2
            while try_times:
                time.sleep(constants.GET_VOLUME_WAIT_INTERVAL)
                LOG.info(_("Create SNAPSHOT TimeOut, try get snapshot "
                           "info in %s time"), 2 - try_times)
                hypermetro_info = self.get_hypermetro_by_lun_id(
                    hcp_param["LOCALOBJID"])
                if hypermetro_info:
                    return hypermetro_info
                else:
                    try_times -= 1

        msg = _('create_hypermetro_pair error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)
        return result['data']

    @utils.synchronized('huawei_delete_hypermetro_pair', external=True)
    def delete_hypermetro(self, metro_id):
        url = "/HyperMetroPair/" + metro_id
        result = self.call(url, None, "DELETE")

        msg = _('delete_hypermetro error.')
        self._assert_rest_result(result, msg)

    def sync_hypermetro(self, metro_id):
        url = "/HyperMetroPair/synchronize_hcpair"

        data = {"ID": metro_id,
                "TYPE": "15361"}
        result = self.call(url, data, "PUT")

        msg = _('sync_hypermetro error.')
        self._assert_rest_result(result, msg)

    def stop_hypermetro(self, metro_id):
        url = '/HyperMetroPair/disable_hcpair'

        data = {"ID": metro_id,
                "TYPE": "15361"}
        result = self.call(url, data, "PUT")

        msg = _('stop_hypermetro error.')
        self._assert_rest_result(result, msg)

    def get_hypermetro_by_id(self, metro_id):
        url = "/HyperMetroPair?filter=ID::%s&range=[0-100]" % metro_id
        result = self.call(url, None, "GET")
        msg = _('get_hypermetro_by_id error.')
        self._assert_rest_result(result, msg)
        if result.get('data'):
            return result['data'][0]

    def get_hypermetro_by_lun_name(self, lun_name):
        url = "/HyperMetroPair?filter=LOCALOBJNAME::%s" % lun_name
        result = self.call(url, None, "GET")
        msg = _('Get hypermetro by local lun name %s error.') % lun_name
        self._assert_rest_result(result, msg)
        if result.get('data'):
            return result['data'][0]

    def get_hypermetro_by_lun_id(self, lun_id):
        url = "/HyperMetroPair?filter=LOCALOBJID::%s" % lun_id
        result = self.call(url, None, "GET")
        msg = _('Get hypermetro by local lun id %s error.') % lun_id
        self._assert_rest_result(result, msg)
        if result.get('data'):
            return result['data'][0]

    def change_hostlun_id(self, map_info, hostlun_id):
        url = "/mappingview"
        view_id = six.text_type(map_info['view_id'])
        lun_id = six.text_type(map_info['lun_id'])
        hostlun_id = six.text_type(hostlun_id)
        data = {"TYPE": 245,
                "ID": view_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id,
                "ASSOCIATEMETADATA": [{"LUNID": lun_id,
                                       "hostLUNId": hostlun_id}]}

        result = self.call(url, data, "PUT")

        msg = 'change hostlun id error.'
        self._assert_rest_result(result, msg)

    def find_view_by_id(self, view_id):
        url = "/MAPPINGVIEW/" + view_id
        result = self.call(url, None, "GET")

        msg = _('Change hostlun id error.')
        self._assert_rest_result(result, msg)
        if 'data' in result:
            return result["data"]["AVAILABLEHOSTLUNIDLIST"]

    def get_metrogroup_by_name(self, name):
        url = "/HyperMetro_ConsistentGroup?type='15364'"
        result = self.call(url, None, "GET")

        msg = _('Get hypermetro group by name error.')
        self._assert_rest_result(result, msg)
        return self._get_id_from_result(result, name, 'NAME')

    def get_metrogroup_by_id(self, id):
        url = "/HyperMetro_ConsistentGroup/" + id
        result = self.call(url, None, "GET")

        msg = _('Get hypermetro group by id error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)
        return result['data']

    def create_metrogroup(self, name, description, domain_id):
        url = "/HyperMetro_ConsistentGroup"
        data = {"NAME": name,
                "TYPE": "15364",
                "DESCRIPTION": description,
                "RECOVERYPOLICY": "1",
                "SPEED": self.configuration.hyper_sync_speed,
                "PRIORITYSTATIONTYPE": "0",
                "DOMAINID": domain_id}
        result = self.call(url, data, "POST")

        msg = _('create hypermetro group error.')
        self._assert_rest_result(result, msg)
        if 'data' in result:
            return result["data"]["ID"]

    def delete_metrogroup(self, metrogroup_id):
        url = "/HyperMetro_ConsistentGroup/" + metrogroup_id
        result = self.call(url, None, "DELETE")

        msg = _('Delete hypermetro group error.')
        self._assert_rest_result(result, msg)

    def get_metrogroup(self, metrogroup_id):
        url = "/HyperMetro_ConsistentGroup/" + metrogroup_id
        result = self.call(url, None, "GET")

        msg = _('Get hypermetro group error.')
        self._assert_rest_result(result, msg)

    def stop_metrogroup(self, metrogroup_id):
        url = "/HyperMetro_ConsistentGroup/stop"
        data = {"TYPE": "15364",
                "ID": metrogroup_id
                }
        result = self.call(url, data, "PUT")

        msg = _('stop hypermetro group error.')
        self._assert_rest_result(result, msg)

    def sync_metrogroup(self, metrogroup_id):
        url = "/HyperMetro_ConsistentGroup/sync"
        data = {"TYPE": "15364",
                "ID": metrogroup_id
                }
        result = self.call(url, data, "PUT")

        msg = _('sync hypermetro group error.')
        self._assert_rest_result(result, msg)

    def add_metro_to_metrogroup(self, metrogroup_id, metro_id):
        url = "/hyperMetro/associate/pair"
        data = {"TYPE": "15364",
                "ID": metrogroup_id,
                "ASSOCIATEOBJTYPE": "15361",
                "ASSOCIATEOBJID": metro_id}
        result = self.call(url, data, "POST")

        msg = _('Add hypermetro to metrogroup error.')
        self._assert_rest_result(result, msg)

    def remove_metro_from_metrogroup(self, metrogroup_id, metro_id):
        url = "/hyperMetro/associate/pair"
        data = {"TYPE": "15364",
                "ID": metrogroup_id,
                "ASSOCIATEOBJTYPE": "15361",
                "ASSOCIATEOBJID": metro_id}
        result = self.call(url, data, "DELETE")

        msg = _('Delete hypermetro from metrogroup error.')
        self._assert_rest_result(result, msg)

    def _get_hypermetro_pairs(self, start, end, params):
        url = ("/HyperMetroPair?range=[%(start)s-%(end)s]"
               % {"start": six.text_type(start), "end": six.text_type(end)})
        result = self.call(url, None, "GET")
        msg = _('Get HyperMetroPair error.')
        self._assert_rest_result(result, msg)
        return result.get('data', [])

    def get_hypermetro_pairs(self):
        metro_pairs_list = self._get_info_by_range(self._get_hypermetro_pairs)

        return metro_pairs_list

    def _get_split_mirrors(self, start, end, params):
        url = ("/splitmirror?range=[%(start)s-%(end)s]"
               % {"start": six.text_type(start), "end": six.text_type(end)})
        result = self.call(url, None, "GET")
        if result['error']['code'] == constants.NO_SPLITMIRROR_LICENSE:
            msg = _('License is unavailable.')
            raise exception.VolumeBackendAPIException(data=msg)
        msg = _('Get SplitMirror error.')
        self._assert_rest_result(result, msg)

        return result.get('data', [])

    def get_split_mirrors(self):
        split_mirrors = self._get_info_by_range(self._get_split_mirrors)

        return split_mirrors

    def get_target_luns(self, id):
        url = ("/SPLITMIRRORTARGETLUN/targetLUN?TYPE=228&PARENTID=%s&"
               "PARENTTYPE=220") % id
        result = self.call(url, None, "GET")
        msg = _('Get target LUN of SplitMirror error.')
        self._assert_rest_result(result, msg)

        target_luns = []
        for item in result.get('data', []):
            target_luns.append(item.get('ID'))
        return target_luns

    def _get_migration_task(self, start, end, params):
        url = ("/LUN_MIGRATION?range=[%(start)s-%(end)s]"
               % {"start": six.text_type(start), "end": six.text_type(end)})
        result = self.call(url, None, "GET")
        if result['error']['code'] == constants.NO_MIGRATION_LICENSE:
            msg = _('License is unavailable.')
            raise exception.VolumeBackendAPIException(data=msg)
        msg = _('Get migration task error.')
        self._assert_rest_result(result, msg)

        return result.get('data', [])

    def get_migration_task(self):
        migration_tasks = self._get_info_by_range(self._get_migration_task)

        return migration_tasks

    def get_portgs_by_portid(self, port_id):
        portgs = []
        if not port_id:
            return portgs
        url = ("/portgroup/associate/fc_port?TYPE=257&ASSOCIATEOBJTYPE=212&"
               "ASSOCIATEOBJID=%s") % port_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get port groups by port error.'))
        for item in result.get("data", []):
            portgs.append(item["ID"])
        return portgs

    def get_views_by_portg(self, portg_id):
        views = []
        if not portg_id:
            return views
        url = ("/mappingview/associate/portgroup?TYPE=245&ASSOCIATEOBJTYPE="
               "257&ASSOCIATEOBJID=%s") % portg_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get views by port group error.'))
        for item in result.get("data", []):
            views.append(item["ID"])
        return views

    def get_lungroup_by_view(self, view_id):
        if not view_id:
            return None
        url = ("/lungroup/associate/mappingview?TYPE=256&ASSOCIATEOBJTYPE="
               "245&ASSOCIATEOBJID=%s") % view_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get LUN group by view error.'))
        for item in result.get("data", []):
            # In fact, there is just one lungroup in a view.
            return item["ID"]

    def get_portgroup_by_view(self, view_id):
        if not view_id:
            return None
        url = ("/portgroup/associate?ASSOCIATEOBJTYPE=245&"
               "ASSOCIATEOBJID=%s") % view_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get port group by view error.'))
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def get_fc_ports_by_portgroup(self, portg_id):
        url = ("/fc_port/associate?ASSOCIATEOBJTYPE=257"
               "&ASSOCIATEOBJID=%s") % portg_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(
            result, _('Get FC ports by port group error.'))
        ports = {}
        for item in result.get("data", []):
            ports[item["WWN"]] = item["ID"]
        return ports

    def create_portg(self, portg_name, description=""):
        url = "/PortGroup"
        data = {"DESCRIPTION": description,
                "NAME": portg_name,
                "TYPE": 257}
        result = self.call(url, data, "POST")
        self._assert_rest_result(result, _('Create port group error.'))
        if "data" in result:
            return result['data']['ID']

    def add_port_to_portg(self, portg_id, port_id):
        url = "/port/associate/portgroup"
        data = {"ASSOCIATEOBJID": port_id,
                "ASSOCIATEOBJTYPE": 212,
                "ID": portg_id,
                "TYPE": 257}
        result = self.call(url, data, "POST")
        self._assert_rest_result(result, _('Add port to port group error.'))

    def delete_portgroup(self, portg_id):
        url = "/PortGroup/%s" % portg_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Delete port group error.'))

    def remove_port_from_portgroup(self, portg_id, port_id):
        url = (("/port/associate/portgroup?ID=%(portg_id)s&TYPE=257&"
               "ASSOCIATEOBJTYPE=212&ASSOCIATEOBJID=%(port_id)s")
               % {"portg_id": portg_id, "port_id": port_id})
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Remove port from port group'
                                           ' error.'))

    def get_portg_info(self, portg_id):
        url = "/portgroup/%s" % portg_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get port group error.'))

        return result.get("data", {})

    def append_portg_desc(self, portg_id, description):
        portg_info = self.get_portg_info(portg_id)
        new_description = portg_info.get('DESCRIPTION') + ',' + description
        url = "/portgroup/%s" % portg_id
        data = {"DESCRIPTION": new_description,
                "ID": portg_id,
                "TYPE": 257}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('Append port group description '
                                           'error.'))

    def update_obj_desc(self, lun_id, description,
                        lun_type=constants.LUN_TYPE):
        cmd_type = 'lun' if lun_type == constants.LUN_TYPE else 'snapshot'
        url = ("/%s/%s" % (cmd_type, lun_id))
        data = {"DESCRIPTION": description,
                "ID": lun_id,
                "TYPE": lun_type}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, _('update object description '
                                           'error.'))

    def get_ports_by_portg(self, portg_id):
        wwns = []
        url = ("/fc_port/associate?TYPE=213&ASSOCIATEOBJTYPE=257"
               "&ASSOCIATEOBJID=%s" % portg_id)
        result = self.call(url, None, "GET")

        msg = _('Get ports by port group error.')
        self._assert_rest_result(result, msg)
        for item in result.get('data', []):
            wwns.append(item['WWN'])
        return wwns

    def get_remote_devices(self):
        url = "/remote_device"
        result = self.call(url, None, "GET", filter_flag=True)
        self._assert_rest_result(result, _('Get remote devices error.'))
        return result.get('data', [])

    def create_pair(self, pair_params):
        url = "/REPLICATIONPAIR"
        result = self.call(url, pair_params, "POST")

        msg = _('Create replication error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)
        return result['data']

    def get_pair_by_id(self, pair_id):
        url = "/REPLICATIONPAIR/" + pair_id
        result = self.call(url, None, "GET")

        msg = _('Get pair failed.')
        self._assert_rest_result(result, msg)
        return result.get('data', {})

    def switch_pair(self, pair_id):
        url = '/REPLICATIONPAIR/switch'
        data = {"ID": pair_id,
                "TYPE": "263"}
        result = self.call(url, data, "PUT")

        msg = _('Switch over pair error.')
        self._assert_rest_result(result, msg)

    def split_pair(self, pair_id):
        url = '/REPLICATIONPAIR/split'
        data = {"ID": pair_id,
                "TYPE": "263"}
        result = self.call(url, data, "PUT")

        msg = _('Split pair error.')
        self._assert_rest_result(result, msg)

    def delete_pair(self, pair_id, force=False):
        url = "/REPLICATIONPAIR/" + pair_id
        data = None
        if force:
            data = {"ISLOCALDELETE": force}

        result = self.call(url, data, "DELETE")

        msg = _('delete_replication error.')
        self._assert_rest_result(result, msg)

    def sync_pair(self, pair_id):
        url = "/REPLICATIONPAIR/sync"
        data = {"ID": pair_id,
                "TYPE": "263"}
        result = self.call(url, data, "PUT")

        msg = _('Sync pair error.')
        self._assert_rest_result(result, msg)

    def check_pair_exist(self, pair_id):
        url = "/REPLICATIONPAIR/" + pair_id
        result = self.call(url, None, "GET")
        error_code = result['error']['code']
        if error_code != 0:
            if error_code == constants.REPLICATIONPAIR_NOT_EXIST:
                return False
            msg = (_("Check replication pair exist error.\nresult: %(res)s.")
                   % {'res': result})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return True

    def set_pair_second_access(self, pair_id, access):
        url = "/REPLICATIONPAIR/" + pair_id
        data = {"ID": pair_id,
                "SECRESACCESS": access}
        result = self.call(url, data, "PUT")

        msg = _('Set pair secondary access error.')
        self._assert_rest_result(result, msg)

    def get_pair_info_by_lun_id(self, lun_id):
        if not lun_id:
            return None

        url = ("/REPLICATIONPAIR/associate?ASSOCIATEOBJTYPE=11&"
               "ASSOCIATEOBJID=%s" % lun_id)
        result = self.call(url, None, "GET")
        msg = _('Get replication pair info by lun id error.')
        self._assert_rest_result(result, msg)
        for info in result.get("data", []):
            if info.get("LOCALRESID") == lun_id:
                return info
        LOG.warning("Can not get the replica pair with the lun id: %s", lun_id)
        return None

    def is_host_associated_to_hostgroup(self, host_id):
        url = "/host/" + host_id
        result = self.call(url, None, "GET")
        data = result.get('data')
        if data is not None:
            return data.get('ISADD2HOSTGROUP') == 'true'
        return False

    def _get_object_count(self, obj_name):
        url = "/" + obj_name + "/count"
        result = self.call(url, None, "GET", filter_flag=True)

        if result['error']['code'] != 0:
            msg = 'Get obj %s count error' % obj_name
            raise exception.VolumeBackendAPIException(data=msg)

        if result.get("data"):
            return result.get("data").get("COUNT")

    def create_replicg(self, replicg_param):
        url = '/CONSISTENTGROUP'
        result = self.call(url, replicg_param, "POST")

        msg = (_("Create replication group %(res)s error.")
               % {'res': replicg_param['DESCRIPTION']})
        self._assert_rest_result(result, msg)

    def get_replicg_by_name(self, group_name):
        url = "/CONSISTENTGROUP?filter=NAME::%s" % group_name
        result = self.call(url, None, 'GET')

        msg = (_("Get replication consisgroup %(name)s failed.")
               % {'name': group_name})
        self._assert_rest_result(result, msg)
        if 'data' in result:
            return result['data'][0]
        else:
            return {}

    def add_replipair_to_replicg(self, replicg_id, pair_ids):
        url = '/ADD_MIRROR'
        data = {'ID': replicg_id,
                'RMLIST': pair_ids}
        result = self.call(url, data, "PUT")
        msg = (_("Add repli_pair %(pair)s to replicg %(group)s error.")
               % {'pair': pair_ids, 'group': replicg_id})
        self._assert_rest_result(result, msg)

    def remove_replipair_from_replicg(self, replicg_id, pair_ids):
        url = '/DEL_MIRROR'
        data = {'ID': replicg_id,
                'RMLIST': pair_ids}
        result = self.call(url, data, "PUT")
        msg = (_("Remove repli_pair %(pair)s from "
                 "replicg %(group)s error.")
               % {'pair': pair_ids, 'group': replicg_id})
        self._assert_rest_result(result, msg)

    def split_replicg(self, replicg_id):
        url = '/SPLIT_CONSISTENCY_GROUP'
        data = {'ID': replicg_id}
        result = self.call(url, data, "PUT")
        msg = (_("Split replicg %(group)s error.")
               % {'group': replicg_id})
        self._assert_rest_result(result, msg)

    def delete_replicg(self, replicg_id):
        url = '/CONSISTENTGROUP/%s?ISLOCALDELETE=0' % replicg_id
        result = self.call(url, None, "DELETE")
        msg = (_("Delete replicg %(group)s error.")
               % {'group': replicg_id})
        self._assert_rest_result(result, msg)

    def sync_replicg(self, replicg_id):
        url = '/SYNCHRONIZE_CONSISTENCY_GROUP'
        data = {'ID': replicg_id}
        result = self.call(url, data, "PUT")
        if result['error']['code'] == constants.REPLICG_IS_EMPTY:
            LOG.warning(("Replicg %(group)s does not have "
                         "remote replications.")
                        % {'group': replicg_id})
            return
        msg = (_("Sync replicg %(group)s error.")
               % {'group': replicg_id})
        self._assert_rest_result(result, msg)

    def get_replicg_info(self, replicg_id):
        url = '/CONSISTENTGROUP/%s' % replicg_id
        result = self.call(url, None, 'GET')

        msg = (_("Get replication consisgroup %(group)s failed.")
               % {'group': replicg_id})
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)
        return result['data']

    def set_cg_second_access(self, replicg_id, access):
        url = "/CONSISTENTGROUP/" + replicg_id
        data = {"SECRESACCESS": access}
        result = self.call(url, data, "PUT")

        msg = (_("Set cg %(group)s secondary access"
                 " to %(access)% failed.")
               % {'group': replicg_id, 'access': access})
        self._assert_rest_result(result, msg)

    def switch_replicg(self, replicg_id):
        url = '/SWITCH_GROUP_ROLE'
        data = {'ID': replicg_id}
        result = self.call(url, data, 'PUT')

        msg = (_("Switch replication consisgroup "
                 "%(group)s error.")
               % {'group': replicg_id})
        self._assert_rest_result(result, msg)

    def get_controller_by_name(self, name):
        controlers = self._get_all_controllers()
        for controller in controlers:
            if controller.get('LOCATION') == name:
                return controller.get('ID')

        return None

    def _get_all_controllers(self):
        url = "/controller"
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get all controller error.'))
        return result.get('data', [])

    def get_license_feature_status(self):
        url = "/license/feature"
        result = self.call(url, None, "GET", filter_flag=True)
        if result['error']['code'] != 0:
            msg = (_("Query license status of features failed.\n"
                     "result: %(res)s.")
                   % {'res': result})
            LOG.error(msg)
            return {}
        dic_result = {}
        for i in result.get('data', []):
            for key, value in i.items():
                dic_result[key] = value

        return dic_result

    def create_clone_lun(self, src_id, lun_name):
        data = {
            "CLONESOURCEID": src_id,
            "ISCLONE": True,
            "NAME": lun_name,
        }

        result = self.call('/lun', data, "POST")
        self._assert_rest_result(result, _('Create clone lun error.'))
        return result['data']

    def split_clone_lun(self, clone_id):
        data = {
            "ID": clone_id,
            "SPLITACTION": 1,
            "ISCLONE": True,
            "SPLITSPEED": 4,
        }

        result = self.call('/lunclone_split_switch', data, "PUT")
        if result['error']['code'] == constants.CLONE_PAIR_SYNC_NOT_EXIST:
            return
        self._assert_rest_result(result, _('Split clone lun error.'))

    def stop_split_lunclone(self, clone_id):
        data = {
            "ID": clone_id,
            "SPLITACTION": 2,
            "ISCLONE": True,
            "SPLITSPEED": 4,
        }
        result = self.call('/lunclone_split_switch', data, "PUT")
        if result['error']['code'] == constants.CLONE_PAIR_SYNC_COMPLETE:
            LOG.info("Split lun finish, delete the clone pair %s." % clone_id)
            self.delete_clone_pair(clone_id)
            return
        self._assert_rest_result(result, _('Stop split clone lun error.'))

    def get_workload_type_id(self, workload_type_name):
        url = "/workload_type?filter=NAME::%s" % workload_type_name
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get workload type error'))

        for item in result.get("data", []):
            if item.get("NAME") == workload_type_name:
                return item.get("ID")

    def get_workload_type_name(self, workload_type_id):
        url = "/workload_type/%s" % workload_type_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get workload type by id error'))
        return result.get("data", {}).get("NAME")

    def create_clone_pair(self, source_id, target_id, clone_speed):
        url = "/clonepair/relation"
        data = {"copyRate": clone_speed,
                "sourceID": source_id,
                "targetID": target_id,
                "isNeedSynchronize": "0"}
        result = self.call(url, data, "POST")
        self._assert_rest_result(result, 'Create ClonePair error, source_id '
                                         'is %s.' % source_id)
        return result['data']['ID']

    def sync_clone_pair(self, pair_id):
        url = "/clonepair/synchronize"
        data = {"ID": pair_id, "copyAction": 0}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, 'Sync ClonePair error, pair is %s.'
                                 % pair_id)

    def stop_clone_pair(self, pair_id):
        url = "/clonepair/synchronize"
        data = {"ID": pair_id, "copyAction": 2}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, 'Stop ClonePair error, pair is %s.' % pair_id)

    def get_clone_pair_info(self, pair_id):
        url = "/clonepair/%s" % pair_id
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, 'Get ClonePair %s error.' % pair_id)
        return result.get('data', {})

    def delete_clone_pair(self, pair_id, delete_dst_lun=False):
        data = {"ID": pair_id,
                "isDeleteDstLun": delete_dst_lun}
        url = "/clonepair/%s" % pair_id
        result = self.call(url, data, "DELETE")
        if result['error']['code'] == constants.CLONE_PAIR_NOT_EXIST:
            LOG.warning('ClonePair %s to delete not exist.', pair_id)
            return
        self._assert_rest_result(result, 'Delete ClonePair %s error.'
                                 % pair_id)

    def get_host_by_hostgroup_id(self, hostgroup_id):
        url = ("/host/associate?ASSOCIATEOBJTYPE=14&ASSOCIATEOBJID=%s"
               % hostgroup_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, 'Get host by hostgroup %s error.'
                                 % hostgroup_id)
        return result.get("data", [])

    def get_mappingview_by_lungroup_id(self, lungroup_id):
        url = ('/mappingview/associate?ASSOCIATEOBJTYPE=256&ASSOCIATEOBJID=%s'
               % lungroup_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, 'Get mappingviews by lungroup %s'
                                         ' error.' % lungroup_id)
        return result.get("data", [])

    def rollback_snapshot(self, snapshot_id, speed):
        url = '/snapshot/rollback'
        data = {'ID': snapshot_id,
                'ROLLBACKSPEED': speed}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, 'Rollback snapshot %s error.'
                                 % snapshot_id)

    def cancel_rollback_snapshot(self, snapshot_id):
        url = '/snapshot/cancelrollback'
        data = {'ID': snapshot_id}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result, 'Cancel rollback snapshot %s error.'
                                 % snapshot_id)

    def ensure_roceini_added(self, initiator_name, host_id):
        # Check and associate RoCE initiator to host on array
        initiator = self._get_roceini_by_id(initiator_name)

        if not initiator:
            self._add_roceini_to_array(initiator_name)
            self._associate_roceini_to_host(initiator_name, host_id)
            return

        if initiator.get('ISFREE') == "true":
            self._associate_roceini_to_host(initiator_name, host_id)
            return

        # if initiator was associated to another host
        if initiator.get("PARENTID") != host_id:
            msg = (_("Initiator %(ini)s has been added to another host "
                     "%(host)s.") % {"ini": initiator_name,
                                     "host": initiator.get('PARENTNAME')})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _get_roceini_by_id(self, nqn):
        """Get RoCE initiator from array."""
        url = '/NVMe_over_RoCE_initiator/%s' % nqn
        result = self.call(url, None, "GET")

        if result.get('error', {}).get('code') == constants.FC_INITIATOR_NOT_EXIST:
            LOG.warning('RoCE NQN %s not exist.', nqn)
            return {}
        self._assert_rest_result(result, 'get RoCE NQN %s error.' % nqn)

        return result.get("data", {})

    def _add_roceini_to_array(self, nqn):
        """Add a new RoCE initiator to storage device."""
        url = "/NVMe_over_RoCE_initiator"
        data = {"ID": nqn}
        result = self.call(url, data, "POST")
        if result.get('error', {}).get('code') == constants.OBJECT_ALREADY_EXIST:
            LOG.warning('RoCE NQN %s has already exist in array.', nqn)
        else:
            self._assert_rest_result(
                result, _('Add RoCE initiator %s to array error.' % nqn))

    def _associate_roceini_to_host(self, nqn, host_id):
        """Associate RoCE initiator with the host."""
        url = "/host/create_associate"
        data = {"ASSOCIATEOBJTYPE": constants.NVME_ROCE_INITIATOR_TYPE,
                "ID": host_id,
                "ASSOCIATEOBJID": nqn}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(
            result, _("Associate RoCE initiator %(ini)s to host %(host)s "
                      "error." % {"ini": nqn, "host": host_id}))

    def is_roce_initiator_associated_to_host(self, initiator_name, host_id):
        initiator = self._get_roceini_by_id(initiator_name)
        if not initiator or initiator.get('ISFREE') == "true":
            return False

        if initiator.get('PARENTID') == host_id:
            return True
        else:
            msg = _("Initiator %(ini)s has been added to host "
                    "%(host)s.") % {"ini": initiator_name,
                                    "host": initiator.get('PARENTID')}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def remove_roce_initiator_from_host(self, initiator_name, host_id):
        url = "/host/remove_associate"
        data = {"ID": host_id,
                "ASSOCIATEOBJTYPE": constants.NVME_ROCE_INITIATOR_TYPE,
                "ASSOCIATEOBJID": initiator_name}
        result = self.call(url, data, "PUT")
        self._assert_rest_result(result,
                                 _('Remove RoCE initiator from host error.'))

    def get_roce_params(self, connector):
        """Get target ROCE params, including IP."""
        host_nqn = connector.get('host_nqn')
        host_name = connector.get('host')
        target_ips = self._get_roce_target_ips(host_nqn, host_name)

        logic_ports = self.get_roce_logical_ports()
        result = []
        for ip in target_ips:
            if self._is_roce_target_ip_in_array(ip, logic_ports):
                format_ip = netaddr.IPAddress(ip)
                if format_ip.version == 6:
                    ip = str(format_ip.format(dialect=netaddr.ipv6_compact))
                    ip = '[' + ip + ']'
                result.append(ip)

        if not result:
            err_msg = _('There is no any logic ips exist on array of the '
                        'configured target_ip %s in conf file' % target_ips)
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        return result

    def _get_roce_target_ips(self, initiator, host_name):
        target_ips = self._get_target_ips_by_initiator_name(initiator)

        if not target_ips:
            target_ips = self._get_target_ips_by_host_name(host_name)

        if not target_ips:
            msg = (_(
                'get_roce_params: Failed to get target IP '
                'for host %(host)s, please check config file.')
                   % {'host': host_name})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.info('Get the default ip: %s.', target_ips)
        return target_ips

    def _get_roce_logic_ports(self, start, end, params):
        url = ("/lif?range=[%(start)s-%(end)s]"
               % {"start": six.text_type(start), "end": six.text_type(end)})
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('get RoCE Logic Ports error.'))
        return result.get('data', [])

    def get_roce_logical_ports(self):
        all_logic_ports = self._get_info_by_range(
            self._get_roce_logic_ports)
        return all_logic_ports

    def _is_roce_target_ip_in_array(self, ip, logic_ports):
        for logic_port in logic_ports:
            if logic_port.get('ADDRESSFAMILY') == constants.ADDRESS_FAMILY_IPV4:
                if ip == logic_port.get('IPV4ADDR'):
                    return True
            else:
                if self._is_same_ipv6(ip, logic_port.get('IPV6ADDR')):
                    return True

        return False

    @staticmethod
    def _is_same_ipv6(left_ip, right_ip):
        format_left_ip = str(
            netaddr.IPAddress(left_ip).format(dialect=netaddr.ipv6_compact))
        format_right_ip = str(
            netaddr.IPAddress(right_ip).format(dialect=netaddr.ipv6_compact))
        if format_left_ip == format_right_ip:
            return True

        return False

    @staticmethod
    def _get_target_ip_list(roce_info, target_ips):
        for target_ip in roce_info.get('TargetIP').split():
            if target_ip.strip():
                target_ips.append(target_ip)

    def _get_target_ips_by_initiator_name(self, initiator):
        target_ips = []
        for info in self.roce_info:
            config_initiator = info.get('Name')
            if not config_initiator:
                continue
            if config_initiator == initiator:
                self._get_target_ip_list(info, target_ips)
        return target_ips

    def _get_target_ips_by_host_name(self, host_name):
        target_ips = []
        temp_target_ips = []
        for info in self.roce_info:
            config_host_name = info.get('HostName')
            if not config_host_name:
                continue
            if config_host_name == '*':
                self._get_target_ip_list(info, temp_target_ips)
            elif re.search(config_host_name, host_name):
                self._get_target_ip_list(info, target_ips)
                break

        if not target_ips and temp_target_ips:
            target_ips = temp_target_ips

        return target_ips
