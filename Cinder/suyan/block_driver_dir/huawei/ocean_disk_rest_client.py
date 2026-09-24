# Copyright (c) 2026 Huawei Technologies Co., Ltd.
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
import time

from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import rest_client

LOG = logging.getLogger(__name__)


class OceanDiskRestClientExtend(rest_client.RestClient):
    def __init__(self, configuration, san_address, san_user, san_password,
                 **kwargs):
        super(OceanDiskRestClientExtend, self).__init__(
            configuration, san_address, san_user, san_password, **kwargs)

    def create_lun(self, lun_params):
        lun_params['MIRRORPOLICY'] = '1'
        url = "/namespace"
        result = self.call(url, lun_params)
        if result.get('error', {}).get('code') == constants.ERROR_VOLUME_ALREADY_EXIST:
            lun_id = self.get_lun_id_by_name(lun_params['NAME'])
            if lun_id:
                return self.get_lun_info(lun_id)

        if result.get('error', {}).get('code') == constants.ERROR_VOLUME_TIMEOUT:
            try_times = constants.TIMEOUT_TRY_TIMES
            while try_times:
                try_times -= 1
                time.sleep(constants.GET_VOLUME_WAIT_INTERVAL)
                LOG.info(("Create Namespace TimeOut, try get namespace info in %s "
                          "time"), constants.TIMEOUT_TRY_TIMES - try_times)
                lun_id = self.get_lun_id_by_name(lun_params['NAME'])
                if lun_id:
                    return self.get_lun_info(lun_id)

        msg = _('Create namespace error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result.get('data')

    def check_lun_exist(self, lun_id, lun_wwn=None):
        url = "/namespace/%s" % lun_id
        result = self.call(url, None, "GET")
        error_code = result.get('error', {}).get('code')
        if error_code != 0:
            if error_code == constants.ERROR_NAMESPACE_NOT_EXIST:
                LOG.warning("Can't find namespace %s on the array.", lun_id)
                return False
            else:
                msg = (_("Check namespace exist error."))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if lun_wwn and result.get('data', {}).get('WWN') != lun_wwn:
            LOG.debug("Namespace ID %(id)s with WWN %(wwn)s does not exist on "
                      "the array.", {"id": lun_id, "wwn": lun_wwn})
            return False

        return True

    def delete_lun(self, lun_id):
        url = "/namespace/%s" % lun_id
        data = {"TYPE": "11", "ID": lun_id}
        result = self.call(url, data, "DELETE")
        if result.get('error', {}).get('code') == constants.ERROR_NAMESPACE_NOT_EXIST:
            return
        self._assert_rest_result(result, _('Delete namespace error.'))

    def get_lun_id_by_name(self, name):
        if not name:
            return None

        url = "/namespace?filter=NAME::%s&range=[0-100]" % name
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get namespace id by name error.'))

        return self._get_id_from_result(result, name, 'NAME')

    def get_lun_info(self, lun_id, lun_type=constants.LUN_TYPE):
        url = "/namespace/%s" % lun_id
        result = self.call(url, None, "GET")

        msg = _('Get namespace info error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result.get('data')

    def extend_lun(self, lun_id, new_volume_size):
        url = "/namespace/expand"
        data = {
            "TYPE": 11, "ID": lun_id,
            "CAPACITY": new_volume_size
        }
        result = self.call(url, data, 'PUT')

        msg = _('Extend namespace error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result.get('data')

    def associate_lun_to_lungroup(
            self, lungroup_id, lun_id, lun_type=constants.LUN_TYPE, is_associated_host=False):
        url = "/namespacegroup/associate"
        data = {
            "ID": lungroup_id,
            "ASSOCIATEOBJTYPE": lun_type,
            "ASSOCIATEOBJID": lun_id
        }
        if is_associated_host and self.is_dorado_v6:
            data["startHostLunId"] = 1
        result = self.call(url, data)
        if result.get('error', {}).get('code') == constants.NAMESPACE_ALREADY_IN_NAMESPACEGROUP:
            return
        self._assert_rest_result(result, _('Associate namespace to namespacegroup error.'))

    def get_lungroupids_by_lunid(self, lun_id, lun_type=constants.LUN_TYPE):
        url = ("/namespacegroup/associate?TYPE=256"
               "&ASSOCIATEOBJTYPE=%s&ASSOCIATEOBJID=%s" % (lun_type, lun_id))

        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get namespacegroup id by lun id error.'))

        lungroup_ids = []
        if 'data' in result:
            for item in result['data']:
                lungroup_ids.append(item['ID'])

        return lungroup_ids

    def remove_lun_from_lungroup(self, lungroup_id, lun_id,
                                 lun_type=constants.LUN_TYPE):
        """Remove lun from lungroup."""
        url = ("/namespacegroup/associate?ID=%s&ASSOCIATEOBJTYPE=%s"
               "&ASSOCIATEOBJID=%s" % (lungroup_id, lun_type, lun_id))

        result = self.call(url, None, 'DELETE')
        self._assert_rest_result(
            result, _('Delete associated namespace from namespacegroup error.'))

    def get_host_lun_id(self, host_id, lun_info, lun_type=constants.LUN_TYPE):
        url = ("/namespace/associate?TYPE=%s&ASSOCIATEOBJTYPE=21&ASSOCIATEOBJID=%s"
               "&filter=NAME::%s&selectFields=ID,NAME,ASSOCIATEMETADATA,WWN"
               % (lun_type, host_id, lun_info['NAME']))
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find host namespace id error.'))

        host_namespace_id = 1
        for item in result.get('data', []):
            if not lun_info['ID'] == item['ID']:
                break
            associate_data = item['ASSOCIATEMETADATA']
            try:
                hostassoinfo = json.loads(associate_data)
                host_namespace_id = hostassoinfo['hostNamespaceID']
                break
            except Exception as err:
                LOG.error("JSON transfer data error. %s.", err)
                raise
        return host_namespace_id

    def find_lungroup_from_map(self, view_id):
        """Get lungroup from the given map"""
        url = ("/mappingview/associate/namespacegroup?TYPE=256&"
               "ASSOCIATEOBJTYPE=245&ASSOCIATEOBJID=%s" % view_id)
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find namespace group from mapping view '
                                 'error.'))
        lungroup_id = None
        if 'data' in result:
            # One map can have only one lungroup.
            for item in result['data']:
                lungroup_id = item['ID']

        return lungroup_id

    def get_qosid_by_lunid(self, lun_id):
        """Get QoS id by name use fuzzy query."""
        qos_name_prefix = constants.QOS_NAME_PREFIX + lun_id + '_'
        url = "/ioclass?filter=NAME:%s" % qos_name_prefix
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get QoS id name error.'))

        for item in result.get('data', []):
            if str(lun_id) in item.get('LUNLIST', []):
                return item.get('ID')

        return ''

    def delete_lungroup(self, lungroup_id):
        url = "/namespacegroup/%s" % lungroup_id
        result = self.call(url, None, "DELETE")
        self._assert_rest_result(result, _('Delete namespacegroup error.'))

    def _is_lun_associated_to_lungroup(self, lungroup_id, lun_info,
                                       lun_type=constants.LUN_TYPE):
        """Check whether the lun is associated to the lungroup."""
        url = ("/namespace/associate?TYPE=%s&ASSOCIATEOBJTYPE=256&ASSOCIATEOBJID=%s"
               "&filter=NAME::%s&selectFields=ID,NAME,ASSOCIATEMETADATA,WWN"
               % (lun_type, lungroup_id, lun_info['NAME']))

        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Check namespacegroup associate error.'))

        if self._get_id_from_result(result, lun_info['ID'], 'ID'):
            return True

        return False

    def _find_lungroup(self, lungroup_name):
        url = "/namespacegroup?filter=NAME::%s&range=[0-100]" % lungroup_name
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get namespacegroup information error.'))

        return self._get_result_id(result)

    def _create_lungroup(self, lungroup_name):
        url = "/namespacegroup"
        data = {
            "DESCRIPTION": lungroup_name,
            "APPTYPE": '0',
            "GROUPTYPE": '0',
            "NAME": lungroup_name
        }
        result = self.call(url, data)

        msg = _('Create namespacegroup error.')
        self._assert_rest_result(result, msg)
        self._assert_data_in_result(result, msg)

        return result.get('data', {}).get('ID')

    def _get_obj_count_from_lungroup_by_type(self, lungroup_id,
                                             lun_type=constants.LUN_TYPE):
        if lun_type != constants.LUN_TYPE:
            return 0

        lunnum = 0
        if not lungroup_id:
            return lunnum

        url = ("/namespace/count?TYPE=%s&ASSOCIATEOBJTYPE=256&"
               "ASSOCIATEOBJID=%s" % (lun_type, lungroup_id))
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Find obj number error.'))
        if 'data' in result:
            lunnum = int(result['data']['COUNT'])
        return lunnum
