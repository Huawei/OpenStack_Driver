# Copyright (c) 2025 Huawei Technologies Co., Ltd.
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
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants
from cinder.volume.drivers.huawei import rest_client

LOG = logging.getLogger(__name__)


class RestClientExtend(rest_client.RestClient):
    def __init__(self, configuration, san_address, san_user, san_password,
                 **kwargs):
        super(RestClientExtend, self).__init__(
            configuration, san_address, san_user, san_password, **kwargs)

    def add_roce_host(self, host_name, initiator):
        host_id = self.get_host_id_by_name(host_name)
        if host_id:
            return host_id

        alua_info = self._find_new_alua_info(self.roce_info, host_name, initiator)

        try:
            host_id = self._add_host(host_name, host_name, alua_info)
        except Exception as err:
            LOG.info(
                'Failed to create host: %(name)s. Check if it exists on the array.',
                {constants.NAME: host_name})
            host_id = self.get_host_id_by_name(host_name)
            if not host_id:
                msg = _('Failed to create host: %(name)s. '
                        'Please check if it exists on the array, err:%(error)s'
                        ) % {constants.NAME: host_name, 'error': err}
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        LOG.info(
            'add_roce_host. create host success. host name: %(name)s, host id: %(id)s',
            {constants.NAME: host_name, 'id': host_id})
        return host_id

    def get_mapping(self, host_id, lun_id):
        url = ('/mapping?hostId=%(host_id)s&lunId=%(lun_id)s'
               % {"host_id": host_id, "lun_id": lun_id})
        result = self.call(url, None, "GET")
        if result.get('error', {}).get('code') == constants.HOST_LUN_MAPPING_NOT_EXIST:
            LOG.info('Mapping between host %s and lun %s does not exist.',
                     host_id, lun_id)
            return {}

        self._assert_rest_result(result, _('Get mapping between host %(host)s and lun %(lun)s error.'
                                           % {"host": host_id, "lun": lun_id}))
        return result['data']

    def get_mapped_host_info(self, lun_id):
        url = ('/mapping/associate?ASSOCIATEOBJTYPE=11&ASSOCIATEOBJID=%(obj_id)s'
               '&range=[0-100]' % {"obj_id": lun_id})
        result = self.call(url, None, "GET")
        self._assert_rest_result(result, _('Get mapped host info error.'))
        return result['data']

    def get_mapped_lun_info(self, host_id):
        url = ('/mapping/associate?ASSOCIATEOBJTYPE=21&ASSOCIATEOBJID=%(obj_id)s'
               '&range=[0-100]' % {"obj_id": host_id})
        result = self.call(url, None, "GET")
        if result.get('error', {}).get('code') == constants.HOST_NOT_EXIST:
            LOG.warning("Host %s does not exist.", host_id)
            return []

        self._assert_rest_result(result, _('Get mapped lun info error.'))
        return result['data']

    def create_mapping(self, host_id, lun_id):
        data = {
            'hostId': host_id,
            'lunId': lun_id,
            'force': True,
            'hostLunIdStart': 1
        }
        url = '/mapping'
        result = self.call(url, data, "POST")
        if result.get('error', {}).get('code') == constants.HOST_LUN_MAPPING_ALREADY_EXIST:
            LOG.info('Mapping between host %s and lun %s already exists.',
                     host_id, lun_id)
            return

        self._assert_rest_result(result, _('Create mapping between host %(host)s and lun %(lun)s error.'
                                           % {"host": host_id, "lun": lun_id}))

    def delete_mapping(self, host_id, lun_id):
        data = {
            'hostId': host_id,
            'lunId': lun_id
        }
        url = '/mapping'
        result = self.call(url, data, "DELETE")
        if result.get('error', {}).get('code') == constants.HOST_LUN_MAPPING_NOT_EXIST:
            LOG.info('Mapping between host %s and lun %s does not exist.',
                     host_id, lun_id)
            return

        self._assert_rest_result(result, _('Delete mapping between host %(host)s and lun %(lun)s error.'
                                           % {"host": host_id, "lun": lun_id}))
