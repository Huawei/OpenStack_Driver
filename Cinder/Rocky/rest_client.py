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

import functools
import inspect
import json
import requests
import six
import sys
import threading
import time

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants

from oslo_concurrency import lockutils
from oslo_log import log as logging
from requests.adapters import HTTPAdapter


LOG = logging.getLogger(__name__)


def _error_code(result):
    return result['error']['code']


# To limit the requests concurrently sent to array
_semaphore = threading.Semaphore(20)


def obj_operation_wrapper(func):
    @functools.wraps(func)
    def wrapped(self, url_format=None, **kwargs):
        url = self._obj_url
        if url_format:
            url += url_format % kwargs

        _semaphore.acquire()

        try:
            result = func(self, url, **kwargs)
        except requests.HTTPError as exc:
            return {"error": {"code": exc.response.status_code,
                              "description": six.text_type(exc)}}
        finally:
            _semaphore.release()

        return result

    return wrapped


class CommonObject(object):
    def __init__(self, client):
        self.client = client

    @obj_operation_wrapper
    def post(self, url, **kwargs):
        return self.client.post(url, **kwargs)

    @obj_operation_wrapper
    def put(self, url, **kwargs):
        return self.client.put(url, **kwargs)

    @obj_operation_wrapper
    def delete(self, url, **kwargs):
        return self.client.delete(url, **kwargs)

    @obj_operation_wrapper
    def get(self, url, **kwargs):
        return self.client.get(url, **kwargs)


def _assert_result(result, msg_format, *args):
    if _error_code(result) != 0:
        args += (result,)
        msg = (msg_format + '\nresult: %s.') % args
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)


class Lun(CommonObject):
    _obj_url = '/lun'

    def create_lun(self, lun_params):
        # Set the mirror switch always on
        lun_params['MIRRORPOLICY'] = '1'
        result = self.post(data=lun_params)
        _assert_result(result, 'Create lun %s error.', lun_params)
        return result['data']

    def create_lunclone(self, src_id, lun_name):
        data = {
            "CLONESOURCEID": src_id,
            "ISCLONE": True,
            "NAME": lun_name,
        }
        result = self.post(data=data)
        _assert_result(result, 'Create clone lun for source ID %s error.',
                       src_id)
        return result['data']

    def delete_lun(self, lun_id):
        result = self.delete('/%(lun)s', lun=lun_id)
        if _error_code(result) == constants.ERROR_LUN_NOT_EXIST:
            LOG.warning("LUN %s to delete does not exist.", lun_id)
            return
        _assert_result(result, 'Delete lun %s error.', lun_id)

    def get_lun_info_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Get lun info by name %s error.', name)
        if result.get('data'):
            return result['data'][0]

    def update_lun(self, lun_id, data):
        result = self.put('/%(id)s', id=lun_id, data=data)
        _assert_result(result, 'Update lun %s properties %s error.',
                       lun_id, data)

    def extend_lun(self, lun_id, new_size):
        data = {'ID': lun_id,
                'CAPACITY': new_size}
        result = self.put('/expand', data=data)
        _assert_result(result, 'Extend lun %s capacity error.', lun_id)

    def add_lun_to_partition(self, lun_id, partition_id):
        data = {"ID": partition_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id}
        result = self.post('/associate/cachepartition', data=data)
        _assert_result(result, 'Add lun %s to partition %s error.',
                       lun_id, partition_id)

    def remove_lun_from_partition(self, lun_id, partition_id):
        data = {"ID": partition_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id}
        result = self.delete('/associate/cachepartition', data=data)
        _assert_result(result, 'Remove lun %s from partition %s error.',
                       lun_id, partition_id)

    def rename_lun(self, lun_id, new_name, description=None):
        data = {"NAME": new_name}
        if description:
            data.update({"DESCRIPTION": description})
        result = self.put('/%(id)s', id=lun_id, data=data)
        _assert_result(result, 'Rename lun %s to %s error.', lun_id, new_name)

    def get_lun_count_of_lungroup(self, lungroup_id):
        result = self.get("/count?ASSOCIATEOBJTYPE=256&ASSOCIATEOBJID=%(id)s",
                          id=lungroup_id)
        _assert_result(result, 'Get lun count of lungroup %s error.',
                       lungroup_id)
        return int(result['data']['COUNT'])

    def get_lun_info_by_id(self, lun_id):
        result = self.get("/%(id)s", id=lun_id)
        _assert_result(result, 'Get lun info by id %s error.', lun_id)
        return result['data']

    def get_lun_host_lun_id(self, host_id, lun_id):
        result = self.get(
            "/associate?ASSOCIATEOBJTYPE=21&ASSOCIATEOBJID=%(id)s", id=host_id)
        _assert_result(result, 'Get lun info related to host %s error.',
                       host_id)

        for item in result.get('data', []):
            if lun_id == item['ID']:
                metadata = json.loads(item['ASSOCIATEMETADATA'])
                return metadata['HostLUNID']


class StoragePool(CommonObject):
    _obj_url = '/storagepool'

    def get_all_pools(self):
        result = self.get()
        _assert_result(result, 'Query storage pools error.')
        return result.get('data', [])

    def get_pool_id(self, pool_name):
        result = self.get('?filter=NAME::%(name)s', name=pool_name)
        _assert_result(result, 'Query storage pool by name %s error.',
                       pool_name)
        if result.get('data'):
            return result['data'][0]['ID']

    def get_pool_by_name(self, pool_name):
        result = self.get('?filter=NAME::%(name)s', name=pool_name,
                          log_filter=True)
        _assert_result(result, 'Query storage pool by name %s error.',
                       pool_name)
        if result.get('data'):
            return result['data'][0]


class Snapshot(CommonObject):
    _obj_url = '/snapshot'

    def activate_snapshot(self, snapshot_ids):
        if isinstance(snapshot_ids, list):
            data = {"SNAPSHOTLIST": snapshot_ids}
        else:
            data = {"SNAPSHOTLIST": [snapshot_ids]}
        result = self.post('/activate', data=data)
        _assert_result(result, 'Activate snapshots %s error.', snapshot_ids)

    def create_snapshot(self, lun_id, snapshot_name, snapshot_description):
        data = {"NAME": snapshot_name,
                "DESCRIPTION": snapshot_description,
                "PARENTID": lun_id}
        result = self.post(data=data)
        _assert_result(result, 'Create snapshot %s for lun %s error.',
                       snapshot_name, lun_id)
        return result['data']

    def stop_snapshot(self, snapshot_id):
        data = {"ID": snapshot_id}
        result = self.put('/stop', data=data)
        _assert_result(result, 'Stop snapshot %s error.', snapshot_id)

    def delete_snapshot(self, snapshot_id):
        result = self.delete('/%(id)s', id=snapshot_id)
        if _error_code(result) == constants.SNAPSHOT_NOT_EXIST:
            LOG.warning('Snapshot %s to delete not exist.', snapshot_id)
            return
        _assert_result(result, 'Delete snapshot %s error.', snapshot_id)

    def get_snapshot_info_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Get snapshot info by name %s error.', name)
        if 'data' in result and result['data']:
            return result['data'][0]

    def get_snapshot_info_by_id(self, snapshot_id):
        result = self.get('/%(id)s', id=snapshot_id)
        _assert_result(result, 'Get snapshot info by id %s error.',
                       snapshot_id)
        return result['data']

    def update_snapshot(self, snapshot_id, data):
        result = self.put('/%(id)s', id=snapshot_id, data=data)
        _assert_result(result, 'Update snapshot %s error.', snapshot_id)

    def get_snapshot_count_of_lungroup(self, lungroup_id):
        result = self.get("/count?ASSOCIATEOBJTYPE=256&ASSOCIATEOBJID=%(id)s",
                          id=lungroup_id)
        _assert_result(result, 'Get snapshot count of lungroup %s error.',
                       lungroup_id)
        return int(result['data']['COUNT'])

    def get_snapshot_host_lun_id(self, host_id, snap_id):
        result = self.get(
            "/associate?ASSOCIATEOBJTYPE=21&ASSOCIATEOBJID=%(id)s", id=host_id)
        _assert_result(result, 'Get snapshot info related to host %s error.',
                       host_id)

        for item in result.get('data', []):
            if snap_id == item['ID']:
                metadata = json.loads(item['ASSOCIATEMETADATA'])
                return metadata['HostLUNID']


class LunCopy(CommonObject):
    _obj_url = '/LUNCOPY'

    def create_luncopy(self, luncopyname, srclunid, tgtlunid, copy_speed):
        param_format = "INVALID;%s;INVALID;INVALID;INVALID"
        data = {"NAME": luncopyname,
                "COPYSPEED": copy_speed,
                "SOURCELUN": param_format % srclunid,
                "TARGETLUN": param_format % tgtlunid}
        result = self.post(data=data)
        _assert_result(result, 'Create luncopy %s error.', luncopyname)

        return result['data']['ID']

    def start_luncopy(self, luncopy_id):
        data = {"ID": luncopy_id}
        result = self.put('/start', data=data)
        _assert_result(result, 'Start LUNCOPY %s error.', luncopy_id)

    def stop_luncopy(self, luncopy_id):
        data = {"ID": luncopy_id}
        result = self.put('/stop', data=data)
        if _error_code(result) in (constants.LUNCOPY_ALREADY_STOPPED,
                                   constants.LUNCOPY_COMPLETED):
            LOG.warning('Luncopy %s already stopped or completed.', luncopy_id)
            return
        _assert_result(result, 'Stop LUNCOPY %s error.', luncopy_id)

    def get_luncopy_info(self, luncopy_id):
        result = self.get('/%(id)s', id=luncopy_id)
        _assert_result(result, 'Get LUNCOPY %s error.', luncopy_id)
        return result.get('data', {})

    def delete_luncopy(self, luncopy_id):
        result = self.delete('/%(id)s', id=luncopy_id)
        if _error_code(result) == constants.LUNCOPY_NOT_EXIST:
            LOG.warning('Luncopy %s to delete not exist.', luncopy_id)
            return
        _assert_result(result, 'Delete LUNCOPY %s error.', luncopy_id)


class Host(CommonObject):
    _obj_url = '/host'

    def get_host_id_by_name(self, host_name):
        result = self.get('?filter=NAME::%(name)s', name=host_name)
        _assert_result(result, 'Get host by name %s error.', host_name)
        if result.get('data'):
            return result['data'][0]['ID']

    def create_host(self, hostname, orig_host_name):
        data = {"NAME": hostname,
                "OPERATIONSYSTEM": "0",
                "DESCRIPTION": orig_host_name}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_NAME_ALREADY_EXIST:
            return self.get_host_id_by_name(hostname)

        _assert_result(result, 'Add host %s error.', hostname)
        return result['data']['ID']

    def delete_host(self, host_id):
        result = self.delete('/%(id)s', id=host_id)
        if _error_code(result) == constants.HOST_NOT_EXIST:
            LOG.warning('Host %s to delete not exist.', host_id)
            return
        _assert_result(result, 'Delete host %s error.', host_id)

    def remove_host_from_hostgroup(self, hostgroup_id, host_id):
        result = self.delete('/associate?ID=%(gid)s&ASSOCIATEOBJTYPE=21&'
                             'ASSOCIATEOBJID=%(hid)s',
                             gid=hostgroup_id, hid=host_id)
        if _error_code(result) == constants.HOST_NOT_IN_HOSTGROUP:
            LOG.warning('Host %s not in hostgroup %s.', host_id, hostgroup_id)
            return
        _assert_result(result, 'Remove host %s from host group %s error.',
                       host_id, hostgroup_id)


class PortGroup(CommonObject):
    _obj_url = '/portgroup'

    def get_portgroup_in_mappingview(self, view_id):
        result = self.get('/associate?ASSOCIATEOBJTYPE=245&'
                          'ASSOCIATEOBJID=%(id)s', id=view_id)
        _assert_result(result, 'Get portgroup in mappingview %s error',
                       view_id)
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def create_portgroup(self, portg_name):
        data = {"NAME": portg_name}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_NAME_ALREADY_EXIST:
            LOG.info('Portgroup %s to create already exist.', portg_name)
            portgroup = self.get_portgroup_by_name(portg_name)
            if portgroup:
                return portgroup['ID']

        _assert_result(result, 'Create portgroup %s error.', portg_name)
        return result['data']['ID']

    def delete_portgroup(self, portgroup_id):
        result = self.delete('/%(id)s', id=portgroup_id)
        if _error_code(result) == constants.PORTGROUP_NOT_EXIST:
            LOG.warning('Portgroup %s to delete not exist.', portgroup_id)
            return
        _assert_result(result, 'Delete portgroup %s error.', portgroup_id)

    def get_portgroup_by_name(self, portg_name):
        result = self.get('?filter=NAME::%(name)s', name=portg_name)
        _assert_result(result, 'Get portgroup by name %s error.', portg_name)
        if 'data' in result and result['data']:
            return result['data'][0]

    def get_portgroup_by_port_id(self, port_id, port_type):
        result = self.get("/associate?ASSOCIATEOBJTYPE=%(type)s&"
                          "ASSOCIATEOBJID=%(id)s", id=port_id, type=port_type)
        _assert_result(result, 'Get portgroup by port %s error.', port_id)
        return [group['ID'] for group in result.get("data", [])]


class HostGroup(CommonObject):
    _obj_url = '/hostgroup'

    def get_hostgroup_in_mappingview(self, view_id):
        result = self.get('/associate?ASSOCIATEOBJTYPE=245&'
                          'ASSOCIATEOBJID=%(id)s', id=view_id)
        _assert_result(result, 'Get hostgroup in mappingview %s error.',
                       view_id)
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def associate_host_to_hostgroup(self, hostgroup_id, host_id):
        data = {"ID": hostgroup_id,
                "ASSOCIATEOBJTYPE": "21",
                "ASSOCIATEOBJID": host_id}
        result = self.post('/associate', data=data)
        if _error_code(result) == constants.HOST_ALREADY_IN_HOSTGROUP:
            LOG.info('Object %(id)s already in hostgroup %(group)s.',
                     {'id': host_id, 'group': hostgroup_id})
            return
        _assert_result(result, 'Associate host %s to hostgroup %s error.',
                       host_id, hostgroup_id)

    def create_hostgroup(self, name):
        data = {'NAME': name}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_NAME_ALREADY_EXIST:
            LOG.info('Hostgroup %s to create already exists.', name)
            hostgroup = self.get_hostgroup_by_name(name)
            return hostgroup['ID'] if hostgroup else None
        _assert_result(result, 'Create hostgroup %s error.', name)
        return result['data']['ID']

    def delete_hostgroup(self, hostgroup_id):
        result = self.delete('/%(id)s', id=hostgroup_id)
        if _error_code(result) == constants.HOSTGROUP_NOT_EXIST:
            LOG.info('Hostgroup %s to delete not exist.', hostgroup_id)
            return
        _assert_result(result, 'Delete hostgroup %s error.', hostgroup_id)

    def get_hostgroup_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Get hostgroup by %s error.', name)
        if 'data' in result and result['data']:
            return result['data'][0]


class LunGroup(CommonObject):
    _obj_url = '/lungroup'

    def associate_lun_to_lungroup(self, lungroup_id, obj_id, obj_type):
        data = {"ID": lungroup_id,
                "ASSOCIATEOBJTYPE": obj_type,
                "ASSOCIATEOBJID": obj_id}
        result = self.post('/associate', data=data)
        if _error_code(result) == constants.OBJECT_ID_NOT_UNIQUE:
            LOG.info('Object %(id)s already in lungroup %(group)s.',
                     {'id': obj_id, 'group': lungroup_id})
            return
        _assert_result(result, 'Associate obj %s to lungroup %s error.',
                       obj_id, lungroup_id)

    def remove_lun_from_lungroup(self, lungroup_id, obj_id, obj_type):
        result = self.delete(
            "/associate?ID=%(lungroup_id)s&ASSOCIATEOBJTYPE=%(obj_type)s&"
            "ASSOCIATEOBJID=%(obj_id)s", lungroup_id=lungroup_id,
            obj_id=obj_id, obj_type=obj_type)
        if _error_code(result) == constants.OBJECT_NOT_EXIST:
            LOG.warning('LUN %(lun)s not exist in lungroup %(gp)s.',
                        {'lun': obj_id, 'gp': lungroup_id})
            return
        _assert_result(result, 'Remove lun %s from lungroup %s error.',
                       obj_id, lungroup_id)

    def get_lungroup_in_mappingview(self, view_id):
        result = self.get('/associate?ASSOCIATEOBJTYPE=245&'
                          'ASSOCIATEOBJID=%(id)s', id=view_id)
        _assert_result(result, 'Get lungroup in mappingview %s error.',
                       view_id)
        if 'data' in result and result['data']:
            return result['data'][0]['ID']

    def get_lungroup_by_name(self, lungroup_name):
        """Get the given hostgroup id."""
        result = self.get('?filter=NAME::%(name)s', name=lungroup_name)
        _assert_result(result, 'Get lungroup info by name %s error.',
                       lungroup_name)
        if 'data' in result and result['data']:
            return result['data'][0]

    def create_lungroup(self, lungroup_name):
        data = {"APPTYPE": '0',
                "NAME": lungroup_name}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_NAME_ALREADY_EXIST:
            LOG.info('Lungroup %s to create already exists.', lungroup_name)
            lungroup = self.get_lungroup_by_name(lungroup_name)
            return lungroup['ID'] if lungroup else None

        _assert_result(result, 'Create lungroup %s error.', lungroup_name)
        return result['data']['ID']

    def delete_lungroup(self, lungroup_id):
        result = self.delete('/%(id)s', id=lungroup_id)
        if _error_code(result) == constants.OBJECT_NOT_EXIST:
            LOG.warning('Lungroup %s to delete not exist.', lungroup_id)
            return
        _assert_result(result, 'Delete lungroup %s error.', lungroup_id)

    def get_lungroup_ids_by_lun_id(self, lun_id, lun_type=constants.LUN_TYPE):
        result = self.get('/associate?TYPE=256&ASSOCIATEOBJTYPE=%(type)s&'
                          'ASSOCIATEOBJID=%(id)s', type=lun_type, id=lun_id)
        _assert_result(result, 'Get lungroup id by lun id %s error.', lun_id)

        lungroup_ids = []
        if 'data' in result:
            for item in result['data']:
                lungroup_ids.append(item['ID'])

        return lungroup_ids


class IscsiInitiator(CommonObject):
    _obj_url = '/iscsi_initiator'

    def add_iscsi_initiator(self, initiator):
        data = {'ID': initiator}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_ID_NOT_UNIQUE:
            LOG.info('iscsi initiator %s already exists.', initiator)
            return
        _assert_result(result, 'Add iscsi initiator %s error.', initiator)

    def associate_iscsi_initiator_to_host(self, initiator, host_id, alua_info):
        data = {
            "PARENTTYPE": "21",
            "PARENTID": host_id,
        }
        data.update(alua_info)

        result = self.put('/%(ini)s', data=data, ini=initiator)
        _assert_result(result, 'Add initiator %s to host %s error.',
                       initiator, host_id)

    def update_iscsi_initiator_chap(self, initiator, chap_info):
        if chap_info:
            data = {"USECHAP": "true",
                    "CHAPNAME": chap_info['CHAPNAME'],
                    "CHAPPASSWORD": chap_info['CHAPPASSWORD']}
        else:
            data = {"USECHAP": "false"}

        result = self.put('/%(ini)s', data=data, ini=initiator)
        _assert_result(result, 'Update initiator %s chap error.', initiator)

    def remove_iscsi_initiator_from_host(self, initiator):
        data = {"ID": initiator}
        result = self.put('/remove_iscsi_from_host', data=data)
        if _error_code(result) == constants.INITIATOR_NOT_IN_HOST:
            LOG.warning('ISCSI initiator %s not in host.', initiator)
            return
        _assert_result(result, 'Remove iscsi initiator %s from host error.',
                       initiator)

    def get_host_iscsi_initiators(self, host_id):
        result = self.get('?PARENTID=%(id)s', id=host_id)
        _assert_result(result, 'Get iscsi initiators of host %s error.',
                       host_id)
        initiators = []
        for item in result.get('data', []):
            initiators.append(item['ID'])
        return initiators

    def get_iscsi_initiator(self, initiator):
        result = self.get('/%(id)s', id=initiator)
        _assert_result(result, 'Get iscsi initiator %s error.', initiator)
        return result['data']


class MappingView(CommonObject):
    _obj_url = '/mappingview'

    def get_mappingview_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Find mapping view by name %s error', name)
        if 'data' in result and result['data']:
            return result['data'][0]

    def create_mappingview(self, name):
        data = {"NAME": name}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_NAME_ALREADY_EXIST:
            LOG.info('Mappingview %s to create already exists.', name)
            mappingview = self.get_mappingview_by_name(name)
            return mappingview['ID'] if mappingview else None
        _assert_result(result, 'Create mappingview by name %s error.', name)
        return result['data']['ID']

    def _associate_group_to_mappingview(self, view_id, group_id, group_type):
        data = {"ASSOCIATEOBJTYPE": group_type,
                "ASSOCIATEOBJID": group_id,
                "ID": view_id}
        result = self.put('/create_associate', data=data)
        if _error_code(result) in (constants.HOSTGROUP_ALREADY_IN_MAPPINGVIEW,
                                   constants.PORTGROUP_ALREADY_IN_MAPPINGVIEW,
                                   constants.LUNGROUP_ALREADY_IN_MAPPINGVIEW):
            LOG.warning('Group %(group_id)s of type %(type)s already exist '
                        'in mappingview %(view_id)s.',
                        {'group_id': group_id, 'type': group_type,
                         'view_id': view_id})
            return
        _assert_result(result, 'Associate group %s to mappingview %s error.',
                       group_id, view_id)

    def associate_hostgroup_to_mappingview(self, view_id, hostgroup_id):
        self._associate_group_to_mappingview(view_id, hostgroup_id, '14')

    def associate_lungroup_to_mappingview(self, view_id, lungroup_id):
        self._associate_group_to_mappingview(view_id, lungroup_id, '256')

    def associate_portgroup_to_mappingview(self, view_id, portgroup_id):
        self._associate_group_to_mappingview(view_id, portgroup_id, '257')

    def _remove_group_from_mappingview(self, view_id, group_id, group_type):
        data = {"ASSOCIATEOBJTYPE": group_type,
                "ASSOCIATEOBJID": group_id,
                "ID": view_id}
        result = self.put('/remove_associate', data=data)
        if _error_code(result) in (constants.HOSTGROUP_NOT_IN_MAPPINGVIEW,
                                   constants.PORTGROUP_NOT_IN_MAPPINGVIEW,
                                   constants.LUNGROUP_NOT_IN_MAPPINGVIEW):
            LOG.warning('Group %(group_id)s of type %(type)s not exist in '
                        'mappingview %(view_id)s.',
                        {'group_id': group_id, 'type': group_type,
                         'view_id': view_id})
            return
        _assert_result(result, 'Remove group %s from mappingview %s error.',
                       group_id, view_id)

    def remove_lungroup_from_mappingview(self, view_id, lungroup_id):
        self._remove_group_from_mappingview(view_id, lungroup_id, '256')

    def remove_hostgroup_from_mappingview(self, view_id, hostgroup_id):
        self._remove_group_from_mappingview(view_id, hostgroup_id, '14')

    def remove_portgroup_from_mappingview(self, view_id, portgroup_id):
        self._remove_group_from_mappingview(view_id, portgroup_id, '257')

    def delete_mapping_view(self, view_id):
        result = self.delete('/%(id)s', id=view_id)
        if _error_code(result) == constants.MAPPINGVIEW_NOT_EXIST:
            LOG.warning('Mappingview %s to delete not exist.', view_id)
            return
        _assert_result(result, 'Delete mappingview %s error.', view_id)

    def change_hostlun_id(self, view_id, lun_id, hostlun_id):
        data = {"ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id,
                "ASSOCIATEMETADATA": [
                    {"LUNID": lun_id,
                     "hostLUNId": six.text_type(hostlun_id)}]
                }
        result = self.put('/%(id)s', id=view_id, data=data)
        _assert_result(result, 'Change hostlun id for lun %s in mappingview '
                               '%s error.', lun_id, view_id)

    def get_mappingview_by_id(self, view_id):
        result = self.get('/%(id)s', id=view_id)
        _assert_result(result, 'Get mappingview info by id %s error.',
                       view_id)
        return result["data"]

    def get_mappingview_by_portgroup_id(self, portgroup_id):
        result = self.get('/associate?ASSOCIATEOBJTYPE=257&'
                          'ASSOCIATEOBJID=%(id)s', id=portgroup_id)
        _assert_result(result, 'Get mappingviews by portgroup %s error.',
                       portgroup_id)
        return [view['ID'] for view in result.get("data", [])]


class FCInitiator(CommonObject):
    _obj_url = '/fc_initiator'

    def get_fc_initiator_count(self):
        result = self.get("/count")
        _assert_result(result, 'Get FC initiator count error.')
        return int(result['data']['COUNT'])

    def _get_fc_initiator(self, start, end):
        result = self.get("?range=[%(start)s-%(end)s]", start=start, end=end)
        _assert_result(result, 'Get online free FC wwn error.')

        totals = []
        frees = []
        for item in result.get('data', []):
            totals.append(item['ID'])
            if item['RUNNINGSTATUS'] == '27' and item['ISFREE'] == 'true':
                frees.append(item['ID'])
        return totals, frees

    def get_fc_initiators(self):
        fc_initiator_count = self.get_fc_initiator_count()
        totals = []
        frees = []
        range_start = 0

        while fc_initiator_count > 0:
            range_end = range_start + constants.GET_PATACH_NUM
            _totals, _frees = self._get_fc_initiator(range_start, range_end)
            totals += _totals
            frees += _frees
            fc_initiator_count -= constants.GET_PATACH_NUM
            range_start += constants.GET_PATACH_NUM
        return totals, frees

    def add_fc_initiator(self, initiator):
        data = {'ID': initiator}
        result = self.post(data=data)
        if _error_code(result) == constants.OBJECT_ID_NOT_UNIQUE:
            LOG.info('FC initiator %s already exists.', initiator)
            return
        _assert_result(result, 'Add FC initiator %s error.', initiator)

    def associate_fc_initiator_to_host(self, host_id, wwn, alua_info):
        data = {
            "PARENTTYPE": 21,
            "PARENTID": host_id,
        }
        data.update(alua_info)

        result = self.put('/%(id)s', data=data, id=wwn)
        _assert_result(result, 'Add FC initiator %s to host %s error.',
                       wwn, host_id)

    def get_host_fc_initiators(self, host_id):
        result = self.get('?PARENTID=%(id)s', id=host_id)
        _assert_result(result, 'Get FC initiators of host %s error.',
                       host_id)
        return [item['ID'] for item in result.get('data', [])]

    def remove_fc_initiator_from_host(self, initiator):
        data = {"ID": initiator}
        result = self.put('/remove_fc_from_host', data=data)
        if _error_code(result) == constants.INITIATOR_NOT_IN_HOST:
            LOG.warning('FC initiator %s not in host.', initiator)
            return
        _assert_result(result, 'Remove fc initiator %s from host error.',
                       initiator)


class HostLink(CommonObject):
    _obj_url = '/host_link'

    def get_fc_target_wwpns(self, ini):
        result = self.get('?INITIATOR_TYPE=223&INITIATOR_PORT_WWN=%(wwn)s',
                          wwn=ini)
        _assert_result(result, 'Get FC target wwn for initiator %s error.',
                       ini)
        return [fc['TARGET_PORT_WWN'] for fc in result.get('data', [])]

    def get_host_link(self, host_id):
        result = self.get('?INITIATOR_TYPE=223&PARENTID=%(id)s', id=host_id)
        _assert_result(result, 'Get host link for host %s error.', host_id)
        return result.get('data', [])


class IOClass(CommonObject):
    _obj_url = '/ioclass'

    def create_qos(self, qos, lun_id):
        localtime = time.strftime('%Y%m%d%H%M%S', time.localtime())
        qos_name = constants.QOS_NAME_PREFIX + lun_id + '_' + localtime

        data = {"NAME": qos_name,
                "LUNLIST": [lun_id],
                "CLASSTYPE": "1",
                "SCHEDULEPOLICY": "2",
                "SCHEDULESTARTTIME": "1410969600",
                "STARTTIME": "08:00",
                "DURATION": "86400",
                "CYCLESET": "[1,2,3,4,5,6,0]",
                }
        data.update(qos)

        result = self.post(data=data)
        _assert_result(result, 'Create QoS policy %s error.', qos)
        return result['data']['ID']

    def delete_qos(self, qos_id):
        result = self.delete('/%(id)s', id=qos_id)
        _assert_result(result, 'Delete QoS policy %s error.', qos_id)

    def activate_deactivate_qos(self, qos_id, enablestatus):
        """Activate or deactivate QoS.

        enablestatus: true (activate)
        enbalestatus: false (deactivate)
        """
        data = {"ID": qos_id,
                "ENABLESTATUS": enablestatus}
        result = self.put('/active', data=data)
        _assert_result(result, 'Change QoS %s to status %s error.',
                       qos_id, enablestatus)

    def get_qos_info(self, qos_id):
        result = self.get('/%(id)s', id=qos_id)
        _assert_result(result, 'Get QoS %s info error.', qos_id)
        return result['data']

    def get_all_qos(self):
        result = self.get()
        _assert_result(result, 'Get all QoS information error.')
        return result.get('data', [])

    def update_qos_luns(self, qos_id, lun_list):
        """Add lun to QoS."""
        data = {"LUNLIST": lun_list}
        result = self.put('/%(qos_id)s', data=data, qos_id=qos_id)
        _assert_result(result, 'Update lun list %s to QoS %s error.',
                       lun_list, qos_id)


class EthPort(CommonObject):
    _obj_url = '/eth_port'

    def get_eth_ports_in_portgroup(self, portgroup_id):
        result = self.get("/associate?ASSOCIATEOBJTYPE=257&"
                          "ASSOCIATEOBJID=%(id)s", id=portgroup_id)
        _assert_result(result, 'Get eth ports in portgroup %s error.',
                       portgroup_id)
        return result.get("data", [])


class IscsiTgtPort(CommonObject):
    _obj_url = '/iscsi_tgt_port'

    def get_iscsi_tgt_ports(self):
        result = self.get()
        _assert_result(result, "Get iscsi target ports info error.")
        return result.get('data', [])


class LunMigration(CommonObject):
    _obj_url = '/lun_migration'

    def create_lun_migration(self, src_id, dst_id):
        data = {"PARENTID": src_id,
                "TARGETLUNID": dst_id,
                "SPEED": '2',
                "WORKMODE": 0}

        result = self.post(data=data)
        _assert_result(result, 'Create migration from %s to %s error.',
                       src_id, dst_id)
        return result['data']

    def get_lun_migration(self, migration_id):
        result = self.get('/%(id)s', id=migration_id)
        _assert_result(result, 'Get migration info %s error.', migration_id)
        return result['data']

    def delete_lun_migration(self, migration_id):
        result = self.delete('/%(id)s', id=migration_id)
        if _error_code(result) == constants.MIGRATION_NOT_EXIST:
            LOG.warning('Migration %s to delete not exist.', migration_id)
            return
        _assert_result(result, 'Delete migration %s error.', migration_id)


class CachePartition(CommonObject):
    _obj_url = '/cachepartition'

    def get_partition_id_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Get partition by name %s error.', name)
        if 'data' in result and len(result['data']) > 0:
            return result['data'][0]['ID']

    def get_partition_info_by_id(self, partition_id):
        result = self.get('/%(id)s', id=partition_id)
        _assert_result(result, 'Get partition info by id %s error.',
                       partition_id)
        return result['data']


class SmartCachePartition(CommonObject):
    _obj_url = '/smartcachepartition'

    def get_cache_id_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Get smartcachepartition by name %s error.',
                       name)
        if 'data' in result and len(result['data']) > 0:
            return result['data'][0]['ID']

    def get_cache_info_by_id(self, cacheid):
        result = self.get('/%(id)s', id=cacheid)
        _assert_result(result, 'Get smartcachepartition by id %s error.',
                       cacheid)
        return result['data']

    def remove_lun_from_cache(self, lun_id, cache_id):
        data = {"ID": cache_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id}

        result = self.put('/remove_associate', data=data)
        _assert_result(result, 'Remove lun %s from smartcachepartition '
                               '%s error.', lun_id, cache_id)

    def add_lun_to_cache(self, lun_id, cache_id):
        data = {"ID": cache_id,
                "ASSOCIATEOBJTYPE": 11,
                "ASSOCIATEOBJID": lun_id}
        result = self.put('/create_associate', data=data)
        _assert_result(result, 'Add lun %s to smartcachepartition '
                               '%s error.', lun_id, cache_id)


class FCPort(CommonObject):
    _obj_url = '/fc_port'

    def get_fc_ports(self):
        result = self.get()
        _assert_result(result, 'Get FC ports from array error.')
        return result.get('data', [])

    def get_fc_ports_in_portgroup(self, portgroup_id):
        result = self.get('/associate?ASSOCIATEOBJTYPE=257'
                          '&ASSOCIATEOBJID=%(id)s', id=portgroup_id)
        _assert_result(result, 'Get FC ports in portgroup %s error.',
                       portgroup_id)
        return result.get("data", [])


class HyperMetroDomain(CommonObject):
    _obj_url = '/HyperMetroDomain'

    def get_hypermetro_domain_id(self, domain_name):
        result = self.get('?range=[0-32]')
        _assert_result(result, 'Get hyper metro domains info error.')
        for item in result.get('data', []):
            if domain_name == item['NAME']:
                return item['ID']


class HyperMetroPair(CommonObject):
    _obj_url = '/HyperMetroPair'

    def create_hypermetro(self, hcp_param):
        result = self.post(data=hcp_param)
        _assert_result(result, 'Create hypermetro pair %s error.', hcp_param)
        return result['data']

    def delete_hypermetro(self, metro_id):
        result = self.delete('/%(id)s', id=metro_id)
        if _error_code(result) == constants.HYPERMETRO_NOT_EXIST:
            LOG.warning('Hypermetro %s to delete not exist.', metro_id)
            return
        _assert_result(result, 'Delete hypermetro %s error.', metro_id)

    def sync_hypermetro(self, metro_id):
        data = {"ID": metro_id}
        result = self.put('/synchronize_hcpair', data=data)
        _assert_result(result, 'Sync hypermetro %s error.', metro_id)

    def stop_hypermetro(self, hypermetro_id):
        data = {"ID": hypermetro_id}
        result = self.put('/disable_hcpair', data=data)
        _assert_result(result, 'Stop hypermetro %s error.', hypermetro_id)

    def get_hypermetro_by_id(self, metro_id):
        result = self.get('?filter=ID::%(id)s', id=metro_id)
        _assert_result(result, 'Get hypermetro by id %s error.', metro_id)
        if result.get('data'):
            return result['data'][0]

    def get_hypermetro_by_lun_name(self, lun_name):
        result = self.get('?filter=LOCALOBJNAME::%(name)s', name=lun_name)
        _assert_result(result, 'Get hypermetro by local lun name'
                               ' %s error.', lun_name)
        if result.get('data'):
            return result['data'][0]


class HyperMetroConsistentGroup(CommonObject):
    _obj_url = '/HyperMetro_ConsistentGroup'

    def get_metrogroup_by_name(self, name):
        result = self.get('?filter=NAME::%(name)s', name=name)
        _assert_result(result, 'Get hypermetro group by name %s error.', name)
        if 'data' in result and len(result['data']) > 0:
            return result['data'][0]

    def create_metrogroup(self, group_params):
        result = self.post(data=group_params)
        _assert_result(result, 'Create hypermetro group %s error.',
                       group_params)

    def delete_metrogroup(self, metrogroup_id):
        result = self.delete('/%(id)s', id=metrogroup_id)
        if _error_code(result) == constants.HYPERMETROGROUP_NOT_EXIST:
            LOG.warning('Hypermetro group %s to delete not exist.',
                        metrogroup_id)
            return
        _assert_result(result, 'Delete hypermetro group %s error.',
                       metrogroup_id)

    def stop_metrogroup(self, metrogroup_id):
        data = {"ID": metrogroup_id}
        result = self.put('/stop', data=data)
        _assert_result(result, 'Stop hypermetro group %s error.',
                       metrogroup_id)

    def sync_metrogroup(self, metrogroup_id):
        data = {"ID": metrogroup_id}
        result = self.put('/sync', data=data)
        if _error_code(result) == constants.NO_HYPERMETRO_EXIST_IN_GROUP:
            LOG.info('Hypermetro group %s to sync is empty.', metrogroup_id)
            return
        _assert_result(result, 'Sync hypermetro group %s error.',
                       metrogroup_id)


class HyperMetro(CommonObject):
    _obj_url = '/hyperMetro'

    def add_metro_to_metrogroup(self, metrogroup_id, metro_id):
        data = {"ID": metrogroup_id,
                "ASSOCIATEOBJID": metro_id}
        result = self.post('/associate/pair', data=data)
        if _error_code(result) == constants.HYPERMETRO_ALREADY_IN_GROUP:
            LOG.warning('Hypermetro %(m_id) to add already in group %(g_id)s',
                        m_id=metro_id, g_id=metrogroup_id)
            return
        _assert_result(result, 'Add hypermetro %s to group %s error.',
                       metro_id, metrogroup_id)

    def remove_metro_from_metrogroup(self, metrogroup_id, metro_id):
        data = {"ID": metrogroup_id,
                "ASSOCIATEOBJID": metro_id}
        result = self.delete('/associate/pair', data=data)
        if _error_code(result) == constants.HYPERMETRO_NOT_IN_GROUP:
            LOG.warning('Hypermetro %(mid) to remove not in group %(gid)s',
                        {'mid': metro_id, 'gid': metrogroup_id})
            return
        _assert_result(result, 'Delete hypermetro %s from group %s error.',
                       metro_id, metrogroup_id)


class Port(CommonObject):
    _obj_url = '/port'

    def add_port_to_portgroup(self, portgroup_id, port_id):
        data = {"ASSOCIATEOBJID": port_id,
                "ASSOCIATEOBJTYPE": 212,
                "ID": portgroup_id}
        result = self.post('/associate/portgroup', data=data)
        if _error_code(result) == constants.PORT_ALREADY_IN_PORTGROUP:
            LOG.warning('Port %(pid)s already in portgroup %(gid)s.',
                        {'pid': port_id, 'gid': portgroup_id})
            return
        _assert_result(result, 'Add port %s to portgroup %s error.',
                       port_id, portgroup_id)

    def remove_port_from_portgroup(self, portgroup_id, port_id):
        result = self.delete('/associate/portgroup?ID=%(gid)s&'
                             'ASSOCIATEOBJTYPE=212&ASSOCIATEOBJID=%(pid)s',
                             gid=portgroup_id, pid=port_id)
        if _error_code(result) == constants.PORT_NOT_IN_PORTGROUP:
            LOG.warning('Port %(pid)s not in portgroup %(gid)s.',
                        {'pid': port_id, 'gid': portgroup_id})
            return
        _assert_result(result, 'Remove port %s from portgroup %s error.',
                       port_id, portgroup_id)


class RemoteDevice(CommonObject):
    _obj_url = '/remote_device'

    def get_remote_device_by_wwn(self, wwn):
        result = self.get()
        _assert_result(result, 'Get remote devices error.')
        for device in result.get('data', []):
            if device.get('WWN') == wwn:
                return device


class ReplicationPair(CommonObject):
    _obj_url = '/REPLICATIONPAIR'

    def create_replication_pair(self, pair_params):
        result = self.post(data=pair_params)
        _assert_result(result, 'Create replication %s error.', pair_params)
        return result['data']

    def get_replication_pair_by_id(self, pair_id):
        result = self.get('/%(id)s', id=pair_id)
        if _error_code(result) == constants.REPLICATION_PAIR_NOT_EXIST:
            _assert_result(result, 'Replication pair %s not exist.', pair_id)
        else:
            _assert_result(result, 'Get replication pair %s error.', pair_id)
        return result['data']

    def switch_replication_pair(self, pair_id):
        data = {"ID": pair_id}
        result = self.put('/switch', data=data)
        _assert_result(result, 'Switch over replication pair %s error.',
                       pair_id)

    def split_replication_pair(self, pair_id):
        data = {"ID": pair_id}
        result = self.put('/split', data=data)
        _assert_result(result, 'Split replication pair %s error.', pair_id)

    def delete_replication_pair(self, pair_id, force=False):
        if force:
            data = {"ISLOCALDELETE": force}
            result = self.delete('/%(id)s', id=pair_id, data=data)
        else:
            result = self.delete('/%(id)s', id=pair_id)

        if _error_code(result) == constants.REPLICATION_PAIR_NOT_EXIST:
            LOG.warning('Replication pair to delete %s not exist.',
                        pair_id)
            return
        _assert_result(result, 'Delete replication pair %s error.', pair_id)

    def sync_replication_pair(self, pair_id):
        data = {"ID": pair_id}
        result = self.put('/sync', data=data)
        _assert_result(result, 'Sync replication pair %s error.', pair_id)

    def set_replication_pair_second_access(self, pair_id, access):
        data = {"SECRESACCESS": access}
        result = self.put('/%(id)s', id=pair_id, data=data)
        _assert_result(result, 'Set replication pair %s secondary access '
                               'to %s error.', pair_id, access)


class ReplicationConsistencyGroup(CommonObject):
    _obj_url = '/CONSISTENTGROUP'

    def create_replication_group(self, group_params):
        result = self.post(data=group_params)
        _assert_result(result, 'Create replication group %s error.',
                       group_params)
        return result['data']

    def get_replication_group_by_name(self, group_name):
        result = self.get('?filter=NAME::%(name)s', name=group_name)
        _assert_result(result, 'Get replication group by name %s error.',
                       group_name)
        if 'data' in result and len(result['data']) > 0:
            return result['data'][0]

    def get_replication_group_by_id(self, group_id):
        result = self.get('/%(id)s', id=group_id)
        _assert_result(result, 'Get replication group by id %s error.',
                       group_id)
        return result['data']

    def delete_replication_group(self, group_id):
        result = self.delete('/%(id)s', id=group_id)
        if _error_code(result) == constants.REPLICATION_GROUP_NOT_EXIST:
            LOG.warning('Replication group %s to delete not exist.', group_id)
            return
        _assert_result(result, 'Delete replication group %s error.', group_id)

    def set_replication_group_second_access(self, group_id, access):
        data = {"SECRESACCESS": access}
        result = self.put("/%(id)s", id=group_id, data=data)
        _assert_result(result, 'Set replication group %s second access to '
                               '%s error.', group_id, access)


class LicenseFeature(CommonObject):
    _obj_url = '/license/feature'

    def get_feature_status(self):
        result = self.get(log_filter=True)
        if result['error']['code'] != 0:
            LOG.warning('Query feature information failed.')
            return {}

        status = {}
        for feature in result.get('data', []):
            status.update(feature)

        return status


class ClonePair(CommonObject):
    _obj_url = '/clonepair'

    def create_clone_pair(self, source_id, target_id, clone_speed):
        data = {"copyRate": clone_speed,
                "sourceID": source_id,
                "targetID": target_id,
                "isNeedSynchronize": "0"}
        result = self.post("/relation", data=data)
        _assert_result(result, 'Create ClonePair error, source_id is %s.',
                       source_id)
        return result['data']['ID']

    def sync_clone_pair(self, pair_id):
        data = {"ID": pair_id, "copyAction": 0}
        result = self.put("/synchronize", data=data)
        _assert_result(result, 'Sync ClonePair error, pair is is %s.', pair_id)

    def get_clone_pair_info(self, pair_id):
        result = self.get('/%(id)s', id=pair_id)
        _assert_result(result, 'Get ClonePair %s error.', pair_id)
        return result.get('data', {})

    def delete_clone_pair(self, pair_id, delete_dst_lun=False):
        data = {"ID": pair_id,
                "isDeleteDstLun": delete_dst_lun}
        result = self.delete("/%(id)s", id=pair_id, data=data)
        if _error_code(result) == constants.CLONE_PAIR_NOT_EXIST:
            LOG.warning('ClonePair %s to delete not exist.', pair_id)
            return
        _assert_result(result, 'Delete ClonePair %s error.', pair_id)


class HostNameIgnoringAdapter(HTTPAdapter):
    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = False
        return super(HostNameIgnoringAdapter, self).cert_verify(
            conn, url, verify, cert)


def rest_operation_wrapper(func):
    @functools.wraps(func)
    def wrapped(self, url, **kwargs):
        need_relogin = False

        if not kwargs.get('log_filter'):
            LOG.info('\nURL: %(url)s\n'
                     'Method: %(method)s\n'
                     'Data: %(data)s\n',
                     {'url': (self._login_url or '') + url,
                      'method': func.__name__,
                      'data': kwargs.get('data')})

        with self._session_lock.read_lock():
            if self._login_url:
                full_url = self._login_url + url
                old_token = self._session.headers.get('iBaseToken')
                try:
                    r = func(self, full_url, **kwargs)
                except requests.RequestException:
                    LOG.exception('Request URL: %(url)s, method: %(method)s '
                                  'failed at first time. Will switch login '
                                  'url and retry this request.',
                                  {'url': full_url,
                                   'method': func.__name__})
                    need_relogin = True
                else:
                    r.raise_for_status()
                    result = r.json()
                    if (_error_code(result) in
                            (constants.ERROR_CONNECT_TO_SERVER,
                             constants.ERROR_UNAUTHORIZED_TO_SERVER)):
                        need_relogin = True
            else:
                need_relogin = True
                old_token = None

        if need_relogin:
            self._relogin(old_token)
            try:
                with self._session_lock.read_lock():
                    full_url = self._login_url + url
                    r = func(self, full_url, **kwargs)
            except requests.RequestException:
                LOG.exception('Request URL: %(url)s, method: %(method)s '
                              'failed again.',
                              {'url': full_url,
                               'method': func.__name__})
                raise

        r.raise_for_status()
        result = r.json()
        if not kwargs.get('log_filter'):
            LOG.info('Response: %s', result)
        return result

    return wrapped


class RestClient(object):
    def __init__(self, address, user, password, vstore=None, ssl_verify=None,
                 cert_path=None):
        self.san_address = address
        self.san_user = user
        self.san_password = password
        self.vstore_name = vstore
        self.ssl_verify = ssl_verify
        self.cert_path = cert_path

        self._login_url = None
        self._login_device_id = None
        self._session_lock = lockutils.ReaderWriterLock()
        self._session = None
        self._init_object_methods()

    def _extract_obj_method(self, obj):
        filter_method_names = ('login', 'get', 'post', 'delete', 'put')

        def prefilter(m):
            return (inspect.ismethod(m) and not inspect.isbuiltin(m) and
                    m.__name__ not in filter_method_names and
                    not m.__name__.startswith('_'))

        members = inspect.getmembers(obj, prefilter)
        for method in members:
            if method[0] in self.__dict__:
                msg = _('Method %s already exists in rest client.'
                        ) % method[0]
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            self.__dict__[method[0]] = method[1]

    def _init_object_methods(self):
        def prefilter(m):
            return inspect.isclass(m) and issubclass(m, CommonObject)

        obj_classes = inspect.getmembers(sys.modules[__name__], prefilter)
        for cls in obj_classes:
            self._extract_obj_method(cls[1](self))

    def _try_login(self, manage_url):
        url = manage_url + "xx/sessions"
        data = {"username": self.san_user,
                "password": self.san_password,
                "scope": "0"}
        if self.vstore_name:
            data['vstorename'] = self.vstore_name

        r = self._session.post(url, data=json.dumps(data),
                               timeout=constants.LOGIN_SOCKET_TIMEOUT)
        r.raise_for_status()

        result = r.json()
        if _error_code(result) != 0:
            msg = _("Failed to login URL %(url)s because of %(reason)s."
                    ) % {"url": url, "reason": result}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._session.headers['iBaseToken'] = result['data']['iBaseToken']
        self._login_device_id = result['data']['deviceid']
        self._login_url = manage_url + self._login_device_id

        if result['data']['accountstate'] in constants.PWD_EXPIRED_OR_INITIAL:
            self._session.delete(self._login_url + "/sessions")
            self._login_device_id = None
            self._login_url = None
            msg = ("Storage password has been expired or initial, "
                   "please change the password.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _loop_login(self):
        self._session = requests.Session()
        self._session.headers.update({
            "Connection": "keep-alive",
            "Content-Type": "application/json; charset=utf-8"})
        self._session.verify = False
        if self.ssl_verify:
            self._session.verify = self.cert_path

        for url in self.san_address:
            try:
                self._session.mount(url.lower(), HostNameIgnoringAdapter())
                self._try_login(url)
            except Exception:
                LOG.exception('Failed to login server %s.', url)
            else:
                # Sort the login url to the last slot of san addresses, so that
                # if this connection error, next time will try other url first.
                self.san_address.remove(url)
                self.san_address.append(url)
                LOG.info('Login %s success.', url)
                return

        self._session.close()
        self._session = None

        msg = _("Failed to login storage with all rest URLs.")
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def login(self):
        with self._session_lock.write_lock():
            self._loop_login()

    def _relogin(self, old_token):
        with self._session_lock.write_lock():
            if (self._session and
                    self._session.headers.get('iBaseToken') != old_token):
                LOG.info('Relogin has been done by other thread, '
                         'no need relogin again.')
                return

            # Try to logout the original session first
            self._logout()
            self._loop_login()

    def _logout(self):
        if not self._login_url:
            return

        try:
            r = self._session.delete(self._login_url + "/sessions")
            r.raise_for_status()
        except Exception:
            LOG.exception("Failed to logout session from URL %s.",
                          self._login_url)
        else:
            result = r.json()
            if _error_code(result) == 0:
                LOG.info("Succeed to logout session from URL %(url)s.",
                         {"url": self._login_url})
            else:
                LOG.warning("Failed to logout session from URL %(url)s "
                            "because of %(reason)s.",
                            {"url": self._login_url, "reason": result})
        finally:
            self._session.close()
            self._session = None
            self._login_url = None
            self._login_device_id = None

    @property
    def device_id(self):
        return self._login_device_id

    @rest_operation_wrapper
    def get(self, url, timeout=constants.SOCKET_TIMEOUT, **kwargs):
        return self._session.get(url, timeout=timeout)

    @rest_operation_wrapper
    def post(self, url, data, timeout=constants.SOCKET_TIMEOUT, **kwargs):
        return self._session.post(url, data=json.dumps(data), timeout=timeout)

    @rest_operation_wrapper
    def put(self, url, data, timeout=constants.SOCKET_TIMEOUT, **kwargs):
        return self._session.put(url, data=json.dumps(data), timeout=timeout)

    @rest_operation_wrapper
    def delete(self, url, timeout=constants.SOCKET_TIMEOUT, **kwargs):
        if 'data' in kwargs:
            return self._session.delete(
                url, data=json.dumps(kwargs['data']), timeout=timeout)
        else:
            return self._session.delete(url, timeout=timeout)

    def add_pair_to_replication_group(self, group_id, pair_id):
        data = {'ID': group_id,
                'RMLIST': [pair_id]}
        result = self.put('/ADD_MIRROR', data=data)
        _assert_result(result, 'Add pair %s to replication group %s error.',
                       pair_id, group_id)

    def remove_pair_from_replication_group(self, group_id, pair_id):
        data = {'ID': group_id,
                'RMLIST': [pair_id]}
        result = self.put('/DEL_MIRROR', data=data)
        if _error_code(result) in (constants.REPLICATION_PAIR_NOT_EXIST,
                                   constants.REPLICATION_GROUP_NOT_EXIST,
                                   constants.REPLICATION_PAIR_NOT_GROUP_MEMBER,
                                   constants.REPLICATION_GROUP_IS_EMPTY):
            LOG.warning('Ignore error %s while remove replication pair '
                        'from group.', _error_code(result))
            return
        _assert_result(result, 'Remove pair %s from replication group %s '
                               'error.', pair_id, group_id)

    def split_replication_group(self, group_id):
        data = {'ID': group_id}
        result = self.put('/SPLIT_CONSISTENCY_GROUP', data=data)
        _assert_result(result, 'Split replication group %s error.', group_id)

    def sync_replication_group(self, group_id):
        data = {'ID': group_id}
        result = self.put('/SYNCHRONIZE_CONSISTENCY_GROUP', data=data)
        if _error_code(result) == constants.REPLICATION_GROUP_IS_EMPTY:
            LOG.info("Replication group %s to sync is empty.", group_id)
            return
        _assert_result(result, 'Sync replication group %s error.', group_id)

    def switch_replication_group(self, group_id):
        data = {'ID': group_id}
        result = self.put('/SWITCH_GROUP_ROLE', data=data)
        _assert_result(result, 'Switch replication group %s error.', group_id)

    def get_array_info(self):
        result = self.get('/system/')
        _assert_result(result, 'Get array info error.')
        return result['data']

    def check_feature(self, obj):
        try:
            result = self.get('/%s/count' % obj, log_filter=True)
        except requests.HTTPError as exc:
            if exc.response.status_code == 404:
                return False
            raise

        return _error_code(result) == 0

    def get_controller_id(self, controller_name):
        result = self.get('/controller')
        _assert_result(result, 'Get controllers error.')

        for con in result.get('data', []):
            if con.get('LOCATION') == controller_name:
                return con['ID']

    def split_lunclone(self, clone_id):
        data = {
            "ID": clone_id,
            "SPLITACTION": 1,
            "ISCLONE": True,
            "SPLITSPEED": 4,
        }
        result = self.put('/lunclone_split_switch', data=data)
        _assert_result(result, 'split clone lun %s error.', clone_id)

    def get_workload_type_id(self, workload_type_name):
        url = "/workload_type?filter=NAME::%s" % workload_type_name
        result = self.get(url)
        _assert_result(result, 'Get workload type error')

        for item in result.get("data", []):
            if item.get("NAME") == workload_type_name:
                return item.get("ID")

    def get_workload_type_name(self, workload_type_id):
        url = "/workload_type/%s" % workload_type_id
        result = self.get(url)
        _assert_result(result, 'Get workload type by id error')
        return result.get("data", {}).get("NAME")
