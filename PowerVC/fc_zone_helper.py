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

import six

from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei import constants


LOG = logging.getLogger(__name__)


class FCZoneHelper(object):
    """FC zone helper for Huawei driver."""

    def __init__(self, fcsan_lookup_service, client):
        self.fc_san = fcsan_lookup_service
        self.client = client

    def _get_online_fc_ports(self):
        port_map = {}

        fc_ports = self.client.get_fc_ports()
        for port in fc_ports:
            if port['RUNNINGSTATUS'] == constants.FC_PORT_CONNECTED:
                port_wwn = port['WWN']
                port_map[port_wwn] = {
                    'id': port['ID'],
                    'runspeed': int(port['RUNSPEED']),
                }

        return port_map

    def _get_fabric(self, ini_port_wwns, tgt_port_wwns):
        ini_tgt_map = self.fc_san.get_device_mapping_from_network(
            ini_port_wwns, tgt_port_wwns)

        def _filter_not_connected_fabric(fabric_name, fabric):
            ini_port_wwn_list = fabric.get('initiator_port_wwn_list')
            tgt_port_wwn_list = fabric.get('target_port_wwn_list')

            if not ini_port_wwn_list or not tgt_port_wwn_list:
                LOG.warning("Fabric %(fabric_name)s doesn't really "
                            "connect host and array: %(fabric)s.",
                            {'fabric_name': fabric_name,
                             'fabric': fabric})
                return None

            return [ini_port_wwn_list, tgt_port_wwn_list]

        valid_fabrics = []
        for fabric in ini_tgt_map:
            pair = _filter_not_connected_fabric(fabric, ini_tgt_map[fabric])
            if pair:
                valid_fabrics.append(pair)

        if not valid_fabrics:
            msg = _("No valid fabric connection: %s.") % ini_tgt_map
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.info("Got fabric: %s.", valid_fabrics)
        return valid_fabrics

    def _get_used_ports(self, host_id):
        portgroup_id = self.client.get_tgt_port_group(
            constants.PORTGROUP_PREFIX + host_id)
        if not portgroup_id:
            return []

        ports = self.client.get_ports_by_portg(portgroup_id)
        return ports

    def _get_fc_zone(self, wwns, host_id):
        port_map = self._get_online_fc_ports()

        fabrics = self._get_fabric(wwns, list(port_map.keys()))
        used_ports = self._get_used_ports(host_id)

        total_ports = []
        ini_tgt_map = {}
        for fabric in fabrics:
            total_ports = list(set(total_ports) | set(fabric[1]))
            new_ports = list(set(fabric[1]) - set(used_ports))
            if not new_ports:
                continue
            for ini in fabric[0]:
                if ini not in ini_tgt_map:
                    ini_tgt_map[ini] = new_ports
                else:
                    ini_tgt_map[ini].extend(new_ports)

        return ini_tgt_map, total_ports, port_map

    def build_ini_targ_map(self, wwns, host_id):
        ini_tgt_map, total_ports, port_map = self._get_fc_zone(wwns, host_id)

        new_ports = set()
        for v in six.itervalues(ini_tgt_map):
            new_ports |= set(v)

        portgroup_name = constants.PORTGROUP_PREFIX + host_id
        portgroup_id = self.client.get_tgt_port_group(portgroup_name)
        if portgroup_id:
            LOG.info("Got existing portgroup: %s.", portgroup_name)
            for port in new_ports:
                self.client.add_port_to_portg(portgroup_id, port_map[port]['id'])

        LOG.info("ini_targ_map: %(map)s, target_wwns: %(wwns)s.",
                 {"map": ini_tgt_map,
                  "wwns": total_ports})
        return list(total_ports), portgroup_id, ini_tgt_map

    def get_init_targ_map(self, wwns, host_id):
        error_ret = ([], {})
        if not host_id:
            return error_ret

        view_name = constants.MAPPING_VIEW_PREFIX + host_id
        view_id = self.client.find_mapping_view(view_name)
        if not view_id:
            return error_ret

        portg_id = self.client.get_portgroup_by_view(view_id)
        if portg_id:
            ports_in_group = self.client.get_fc_ports_by_portgroup(portg_id)
            for port_id in ports_in_group.values():
                self.client.remove_port_from_portgroup(portg_id, port_id)
            ports = list(ports_in_group.keys())
        else:
            fc_ports = self.client.get_fc_ports()
            ports = [p['WWN'] for p in fc_ports]

        return portg_id, dict.fromkeys(wwns, ports)
