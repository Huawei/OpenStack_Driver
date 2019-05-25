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
        engine_map = {}
        contr_map = {}
        slot_map = {}
        port_map = {}

        fc_ports = self.client.get_fc_ports()
        for port in fc_ports:
            if port['RUNNINGSTATUS'] == constants.FC_PORT_CONNECTED:
                location = port['LOCATION'].split('.')
                engine = location[0]
                contr = port['PARENTID'].split('.')[0]
                slot = port['PARENTID']
                port_wwn = port['WWN']

                if engine not in engine_map:
                    engine_map[engine] = [contr]
                elif contr not in engine_map[engine]:
                    engine_map[engine].append(contr)

                if contr not in contr_map:
                    contr_map[contr] = [slot]
                elif slot not in contr_map[contr]:
                    contr_map[contr].append(slot)

                if slot not in slot_map:
                    slot_map[slot] = [port_wwn]
                elif port_wwn not in slot_map[slot]:
                    slot_map[slot].append(port_wwn)

                port_map[port_wwn] = {
                    'id': port['ID'],
                    'runspeed': int(port['RUNSPEED']),
                }

        return engine_map, contr_map, slot_map, port_map

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

    def _filter_fabric(self, fabrics, host_id):
        host_initiators = self.client.get_host_fc_initiators(host_id)
        for fabric in fabrics:
            used_ini = set(fabric[0]) & set(host_initiators)
            if len(used_ini) >= 2:
                fabric[0] = list(used_ini)
            elif len(used_ini) == 1:
                others = list(set(fabric[0]) - used_ini)
                fabric[0] = list(used_ini) + others[:1]
            else:
                fabric[0] = fabric[0][:2]

    def _get_used_ports(self, host_id):
        portgroup_id = self.client.get_tgt_port_group(
            constants.PORTGROUP_PREFIX + host_id)
        if not portgroup_id:
            return []

        ports = self.client.get_ports_by_portg(portgroup_id)
        return ports

    def _count_port_weight(self, port, port_map):
        port_bandwidth = port_map[port]['runspeed']
        portgroup_ids = self.client.get_portgs_by_portid(
            port_map[port]['id'])
        return len(portgroup_ids), port_bandwidth

    def _select_optimal_ports(self, ports, port_map, count):
        if not ports or count <= 0:
            return []

        port_pairs = []
        for port in ports:
            weight = self._count_port_weight(port, port_map)
            port_pairs.append((weight, port))

        def _cmp(a, b):
            if a[0] != b[0]:
                return a[0] - b[0]
            else:
                return b[1] - a[1]

        sorted_pairs = sorted(port_pairs, cmp=_cmp, key=lambda a: a[0])
        return [pair[1] for pair in sorted_pairs[:count]]

    def _select_ports_per_fabric(self, fabric_ports, slot_ports, port_map,
                                 used_ports, count):
        if count <= 0:
            count = 1

        selected_ports = set()
        ports_in_use = set(fabric_ports) & set(slot_ports) & set(used_ports)
        if len(ports_in_use) >= count:
            selected_ports.update(ports_in_use)
        else:
            candid_ports = set(fabric_ports) & set(slot_ports) - ports_in_use
            new_selects = self._select_optimal_ports(
                candid_ports, port_map, count - len(ports_in_use))
            selected_ports.update(ports_in_use)
            selected_ports.update(new_selects)

        return selected_ports

    def _select_ports_per_slot(self, fabrics, slot_ports, port_map,
                                used_ports, count):
        count_left = count
        selected_ports = set()

        for fabric in fabrics:
            ports = self._select_ports_per_fabric(
                fabric[1], slot_ports, port_map, used_ports,
                count_left // (len(fabrics) - fabrics.index(fabric))
            )
            selected_ports.update(ports)
            count_left -= len(ports)

        return selected_ports

    def _select_ports_per_contr(self, fabrics, slots, slot_map, port_map,
                                used_ports, count):
        count_left = count
        selected_ports = set()

        for slot in slots:
            ports = self._select_ports_per_slot(
                fabrics, slot_map[slot], port_map, used_ports,
                count_left // (len(slots) - slots.index(slot))
            )
            selected_ports.update(ports)
            count_left -= len(ports)

        return selected_ports

    def _select_ports_per_engine(self, fabrics, contr_map, slot_map, port_map,
                                 used_ports, count):
        count_left = count
        selected_ports = set()

        contrs = sorted(contr_map, key=lambda i: len(contr_map[i]))
        for contr in contrs:
            ports = self._select_ports_per_contr(
                fabrics, contr_map[contr], slot_map, port_map, used_ports,
                count_left // (len(contrs) - contrs.index(contr))
            )
            selected_ports.update(ports)
            count_left -= len(ports)

        return selected_ports

    def _get_fc_zone(self, wwns, host_id):
        engine_map, contr_map, slot_map, port_map = self._get_online_fc_ports()

        fabrics = self._get_fabric(wwns, list(port_map.keys()))
        self._filter_fabric(fabrics, host_id)

        count = constants.OPTIMAL_MULTIPATH_NUM // len(fabrics)
        used_ports = self._get_used_ports(host_id)

        selected_ports = set()
        engines = [e for e in engine_map]
        for eng in engines:
            ports = self._select_ports_per_engine(
                fabrics, contr_map, slot_map, port_map, used_ports,
                count // (len(engines) - engines.index(eng))
            )
            selected_ports.update(ports)
            count -= len(ports)

        ini_tgt_map = {}
        for fabric in fabrics:
            new_ports = list(selected_ports & set(fabric[1]) - set(used_ports))
            if not new_ports:
                continue
            for ini in fabric[0]:
                if ini not in ini_tgt_map:
                    ini_tgt_map[ini] = new_ports
                else:
                    ini_tgt_map[ini].extend(new_ports)

        return ini_tgt_map, selected_ports, port_map

    def build_ini_targ_map(self, wwns, host_id):
        ini_tgt_map, total_ports, port_map = self._get_fc_zone(wwns, host_id)

        new_ports = set()
        for v in six.itervalues(ini_tgt_map):
            new_ports |= set(v)

        portgroup_name = constants.PORTGROUP_PREFIX + host_id
        portgroup_id = self.client.get_tgt_port_group(portgroup_name)
        if not portgroup_id:
            portgroup_id = self.client.create_portg(portgroup_name)

        for port in new_ports:
            self.client.add_port_to_portg(portgroup_id, port_map[port]['id'])

        LOG.debug("build_ini_targ_map: Port group name: %(portg_name)s, "
                  "init_targ_map: %(map)s, target_wwns: %(wwns)s.",
                  {"portg_name": portgroup_name,
                   "map": ini_tgt_map,
                   "wwns": total_ports})
        return list(total_ports), portgroup_id, ini_tgt_map

    def get_init_targ_map(self, wwns, host_id):
        error_ret = ([], None, {})
        if not host_id:
            return error_ret

        view_name = constants.MAPPING_VIEW_PREFIX + host_id
        view_id = self.client.find_mapping_view(view_name)
        if not view_id:
            return error_ret
        portg_id = self.client.get_portgroup_by_view(view_id)

        ports = {}
        if portg_id:
            ports = self.client.get_fc_ports_by_portgroup(portg_id)

        for port_id in ports.values():
            self.client.remove_port_from_portgroup(portg_id, port_id)
        init_targ_map = {}
        for wwn in wwns:
            init_targ_map[wwn] = list(ports.keys())
        return list(ports.keys()), portg_id, init_targ_map
