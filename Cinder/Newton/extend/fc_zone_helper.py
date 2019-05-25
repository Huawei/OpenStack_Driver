# Copyright (c) 2015 Huawei Technologies Co., Ltd.
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

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import importutils

from cinder import exception
from cinder.i18n import _, _LE, _LI
from cinder.volume import configuration as config
from cinder.zonemanager import utils as fczm_utils

LOG = logging.getLogger(__name__)

controller_list = ['A', 'B', 'C', 'D']

zone_manager_opts = [
    cfg.StrOpt('zone_driver',
               default='cinder.zonemanager.drivers.brocade.brcd_fc_zone_driver'
               '.BrcdFCZoneDriver',
               help='FC Zone Driver responsible for zone management')
]


class FCZoneHelper(object):
    """FC zone helper for Huawei driver."""

    def __init__(self, zm, client):
        self.zm = zm
        self.client = client

    def _check_fc_port_and_init(self, wwns, hostid, fabric_map, nsinfos):
        """Check FC port on array and wwn on host is connected to switch.

        If no FC port on array is connected to switch or no ini on host is
        connected to switch, raise a error.
        """
        if not fabric_map:
            msg = _('No FC port on array is connected to switch.')
            LOG.error(msg)
            raise exception.CinderException(msg)

        no_wwn_connected_to_switch = True
        for wwn in wwns:
            formatted_initiator = fczm_utils.get_formatted_wwn(wwn)
            for fabric in fabric_map:
                nsinfo = nsinfos[fabric]
                if formatted_initiator in nsinfo:
                    no_wwn_connected_to_switch = False
                    self.client.ensure_fc_initiator_added(wwn, hostid)
                    break
        if no_wwn_connected_to_switch:
            msg = _('No wwn on host is connected to switch.')
            LOG.error(msg)
            raise exception.CinderException(msg)

    def build_ini_tgt_map(self, wwns, host_id, port_list, is_add):
        fabric_map = self.zm.get_san_context(port_list)

        nsinfos = {}
        cfgmap_from_fabrics = {}
        for fabric in fabric_map:
            nsinfos[fabric] = self._get_nameserver_info(fabric)
            cfgmap_from_fabric = self._get_active_zone_set(fabric)
            cfgmap_from_fabrics[fabric] = cfgmap_from_fabric

        self._check_fc_port_and_init(wwns, host_id, fabric_map, nsinfos)
        return self._build_ini_tgt_map(wwns, is_add, nsinfos,
                                       cfgmap_from_fabrics)

    def _build_ini_tgt_map(self, wwns, need_add_con, nsinfos,
                           cfgmap_from_fabrics):
        tgt_port_wwns = []
        init_targ_map_total = {}
        fabric_maps = {}
        for contr in controller_list:
            port_list_from_contr = self.client.get_fc_ports_from_contr(contr)
            if port_list_from_contr:
                fabric_map = self.zm.get_san_context(port_list_from_contr)
                fabric_maps[contr] = fabric_map
        for wwn in wwns:
            init_targ_map = {}
            tmp_port_list = []
            tgt_port_for_map = []
            tmp_flag = False
            need_new_zone = False
            for contr in fabric_maps:
                (fc_port_for_zone, tmp_flag) = \
                    self._get_one_fc_port_for_zone(wwn, contr, nsinfos,
                                                   cfgmap_from_fabrics,
                                                   fabric_maps)
                if tmp_flag:
                    need_new_zone = True
                if fc_port_for_zone:
                    tgt_port_wwns.append(fc_port_for_zone)
                    if not tmp_flag:
                        tgt_port_for_map.append(fc_port_for_zone)
                    if tmp_flag:
                        tmp_port_list.append(fc_port_for_zone)

            init_targ_map[wwn] = tmp_port_list
            LOG.debug("tmp_port_list: %s" % tmp_port_list)
            init_targ_map_total[wwn] = tgt_port_for_map
            if need_new_zone and need_add_con:
                LOG.debug("Got init_targ_map to create zone: %s"
                          % init_targ_map)
                self.zm.add_connection(init_targ_map)

        tgt_port_wwns = list(set(tgt_port_wwns))

        return (tgt_port_wwns, init_targ_map_total)

    def _get_fabric_vendor(self):
        zone_config = config.Configuration(zone_manager_opts,
                                           'fc-zone-manager')
        fabric_driver = zone_config.zone_driver
        LOG.debug('Using fabric driver: %s' % fabric_driver)
        driver_vendor = None
        try:
            driver_vendor = fabric_driver.split('.')[3]
        except Exception:
            msg = _('Get fabric driver vendor error.')
            LOG.exception(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return driver_vendor

    def _get_nameserver_info(self, fabric):
        driver_vendor = self._get_fabric_vendor()
        if driver_vendor == 'brocade':
            nsinfo = self._get_brcd_nsinfo(fabric)
        elif driver_vendor == 'cisco':
            nsinfo = self._get_cisco_nsinfo(fabric)
        else:
            msg = ('Unsupported fabric, vendor name: %s.' % driver_vendor)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return nsinfo

    def _get_cisco_config(self, fabric):
        fabric_ip = self.zm.driver.fabric_configs[fabric].safe_get(
            'cisco_fc_fabric_address')
        fabric_user = self.zm.driver.fabric_configs[fabric].safe_get(
            'cisco_fc_fabric_user')
        fabric_pwd = self.zm.driver.fabric_configs[fabric].safe_get(
            'cisco_fc_fabric_password')
        fabric_port = self.zm.driver.fabric_configs[fabric].safe_get(
            'cisco_fc_fabric_port')
        zoning_vsan = self.zm.driver.fabric_configs[fabric].safe_get(
            'cisco_zoning_vsan')
        return (fabric_ip, fabric_user, fabric_pwd, fabric_port, zoning_vsan)

    def _get_brcd_nsinfo(self, fabric):
        conn = self.zm.driver._get_cli_client(fabric)
        try:
            nsinfo = conn.get_nameserver_info()
            LOG.debug("name server info from fabric: %s", nsinfo)
            conn.cleanup()
        except exception.BrocadeZoningCliException:
            if not conn.is_supported_firmware():
                msg = _("Unsupported firmware on switch %s. Make sure "
                        "switch is running firmware v6.4 or higher."
                        ) % conn.switch_ip
                LOG.error(msg)
                raise exception.FCZoneDriverException(msg)
            with excutils.save_and_reraise_exception():
                LOG.exception(_LE("Error getting name server info."))
        except Exception:
            msg = _("Failed to get name server info.")
            LOG.exception(msg)
            raise exception.FCZoneDriverException(msg)
        return nsinfo

    def _get_cisco_nsinfo(self, fabric):
        (fabric_ip, fabric_user, fabric_pwd, fabric_port, zoning_vsan) = (
            self._get_cisco_config(fabric))
        try:
            conn = importutils.import_object(
                self.zm.driver.configuration.cisco_sb_connector,
                ipaddress=fabric_ip,
                username=fabric_user,
                password=fabric_pwd, port=fabric_port,
                vsan=zoning_vsan)
            nsinfo = conn.get_nameserver_info()
            LOG.debug("name server info from fabric: %s",
                      nsinfo)
            conn.cleanup()
        except exception.CiscoZoningCliException:
            with excutils.save_and_reraise_exception():
                LOG.exception(_LE("Error getting show fcns database "
                                  "info."))
        except Exception:
            msg = ("Failed to get show fcns database info.")
            LOG.exception(msg)
            raise exception.FCZoneDriverException(msg)
        return nsinfo

    def _get_one_fc_port_for_zone(self, initiator, contr, nsinfos,
                                  cfgmap_from_fabrics, fabric_maps):
        """Get on FC port per one controller.

        task flow:
        1. Get all the FC port from the array.
        2. Filter out ports belonged to the specific controller
           and the status is connected.
        3. Filter out ports connected to the fabric configured in cinder.conf.
        4. Get active zones set from switch.
        5. Find a port according to three cases.
        """
        LOG.info(_LI("Get in function _get_one_fc_port_for_zone. "
                     "Initiator: %s"), initiator)

        formatted_initiator = fczm_utils.get_formatted_wwn(initiator)
        fabric_map = fabric_maps[contr]
        if not fabric_map:
            return (None, False)

        port_zone_number_map = {}

        for fabric in fabric_map:
            LOG.info(_LI("Dealing with fabric: %s"), fabric)
            nsinfo = nsinfos[fabric]
            if formatted_initiator not in nsinfo:
                continue

            final_port_list_per_fabric = fabric_map[fabric]
            cfgmap_from_fabric = cfgmap_from_fabrics[fabric]

            zones_members = cfgmap_from_fabric['zones'].values()

            for port in final_port_list_per_fabric:
                port_zone_number_map[port] = 0
                formatted_port = fczm_utils.get_formatted_wwn(port)
                for zones_member in zones_members:
                    if formatted_port in zones_member:
                        # For the second case use.
                        if formatted_initiator in zones_member:
                            # First case: found a port in the same
                            # zone with the given initiator.
                            return (port, False)
                        # For the third case use.
                        port_zone_number_map[port] += 1
        if port_zone_number_map == {}:
            return (None, False)

        temp_list = []
        temp_list = sorted(port_zone_number_map.items(), key=lambda d: d[1])
        # Third case: find a port referenced in fewest zone.
        return (temp_list[0][0], True)

    def _get_active_zone_set(self, fabric):
        driver_vendor = self._get_fabric_vendor()
        if driver_vendor == 'brocade':
            conn = self.zm.driver._get_cli_client(fabric)
            cfgmap_from_fabric = self.zm.driver._get_active_zone_set(conn)
            conn.cleanup()
        elif driver_vendor == 'cisco':
            (fabric_ip, fabric_user, fabric_pwd, fabric_port, zoning_vsan) = (
                self._get_cisco_config(fabric))
            cfgmap_from_fabric = self.zm.driver.get_active_zone_set(
                fabric_ip, fabric_user, fabric_pwd, fabric_port, zoning_vsan)
        else:
            msg = ('Unsupported fabric, vendor name: %s.' % driver_vendor)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return cfgmap_from_fabric
