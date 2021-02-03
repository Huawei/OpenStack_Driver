# Copyright (c) 2013 Huawei Technologies Co., Ltd.
# Copyright (c) 2012 OpenStack Foundation
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
"""
Volume Drivers for Huawei OceanStor T series storage arrays.
"""

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils

from cinder import exception
from cinder.i18n import _
from cinder.i18n import _LE
from cinder.i18n import _LI
from cinder.i18n import _LW
from cinder import utils
from cinder.volume import driver
from cinder.volume.drivers.huawei.extend import fc_zone_helper
from cinder.volume.drivers.huawei import ssh_client
from cinder.zonemanager import utils as fczm_utils

LOG = logging.getLogger(__name__)

FC_PORT_CONNECTED = '10'
contrs = ['A', 'B']

zone_manager_opts = [
    cfg.StrOpt('zone_driver',
               default='cinder.zonemanager.drivers.brocade.brcd_fc_zone_driver'
               '.BrcdFCZoneDriver',
               help='FC Zone Driver responsible for zone management')
]
huawei_opts = [
    cfg.StrOpt('cinder_huawei_conf_file',
               default='/etc/cinder/cinder_huawei_conf.xml',
               help='The configuration file for the Cinder Huawei driver.')
]

CONF = cfg.CONF
CONF.register_opts(huawei_opts)


class HuaweiTISCSIDriver(driver.ISCSIDriver):
    """ISCSI driver for Huawei OceanStor T series storage arrays."""

    VERSION = '1.1.0'

    def __init__(self, *args, **kwargs):
        super(HuaweiTISCSIDriver, self).__init__(*args, **kwargs)
        self.configuration = kwargs.get('configuration', None)
        if not self.configuration:
            msg = (_('_instantiate_driver: configuration not found.'))
            raise exception.InvalidInput(reason=msg)

        self.configuration.append_config_values(huawei_opts)

    def do_setup(self, context):
        """Instantiate common class."""
        self.sshclient = ssh_client.TseriesClient(
            configuration=self.configuration)
        self.sshclient.do_setup(context)
        self.sshclient.check_storage_pools()

    def check_for_setup_error(self):
        """Check something while starting."""
        self.sshclient.check_for_setup_error()

    def create_volume(self, volume):
        """Create a new volume."""
        return self.sshclient.create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot."""
        return self.sshclient.create_volume_from_snapshot(volume, snapshot)

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume."""
        return self.sshclient.create_cloned_volume(volume, src_vref)

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        self.sshclient.extend_volume(volume, new_size)

    def delete_volume(self, volume):
        """Delete a volume."""
        self.sshclient.delete_volume(volume)

    def create_export(self, context, volume, connector=None):
        """Export the volume."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def create_snapshot(self, snapshot):
        """Create a snapshot."""
        snapshot_id = self.sshclient.create_snapshot(snapshot)
        return {'provider_location': snapshot_id}

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        self.sshclient.delete_snapshot(snapshot)

    def initialize_ultrapath_connection(self, volume, connector):
        """Map a volume to a host and return target iSCSI information."""
        def get_targets_ips_info(initiator):
            iscsi_conf = self._get_iscsi_conf(self.configuration)
            target_ip = []

            if iscsi_conf['DefaultTargetIP']:
                for ip in iscsi_conf['DefaultTargetIP'].split(','):
                    target_ip.append(ip)

            if not target_ip:
                msg = (_('get_targets_ips_info: Failed to get target IP '
                         'for initiator %(ini)s, please check config file.')
                       % {'ini': initiator})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

            return self.sshclient.get_tgt_iqn_ultrapath(
                map(lambda x: x.strip(), target_ip))

        msg = (_('initialize_multipath_connection: volume name: %(vol)s, '
                 'host: %(host)s, initiator: %(ini)s')
               % {'vol': volume['name'],
                  'host': connector['host'],
                  'ini': connector['initiator']})
        LOG.debug(msg)
        self.sshclient.update_login_info()
        ips_info = get_targets_ips_info(connector['initiator'])

        # First, add a host if not added before.
        host_id = self.sshclient.add_host(connector['host'], connector['ip'],
                                          connector['initiator'])

        iscsi_conf = self._get_iscsi_conf(self.configuration)
        chapinfo = self.sshclient.find_chap_info(iscsi_conf,
                                                 connector['initiator'])
        used = self.sshclient.is_initiator_used_chap(connector['initiator'])
        if not chapinfo and used:
            msg = (_("Chap is not configed but initiator %s used chap on "
                     "array, please cheak and remove chap for this initiator.")
                   % connector['initiator'])
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Then, add the iSCSI port to the host.
        self.sshclient.add_iscsi_port_to_host(host_id, connector, chapinfo)

        # Finally, map the volume to the host.
        lun_id = self.sshclient.check_volume_exist_on_array(volume)
        if not lun_id:
            msg = _("Volume %s not exists on the array.") % volume['id']
            raise exception.VolumeBackendAPIException(data=msg)

        hostlun_id = self.sshclient.map_volume(host_id, lun_id)

        # Change LUN ctr for better performance, just for single path.
        lun_details = self.sshclient.get_lun_details(lun_id)

        target_portal_list = []
        target_iqn_list = []
        for info in ips_info:
            target_portal_list.append('%s:%s' % (info[1], '3260'))
            target_iqn_list.append(info[0])
        properties = {}
        properties['target_discovered'] = False
        properties['target_portal'] = target_portal_list
        properties['target_iqn'] = target_iqn_list
        properties['target_lun'] = int(hostlun_id)
        properties['volume_id'] = volume['id']
        properties['lun_wwn'] = lun_details['LUNWWN']
        properties['target_num'] = len(ips_info)
        properties['description'] = 'huawei'

        if chapinfo:
            properties['auth_method'] = 'CHAP'
            properties['auth_username'] = chapinfo[0]
            properties['auth_password'] = chapinfo[1]

        return {'driver_volume_type': 'iscsi', 'data': properties}

    def initialize_common_connection(self, volume, connector):
        """Map a volume to a host and return target iSCSI information."""
        msg = (_('initialize_common_connection: volume name: %(vol)s, '
                 'host: %(host)s, initiator: %(ini)s')
               % {'vol': volume['name'],
                  'host': connector['host'],
                  'ini': connector['initiator']})
        LOG.debug(msg)
        self.sshclient.update_login_info()
        (iscsi_iqn, target_ip, port_ctr) = (
            self._get_iscsi_params(connector['initiator']))

        # First, add a host if not added before.
        host_id = self.sshclient.add_host(connector['host'], connector['ip'],
                                          connector['initiator'])

        iscsi_conf = self._get_iscsi_conf(self.configuration)
        chapinfo = self.sshclient.find_chap_info(iscsi_conf,
                                                 connector['initiator'])
        used = self.sshclient.is_initiator_used_chap(connector['initiator'])
        if not chapinfo and used:
            msg = (_("Chap is not configed but initiator %s used chap on "
                     "array, please cheak and remove chap for this initiator.")
                   % connector['initiator'])
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Then, add the iSCSI port to the host.
        self.sshclient.add_iscsi_port_to_host(host_id, connector, chapinfo)

        # Finally, map the volume to the host.
        lun_id = self.sshclient.check_volume_exist_on_array(volume)
        if not lun_id:
            msg = _("Volume %s not exists on the array.") % volume['id']
            raise exception.VolumeBackendAPIException(data=msg)

        hostlun_id = self.sshclient.map_volume(host_id, lun_id)

        # Change LUN ctr for better performance, just for single path.
        lun_details = self.sshclient.get_lun_details(lun_id)

        properties = {}
        properties['target_discovered'] = False
        properties['target_portal'] = ('%s:%s' % (target_ip, '3260'))
        properties['target_iqn'] = iscsi_iqn
        properties['target_lun'] = int(hostlun_id)
        properties['volume_id'] = volume['id']
        properties['lun_wwn'] = lun_details['LUNWWN']
        properties['description'] = 'huawei'

        if chapinfo:
            properties['auth_method'] = 'CHAP'
            properties['auth_username'] = chapinfo[0]
            properties['auth_password'] = chapinfo[1]

        return {'driver_volume_type': 'iscsi', 'data': properties}

    @utils.synchronized('huawei_t_mount', external=False)
    def initialize_connection(self, volume, connector):
        """Map a volume to a host and return target iSCSI information."""
        if 'nova_use_ultrapath' in connector:
            if connector.get('nova_use_ultrapath'):
                return self.initialize_ultrapath_connection(volume, connector)
            else:
                return self.initialize_common_connection(volume, connector)

        if self.configuration.safe_get("use_ultrapath_for_image_xfer"):
            return self.initialize_ultrapath_connection(volume, connector)
        return self.initialize_common_connection(volume, connector)

    def _get_iscsi_params(self, initiator):
        """Get target iSCSI params, including iqn and IP."""
        iscsi_conf = self._get_iscsi_conf(self.configuration)
        target_ip = None
        for ini in iscsi_conf['Initiator']:
            if ini['Name'] == initiator:
                target_ip = ini['TargetIP']
                break
        # If didn't specify target IP for some initiator, use default IP.
        if not target_ip:
            if iscsi_conf['DefaultTargetIP']:
                target_ip = iscsi_conf['DefaultTargetIP']

            else:
                msg = (_('_get_iscsi_params: Failed to get target IP '
                         'for initiator %(ini)s, please check config file.')
                       % {'ini': initiator})
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        (target_iqn, port_ctr) = self.sshclient.get_tgt_iqn(target_ip)
        return (target_iqn, target_ip, port_ctr)

    def _get_iscsi_conf(self, configuration):
        """Get iSCSI info from config file.

        This function returns a dict:
        {'DefaultTargetIP': '11.11.11.11',
         'Initiator': [{'Name': 'iqn.xxxxxx.1', 'TargetIP': '11.11.11.12'},
                       {'Name': 'iqn.xxxxxx.2', 'TargetIP': '11.11.11.13'}
                      ]
        }

        """

        iscsiinfo = {}
        config_file = configuration.cinder_huawei_conf_file
        root = self.sshclient.parse_xml_file(config_file)

        default_ip = root.findtext('iSCSI/DefaultTargetIP')
        if default_ip:
            iscsiinfo['DefaultTargetIP'] = default_ip.strip()
        else:
            iscsiinfo['DefaultTargetIP'] = None
        initiator_list = []
        tmp_dic = {}
        for dic in root.findall('iSCSI/Initiator'):
            # Strip the values of dict.
            for k, v in dic.items():
                tmp_dic[k] = v.strip()
            initiator_list.append(tmp_dic)
        iscsiinfo['Initiator'] = initiator_list
        return iscsiinfo

    @utils.synchronized('huawei_t_mount', external=False)
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate the map."""
        # Check the connector, as we can't get initiatorname during
        # local_delete and the whole info in force-delete
        if ('host' not in connector) or ('initiator' not in connector):
            LOG.info(_LI("terminate_connection: delete or force delete."))
            return

        host_name = connector['host']
        iqn = connector['initiator']
        LOG.debug('terminate_connection: volume: %(vol)s, host: %(host)s, '
                  'connector: %(initiator)s'
                  % {'vol': volume['name'],
                     'host': host_name,
                     'initiator': iqn})

        self.sshclient.update_login_info()
        lun_id = self.sshclient.check_volume_exist_on_array(volume)
        iscsi_conf = self._get_iscsi_conf(self.configuration)
        chapinfo = self.sshclient.find_chap_info(iscsi_conf,
                                                 connector['initiator'])

        if not lun_id:
            LOG.warning(_LW("Volume %s not exists on the array."),
                        volume['id'])
        host_id = self.sshclient.get_host_id(host_name, iqn)
        self.sshclient.remove_map(lun_id, host_id)

        if (host_id is not None
                and not self.sshclient.get_host_map_info(host_id)):
            if (chapinfo and self.sshclient._chapuser_added_to_initiator(
                    connector['initiator'], chapinfo[0])):
                self.sshclient._remove_chap(connector['initiator'], chapinfo)

        info = {'driver_volume_type': 'iSCSI',
                'data': {'iqn': iqn}}
        LOG.info(_LI('terminate_connection, return data is: %s.'), info)
        return info

    def _remove_iscsi_port(self, hostid, initiator):
        """Remove iSCSI ports and delete host."""
        # Delete the host initiator if no LUN mapped to it.
        port_num = 0
        port_info = self.sshclient.get_host_port_info(hostid)
        if port_info:
            port_num = len(port_info)
            for port in port_info:
                if port[2] == initiator:
                    self.sshclient.delete_hostport(port[0])
                    port_num -= 1
                    break
        else:
            LOG.warning(_LW('_remove_iscsi_port: iSCSI port was not found '
                            'on host %(hostid)s.'), {'hostid': hostid})

        # Delete host if no initiator added to it.
        if port_num == 0:
            self.sshclient.delete_host(hostid)

    def get_volume_stats(self, refresh=False):
        """Get volume stats."""
        self._stats = self.sshclient.get_volume_stats(refresh)
        self._stats['storage_protocol'] = 'iSCSI'
        self._stats['driver_version'] = self.VERSION
        backend_name = self.configuration.safe_get('volume_backend_name')
        self._stats['volume_backend_name'] = (backend_name or
                                              self.__class__.__name__)
        return self._stats


class HuaweiTFCDriver(driver.FibreChannelDriver):
    """FC driver for Huawei OceanStor T series storage arrays."""

    VERSION = '1.0.0'

    def __init__(self, *args, **kwargs):
        super(HuaweiTFCDriver, self).__init__(*args, **kwargs)
        self.configuration = kwargs.get('configuration', None)
        if not self.configuration:
            msg = (_('_instantiate_driver: configuration not found.'))
            raise exception.InvalidInput(reason=msg)
        # zone manager
        self.zm = None

        self.configuration.append_config_values(huawei_opts)

    def do_setup(self, context):
        """Instantiate common class."""
        self.sshclient = ssh_client.TseriesClient(
            configuration=self.configuration)
        self.sshclient.do_setup(context)
        self.sshclient.check_storage_pools()

    def check_for_setup_error(self):
        """Check something while starting."""
        self.sshclient.check_for_setup_error()

    def create_volume(self, volume):
        """Create a new volume."""
        return self.sshclient.create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a volume from a snapshot."""
        return self.sshclient.create_volume_from_snapshot(volume, snapshot)

    def create_cloned_volume(self, volume, src_vref):
        """Create a clone of the specified volume."""
        return self.sshclient.create_cloned_volume(volume, src_vref)

    def extend_volume(self, volume, new_size):
        """Extend a volume."""
        self.sshclient.extend_volume(volume, new_size)

    def delete_volume(self, volume):
        """Delete a volume."""
        self.sshclient.delete_volume(volume)

    def create_export(self, context, volume, connector=None):
        """Export the volume."""
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreate an export for a volume."""
        pass

    def remove_export(self, context, volume):
        """Remove an export for a volume."""
        pass

    def create_snapshot(self, snapshot):
        """Create a snapshot."""
        snapshot_id = self.sshclient.create_snapshot(snapshot)
        return {'provider_location': snapshot_id}

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        self.sshclient.delete_snapshot(snapshot)

    def validate_connector(self, connector):
        """Check for wwpns in connector."""
        if 'wwpns' not in connector:
            err_msg = (_LE('validate_connector: The FC driver requires the'
                           ' wwpns in the connector.'))
            LOG.error(err_msg)
            raise exception.InvalidConnectorException(missing='wwpns')

    @utils.synchronized('huawei_t_mount', external=False)
    def initialize_connection(self, volume, connector):
        """Create FC connection between a volume and a host."""
        LOG.debug('initialize_connection: volume name: %(vol)s, '
                  'host: %(host)s, initiator: %(wwn)s'
                  % {'vol': volume['name'],
                     'host': connector['host'],
                     'wwn': connector['wwpns']})
        lun_id = self.sshclient.check_volume_exist_on_array(volume)
        if not lun_id:
            msg = _("Volume %s not exists on the array.") % volume['id']
            raise exception.VolumeBackendAPIException(data=msg)

        self.sshclient.update_login_info()
        # First, add a host if it is not added before.
        host_id = self.sshclient.add_host(connector['host'], connector['ip'])
        # Then, add free FC ports to the host.
        wwns = connector['wwpns']
        if not self.zm:
            self.zm = fczm_utils.create_zone_manager()
        if self.zm:
            # Use FC switch
            zone_helper = fc_zone_helper.FCZoneHelper(self.zm, self.sshclient)
            port_list = self.sshclient.get_all_fc_ports_from_array()
            (tgt_port_wwns,
             init_targ_map) = zone_helper.build_ini_tgt_map(wwns, host_id,
                                                            port_list, True)

        else:
            free_wwns = self.sshclient.get_connected_free_wwns()
            for wwn in free_wwns:
                if wwn in wwns:
                    self.sshclient.add_fc_port_to_host(host_id, wwn)
            fc_port_details = self.sshclient.get_host_port_details(host_id)
            tgt_port_wwns = self._get_tgt_fc_port_wwns(fc_port_details)

        LOG.debug('initialize_connection: Target FC ports WWNS: %s'
                  % tgt_port_wwns)

        try:
            hostlun_id = self.sshclient.map_volume(host_id, lun_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                # Remove the FC port from the host if the map failed.
                self._remove_fc_ports(host_id, wwns)

        properties = {}
        properties['target_discovered'] = False
        properties['target_wwn'] = tgt_port_wwns
        properties['target_lun'] = int(hostlun_id)
        properties['volume_id'] = volume['id']

        return {'driver_volume_type': 'fibre_channel',
                'data': properties}

    def _get_tgt_fc_port_wwns(self, port_details):
        wwns = []
        for port in port_details:
            wwns.append(port['TargetWWN'])
        return wwns

    @utils.synchronized('huawei_t_mount', external=False)
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate the map."""
        wwns = connector['wwpns']
        host_name = connector['host']
        LOG.debug('terminate_connection: volume: %(vol)s, host: %(host)s, '
                  'connector: %(wwpns)s'
                  % {'vol': volume['name'],
                     'host': host_name,
                     'wwpns': wwns})

        lun_id = self.sshclient.check_volume_exist_on_array(volume)
        if not lun_id:
            LOG.warning(_LW("Volume %s not exists on the array."),
                        volume['id'])

        self.sshclient.update_login_info()
        host_id = self.sshclient.get_host_id(host_name)
        self.sshclient.remove_map(lun_id, host_id)

        # Remove all FC ports and delete the host if no volume mapping to it.
        if host_id and not self.sshclient.get_host_map_info(host_id):
            self._delete_zone_and_remove_initiators(wwns, host_id)

        info = {'driver_volume_type': 'fibre_channel',
                'data': {'wwns': wwns}}
        LOG.info(_LI('terminate_connection, return data is: %s.'), info)
        return info

    def _delete_zone_and_remove_initiators(self, wwns, host_id):
        if host_id is None:
            return

        self._remove_fc_ports(host_id, wwns)

        if not self.zm:
            self.zm = fczm_utils.create_zone_manager()
        if self.zm:
            # Use FC switch, need to delete zone
            # Use FC switch
            zone_helper = fc_zone_helper.FCZoneHelper(self.zm, self.sshclient)
            port_list = self.sshclient.get_all_fc_ports_from_array()
            (tgt_port_wwns,
             init_targ_map) = zone_helper.build_ini_tgt_map(wwns, host_id,
                                                            port_list, True)
            self.zm.delete_connection(init_targ_map)

    def _remove_fc_ports(self, hostid, wwns):
        """Remove FC ports and delete host."""
        port_num = 0
        port_info = self.sshclient.get_host_port_info(hostid)
        if port_info:
            port_num = len(port_info)
            for port in port_info:
                if port[2] in wwns:
                    self.sshclient.delete_hostport(port[0])
                    port_num -= 1
        else:
            LOG.warning(_LW('_remove_fc_ports: FC port was not found '
                            'on host %(hostid)s.'), {'hostid': hostid})

        if port_num == 0:
            self.sshclient.delete_host(hostid)

    def get_volume_stats(self, refresh=False):
        """Get volume stats."""
        self._stats = self.sshclient.get_volume_stats(refresh)
        self._stats['storage_protocol'] = 'FC'
        self._stats['driver_version'] = self.VERSION
        backend_name = self.configuration.safe_get('volume_backend_name')
        self._stats['volume_backend_name'] = (backend_name or
                                              self.__class__.__name__)
        return self._stats
