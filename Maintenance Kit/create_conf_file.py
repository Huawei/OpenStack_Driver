#!/usr/bin/python
# Copyright (c) 2015 - 2016 Huawei Technologies Co., Ltd.
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
import codecs
import ConfigParser
import getpass
import os
import re

from xml.dom.minidom import Document
from xml.etree import ElementTree as ET

Product_Table = {'1': 'T',
                 '2': 'V3',
                 '3': '18000',
                 '4': 'Dorado'}
Product_Protocol_Table = {'1': 'FC',
                          '2': 'iSCSI'}
Product_LUNType_Table = {'1': 'Thin',
                         '2': 'Thick'}
OSType_Table = {"1": "Linux",
                "2": "Windows",
                "3": "Solaris",
                "4": "HP-UX",
                "5": "AIX",
                "6": "XenServer",
                "7": "Mac OS",
                "8": "VMware ESX",
                "9": "Windows Server 2012"}
Cinder_Conf_Table = ['volume_driver', 'cinder_huawei_conf_file',
                     'volume_backend_name']
Manila_Conf_Table = ['share_driver', 'manila_huawei_conf_file',
                     'share_backend_name', 'driver_handles_share_servers']
Modle_Table = {'1': 'cinder',
               '2': 'manila'}
CINDER_CONF_EXTRA_FEATURE = ("Extra feature config:\n"
                             "    1: Replication\n"
                             "    2: Hypermetro\n"
                             "    3: FC auto zoning\n"
                             "    4: Backup snapshot\n"
                             "    5: Exit\n"
                             "Enter your choice<1-5>:")
CINDER_XML_EXTRA_FEATURE = ("Extra feature config:\n"
                            "    1: Iscsi multipath\n"
                            "    2: Chap and Alua\n"
                            "    3: Exit\n"
                            "Enter your choice<1-3>:")
MANILA_EXTRA_FEATURE = ("Extra feature config:\n"
                        "    1: Multi-tenant\n"
                        "    2: Exit\n"
                        "Enter your choice<1-2>:")


class CreateConfFile(object):
    def __init__(self):
        self.xml_file_name = ''
        self.conf_name = ''
        self.backend_name = ''
        self.welcome = ("==================================="
                        "===================================\n"
                        "    Welcome to use Huawei Oceanstor OpenStack "
                        "Driver Config Tool !\n"
                        "==================================="
                        "===================================\n"
                        "You are configing the Huawei storage "
                        "driver for OpenStack Cinder or \n"
                        "Manila, this would create a "
                        "cinder_huawei_conf_'BackendName'.xml in \n"
                        "/etc/cinder/ or manila_huawei_conf_'BackendName'.xml"
                        " in /etc/manila/.\n"
                        "-----------------------------------"
                        "-----------------------------------")
        self.conf_file_exit = False
        self.SVP_IP = ''
        self.ControllerIPs = []
        self.ControllerIP_Num = 0
        self.Product = ''
        self.Protocol = ''
        self.HostIP = '10.0.0.0'
        self.OSType = 'Linux'
        self.LUNType = 'Thick'
        self.StoragePool = ''
        self.DefaultTargetIP = ''

    def Indent(self, dom, node, indent=0):
        children = node.childNodes[:]
        if indent:
            text = dom.createTextNode('\n' + '\t' * indent)
            node.parentNode.insertBefore(text, node)
        if children:
            if children[-1].nodeType == node.ELEMENT_NODE:
                text = dom.createTextNode('\n' + '\t' * indent)
                node.appendChild(text)
            for n in children:
                if n.nodeType == node.ELEMENT_NODE:
                    self.Indent(dom, n, indent + 1)

    def check_cinder_xmlfile(self):
        is_file_exit = os.path.isfile(self.xml_file_name)
        if is_file_exit is False:
            self.conf_file_exit = False
            return
        self.conf_file_exit = True
        tree = ET.parse(self.xml_file_name)
        root = tree.getroot()
        self.Product = root.findtext('Storage/Product').strip()
        self.Protocol = root.findtext('Storage/Protocol').strip()

        if self.Product == 'T':
            while True:
                text = ("Storage/ControllerIP%s" % (self.ControllerIP_Num))
                node = root.find(text)
                if node is None:
                    break
                else:
                    controllerIP_Tmp = root.findtext(text).strip()
                    self.ControllerIPs.append(controllerIP_Tmp)
                    self.ControllerIP_Num = self.ControllerIP_Num + 1
        self.RestURL = root.findtext('Storage/RestURL').strip()
        if self.RestURL:
            self.SVP_IP = self.RestURL[8:]
            self.SVP_IP = self.SVP_IP[:-20]
            if self.Product == 'V3':
                self.SVP_IP = self.SVP_IP.split(':')[0]

        if root.findtext('LUN/LUNType'):
            self.LUNType = root.findtext('LUN/LUNType').strip()
        if self.Product in ['V3', '18000']:
            self.StoragePool = root.findtext('LUN/StoragePool').strip()
        if self.Product == 'T':
            StoragePool = root.find('LUN/StoragePool')
            self.StoragePool = StoragePool.get('Name')
        self.DefaultTargetIP = root.findtext('iSCSI/DefaultTargetIP').strip()
        host = root.find('Host')
        if len(host):
            self.HostIP = host.get('HostIP')
            self.OSType = host.get('OSType')

    def create_xmlfile(self):
        print(self.welcome)  # noqa
        self.backend_name = self._raw_input("BackendName:")
        if not self.backend_name:
            self.backend_name = self._raw_input("BackendName:")
            if not self.backend_name:
                raise
        xml_path = ('/etc/cinder/cinder_huawei_conf%s.xml'
                    % ('_' + self.backend_name))
        if os.path.isfile(xml_path):
            print('%s is exist, please input a new backend name!' % xml_path)  # noqa
            self.backend_name = self._raw_input("BackendName:")
            new_xml_path = ('/etc/cinder/cinder_huawei_conf%s.xml'
                            % ('_' + self.backend_name))
            if os.path.isfile(new_xml_path):
                raise

        valid = self.validate_backend_name(self.backend_name)
        if not valid:
            msg = ('Backend name (%s) is invalid. Only support English '
                   'letters, numbers, underline(_) and short dashes(-).'
                   % self.backend_name)
            self.print_message(msg)
            raise

        text_Modle = ("Model:\n"
                      "    1: cinder\n"
                      "    2: manila\n"
                      "Enter your choice<1-2>:")
        self.model = self._raw_input(text_Modle)
        if self.model in Modle_Table.keys():
            if self.model == '1':
                self.xml_file_name = ('/etc/cinder/cinder_huawei_conf%s.xml'
                                      % ('_' + self.backend_name))
                self.conf_name = '/etc/cinder/cinder.conf'
                self.check_cinder_xmlfile()
                self.create_cinder_xmlfile()
                self.change_conf_file_owner(self.conf_name, self.xml_file_name)
                msg = ('Create %s Successfully!' % self.xml_file_name)
                self.print_message(msg)
                self.build_cinder_conf()
                conf_msg = ('Modify %s Successfully!' % self.conf_name)
                self.print_message(conf_msg)
            if self.model == '2':
                self.xml_file_name = ('/etc/manila/manila_huawei_conf%s.xml'
                                      % ('_' + self.backend_name))
                self.conf_name = '/etc/manila/manila.conf'
                self.create_manila_xmlfile()
                self.change_conf_file_owner(self.conf_name, self.xml_file_name)
                msg = ('Create %s Successfully!' % self.xml_file_name)
                self.print_message(msg)
                self.build_manila_conf()
                conf_msg = ('Modify %s Successfully!' % self.conf_name)
                self.print_message(conf_msg)
        else:
            raise

    def create_cinder_xmlfile(self):
        self.doc = Document()
        self.config = self.doc.createElement('config')
        self.doc.appendChild(self.config)
        self.storage = self.doc.createElement('Storage')
        self.config.appendChild(self.storage)
        if self.conf_file_exit:
            text_Product = ("Product:[%s]" % (self.Product))
            text_Protocol = ("Protocol:[%s]" % (self.Protocol))
            text_LUNType = ("LUNType:[%s]" % (self.LUNType))
            text_StoragePool = ("StoragePoolName:[%s]" % (self.StoragePool))
            text_HostIP = ("HostIP:[%s]" % (self.HostIP))
            text_OSType = ("OSType:[%s]" % (self.OSType))
            if self.Product == 'T':
                text_ControllerIP_Num = ("The number of Control ip "
                                         "you want to config:[%d]"
                                         % (self.ControllerIP_Num))
            else:
                text_ControllerIP_Num = ("The number of Control ip "
                                         "you want to config:")
            if self.Protocol == 'FC':
                text_DefaultTargetIP = ("iSCSI IP:")
            else:
                text_DefaultTargetIP = ("iSCSI IP:[%s]"
                                        % (self.DefaultTargetIP))

        else:
            text_Product = ("Product:\n"
                            "    1: T series\n"
                            "    2: V3 series\n"
                            "    3: 18000 series\n"
                            "    4: Dorado series\n"
                            "Enter your choice<1-4>:")
            text_Protocol = ("Protocol:\n"
                             "    1: FC\n"
                             "    2: iSCSI\n"
                             "Enter your choice<1-2>:")
            text_LUNType = ("LUNType:\n"
                            "    1: Thin\n"
                            "    2: Thick\n"
                            "Enter your choice<1-2>:")
            text_StoragePool = ("StoragePoolName:")
            text_DefaultTargetIP = ("iSCSI IP:")
            text_HostIP = ("HostIP(Local host ip):")
            text_OSType = ("OSType:\n"
                           "    1: Linux\n"
                           "    2: Windows\n"
                           "    3: Solaris\n"
                           "    4: HP-UX\n"
                           "    5: AIX\n"
                           "    6: XenServer\n"
                           "    7: Mac OS\n"
                           "    8: VMware ESX\n"
                           "    9: Windows Server 2012\n"
                           "Enter your choice<1-9>:")
            text_ControllerIP_Num = ("The number of Control ip "
                                     "you want to cinfig:")

        if self.SVP_IP:
            text_SVP_IP = ("Storage IP:[%s]" % (self.SVP_IP))
        else:
            text_SVP_IP = ("Storage IP:")
        self.create_product(text_Product)
        self.create_protocol(text_Protocol, text_ControllerIP_Num)
        self.create_svp_ip(text_SVP_IP)
        self.create_user_password()
        self.create_luntype(text_LUNType)
        self.create_StoragePool(text_StoragePool)
        self.create_iscsi(text_DefaultTargetIP)
        self.create_host(text_HostIP, text_OSType)

        doccopy = self.doc.cloneNode(True)
        self.Indent(doccopy, doccopy.documentElement)
        cinder_conf_file = open(self.xml_file_name, 'wb')
        writer = codecs.lookup('utf-8')[3](cinder_conf_file)
        doccopy.writexml(writer, encoding = 'utf-8')
        cinder_conf_file.close()
        doccopy.unlink()

        cinder_conf_file = open(self.xml_file_name, 'rb')
        file_content = cinder_conf_file.read()
        cinder_conf_file.close()

        pos = file_content.find('<config>')
        file_content = file_content[:pos] + '\n' + file_content[pos:]
        cinder_conf_file = open(self.xml_file_name, 'wb')
        cinder_conf_file.write(file_content)
        cinder_conf_file.close()

    def create_manila_xmlfile(self):
        self.doc = Document()
        self.config = self.doc.createElement('config')
        self.doc.appendChild(self.config)
        self.storage = self.doc.createElement('Storage')
        self.config.appendChild(self.storage)
        if self.conf_file_exit:
            text_Product = ("Product:[%s]" % (self.Product))
            text_Protocol = ("Protocol:[%s]" % (self.Protocol))
            text_StoragePool = ("StoragePoolName:[%s]" % (self.StoragePool))
            v_Product = self.Product
            v_Protocol = self.Protocol
            LogicalPortIP = ("LogicalPortIP:[%s]" % (self.DefaultTargetIP))

        else:
            text_Product = None
            text_LUNType = ("LUNType:\n"
                            "    1: Thin\n"
                            "    2: Thick\n"
                            "Enter your choice<1-2>:")
            text_StoragePool = ("StoragePoolName:")
            LogicalPortIP = ("LogicalPortIP:")
            text_SVP_IP = ("Storage IP:")
        self.create_product(text_Product)
        self.create_logic_ip(LogicalPortIP)
        self.create_svp_ip(text_SVP_IP)
        self.create_user_password()
        self.create_StoragePool(text_StoragePool)

        doccopy = self.doc.cloneNode(True)
        self.Indent(doccopy, doccopy.documentElement)
        cinder_conf_file = open(self.xml_file_name, 'wb')
        writer = codecs.lookup('utf-8')[3](cinder_conf_file)
        doccopy.writexml(writer, encoding='utf-8')
        cinder_conf_file.close()
        doccopy.unlink()

        cinder_conf_file = open(self.xml_file_name, 'rb')
        file_content = cinder_conf_file.read()
        cinder_conf_file.close()

        pos = file_content.find('<config>')
        file_content = file_content[:pos] + '\n' + file_content[pos:]
        cinder_conf_file = open(self.xml_file_name, 'wb')
        cinder_conf_file.write(file_content)
        cinder_conf_file.close()

    def create_product(self, text_Product):
        v_Product_num = None
        if self.model == '1':
            v_Product_num = self._raw_input(text_Product)
            if v_Product_num in Product_Table.keys():
                v_Product = Product_Table[v_Product_num]
                self.Product = v_Product
            else:
                raise
        if self.model == '2':
            self.Product = 'V3'
        Product = self.doc.createElement('Product')
        Product_text = self.doc.createTextNode(self.Product)
        Product.appendChild(Product_text)
        self.storage.appendChild(Product)
        if v_Product_num and v_Product_num == '4':
            self.Product = 'V3'

    def create_protocol(self, text_Protocol, text_ControllerIP_Num):
        v_Protocol_num = self._raw_input(text_Protocol)
        if v_Protocol_num in Product_Protocol_Table.keys():
            v_Protocol = Product_Protocol_Table[v_Protocol_num]
            self.Protocol = v_Protocol
        else:
            raise
        Protocol = self.doc.createElement('Protocol')
        Protocol_text = self.doc.createTextNode(self.Protocol)
        Protocol.appendChild(Protocol_text)
        self.storage.appendChild(Protocol)

        if self.Product == 'T':
            ControllerIP_Num_str = self._raw_input(text_ControllerIP_Num)
            if (not ControllerIP_Num_str) and (self.ControllerIP_Num):
                ControllerIP_Num_int = self.ControllerIP_Num
            else:
                ControllerIP_Num_int = int(ControllerIP_Num_str)
            controllerIPs = []
            for i in range(1, ControllerIP_Num_int + 1):
                if ((self.ControllerIP_Num >= ControllerIP_Num_int)
                   and self.ControllerIPs[i - 1]):
                    text = ("ControllerIP%s:[%s]"
                            % ((i - 1), (self.ControllerIPs[i - 1])))
                else:
                    text = ("ControllerIP%s:" % (i - 1))
                controllerIP_Tmp = self._raw_input(text)
                if (not controllerIP_Tmp) and (self.ControllerIPs[i - 1]):
                    controllerIPs.append(self.ControllerIPs[i - 1])
                else:
                    controllerIPs.append(controllerIP_Tmp)

            for i in range(1, ControllerIP_Num_int + 1):
                text = ("ControllerIP%s" % (i - 1))
                controllerip = self.doc.createElement(text)
                controllerip_text = (
                    self.doc.createTextNode(controllerIPs[i - 1]))
                controllerip.appendChild(controllerip_text)
                self.storage.appendChild(controllerip)

    def create_svp_ip(self, text_SVP_IP):
        if self.Product == '18000':
            SVP_IP = self._raw_input(text_SVP_IP)
            if SVP_IP:
                self.SVP_IP = SVP_IP
            V_RestURL = 'https://' + self.SVP_IP + '/deviceManager/rest/'
        if self.Product == 'V3':
            SVP_IP = self._raw_input(text_SVP_IP)
            if SVP_IP:
                self.SVP_IP = SVP_IP
            V_RestURL = 'https://' + self.SVP_IP + ':8088/deviceManager/rest/'
        if self.Product == 'T':
            V_RestURL = ''
        RestURL = self.doc.createElement('RestURL')
        RestURL_text = self.doc.createTextNode(V_RestURL)
        RestURL.appendChild(RestURL_text)
        self.storage.appendChild(RestURL)

    def create_user_password(self):
        v_UserName = self._raw_input("UserName:")
        v_UserName = '!$$$' + base64.b64encode(v_UserName)
        username = self.doc.createElement('UserName')
        username_text = self.doc.createTextNode(v_UserName)
        username.appendChild(username_text)
        self.storage.appendChild(username)
        v_UserPassword = getpass.getpass("UserPassword:")
        v_UserPassword_repeat = getpass.getpass("RepeatUserPassword:")
        while v_UserPassword != v_UserPassword_repeat:
            msg = 'Input password not consist, please enter again!'
            self.print_message(msg)
            v_UserPassword = getpass.getpass("UserPassword:")
            v_UserPassword_repeat = getpass.getpass("RepeatUserPassword:")

        v_UserPassword = '!$$$' + base64.b64encode(v_UserPassword)
        userpassword = self.doc.createElement('UserPassword')
        userpassword_text = self.doc.createTextNode(v_UserPassword)
        userpassword.appendChild(userpassword_text)
        self.storage.appendChild(userpassword)

    def create_luntype(self, text_LUNType):
        self.lun = self.doc.createElement('LUN')
        self.config.appendChild(self.lun)

        LUNType = self.doc.createElement('LUNType')
        LUNType_text = self.doc.createTextNode(self.LUNType)
        LUNType.appendChild(LUNType_text)
        self.lun.appendChild(LUNType)

        if self.Product == 'T':
            StripUnitSize = self.doc.createElement('StripUnitSize')
            StripUnitSize_text = self.doc.createTextNode('64')
            StripUnitSize.appendChild(StripUnitSize_text)
            self.lun.appendChild(StripUnitSize)

        WriteType = self.doc.createElement('WriteType')
        WriteType_text = self.doc.createTextNode('1')
        WriteType.appendChild(WriteType_text)
        self.lun.appendChild(WriteType)

        prefetch = self.doc.createElement('Prefetch')
        prefetch.setAttribute('Type', '3')
        prefetch.setAttribute('Value', '0')
        self.lun.appendChild(prefetch)

    def create_StoragePool(self, text_StoragePool):
        if self.model == '1':
            v_StoragePool = self._raw_input(text_StoragePool)
            if (not v_StoragePool) and (self.StoragePool):
                v_StoragePool = self.StoragePool
            if self.Product == 'T':
                StoragePool = self.doc.createElement('StoragePool')
                StoragePool.setAttribute('Name', v_StoragePool)
                self.lun.appendChild(StoragePool)
            else:
                StoragePool = self.doc.createElement('StoragePool')
                StoragePool_text = self.doc.createTextNode(v_StoragePool)
                StoragePool.appendChild(StoragePool_text)
                self.lun.appendChild(StoragePool)
        if self.model == '2':
            self.filesys = self.doc.createElement('Filesystem')
            self.config.appendChild(self.filesys)
            v_StoragePool = self._raw_input(text_StoragePool)
            if (not v_StoragePool) and (self.StoragePool):
                v_StoragePool = self.StoragePool
            StoragePool = self.doc.createElement('StoragePool')
            StoragePool_text = self.doc.createTextNode(v_StoragePool)
            StoragePool.appendChild(StoragePool_text)
            self.filesys.appendChild(StoragePool)

    def create_logic_ip(self, LogicalPortIP):
        self.logic_ip = self._raw_input(LogicalPortIP)
        Logic_ip = self.doc.createElement('LogicalPortIP')
        Logic_ip_text = self.doc.createTextNode(self.logic_ip)
        Logic_ip.appendChild(Logic_ip_text)
        self.storage.appendChild(Logic_ip)

    def create_iscsi(self, text_DefaultTargetIP):
        if self.Protocol == 'iSCSI':
            iscsi = self.doc.createElement('iSCSI')
            self.config.appendChild(iscsi)
            v_DefaultTargetIP = self._raw_input(text_DefaultTargetIP)
            if (not v_DefaultTargetIP) and (self.DefaultTargetIP):
                v_DefaultTargetIP = self.DefaultTargetIP
            defaulttargetip = self.doc.createElement('DefaultTargetIP')
            defaulttargetip_text = self.doc.createTextNode(v_DefaultTargetIP)
            defaulttargetip.appendChild(defaulttargetip_text)
            iscsi.appendChild(defaulttargetip)

            feature_num = None
            while feature_num != 3:
                feature_num = self._raw_input(CINDER_XML_EXTRA_FEATURE)
                feature_num = int(feature_num)
                if feature_num and feature_num in range(1, 4):
                    if feature_num == 2:
                        ini = self._raw_input('Initiator Name:')
                        user = self._raw_input('CHAP Username:')
                        password = getpass.getpass('CHAP password:')
                        if user and password:
                            CHAPinfo = user + ';' + password
                        initiator = self.doc.createElement('Initiator')
                        initiator.setAttribute('Name', ini)
                        initiator.setAttribute('CHAPinfo', CHAPinfo)
                        initiator.setAttribute('ALUA', '1')
                        iscsi.appendChild(initiator)
                    if feature_num == 1 and self.Product != 'T':
                        ini = self._raw_input('Initiator Name:')
                        TargetPortGroup = self._raw_input('PortGroup Name:')
                        initiator = self.doc.createElement('Initiator')
                        initiator.setAttribute('Name', ini)
                        initiator.setAttribute('TargetPortGroup',
                                               TargetPortGroup)
                        iscsi.appendChild(initiator)
                    if feature_num == 1 and self.Product == 'T':
                        msg = 'T series not support Iscsi multipath!'
                        self.print_message(msg)
            if feature_num == 3:
                initiator = self.doc.createElement('Initiator')
                initiator.setAttribute('Name', 'xxxxxx')
                initiator.setAttribute('TargetIP', '192.168.100.2')
                iscsi.appendChild(initiator)

        else:
            iscsi = self.doc.createElement('iSCSI')
            self.config.appendChild(iscsi)
            defaulttargetip = self.doc.createElement('DefaultTargetIP')
            defaulttargetip_text = self.doc.createTextNode('')
            defaulttargetip.appendChild(defaulttargetip_text)
            iscsi.appendChild(defaulttargetip)

    def create_host(self, text_HostIP, text_OSType):
        Host = self.doc.createElement('Host')
        Host.setAttribute('HostIP', self.HostIP)
        Host.setAttribute('OSType', self.OSType)
        self.config.appendChild(Host)

    def config_hypermetro(self, conf):
        san_user = self._raw_input('Hypermetro Device UserName:')
        san_password = getpass.getpass("Hypermetro Device UserPassword:")
        san_password_repeat = getpass.getpass("RepeatUserPassword:")

        while san_password != san_password_repeat:
            msg = 'Input password not consist, please enter again!'
            self.print_message(msg)
            san_password = getpass.getpass("Hypermetro Device UserPassword:")
            san_password_repeat = getpass.getpass("RepeatUserPassword:")

        storage_ip = self._raw_input("Storage IP:")

        if self.Product == '18000':
            if storage_ip:
                san_address = 'https://' + storage_ip + '/deviceManager/rest/'
        if self.Product == 'V3':
            if storage_ip:
                san_address = 'https://' + storage_ip + \
                    ':8088/deviceManager/rest/'

        storage_pool = self._raw_input("StoragePoolName:")
        iSCSI_IP = self._raw_input("iSCSI IP:")
        metro_domain = self._raw_input("Metro domain name:")

        hypermetro_device = 'storage_pool:' + storage_pool + \
            ',san_address:' + san_address + ',san_user:' + san_user + \
            ',san_password:' + san_password + ',iscsi_default_target_ip:' + \
            iSCSI_IP + ',metro_domain:' + metro_domain
        conf.set(self.backend_name, "hypermetro_device", hypermetro_device)

    def config_replication(self, conf):
        san_user = self._raw_input('Replication Device UserName:')
        san_password = getpass.getpass("Replication Device UserPassword:")
        san_password_repeat = getpass.getpass("RepeatUserPassword:")

        while san_password != san_password_repeat:
            msg = 'Input password not consist, please enter again!'
            self.print_message(msg)
            san_password = getpass.getpass("Replication Device UserPassword:")
            san_password_repeat = getpass.getpass("RepeatUserPassword:")

        backend_id = self._raw_input("Backend_id:")
        storage_ip = self._raw_input("Storage IP:")

        if self.Product == '18000':
            if storage_ip:
                san_address = 'https://' + storage_ip + \
                    '/deviceManager/rest/'
        if self.Product == 'V3':
            if storage_ip:
                san_address = 'https://' + storage_ip + \
                    ':8088/deviceManager/rest/'

        storage_pool = self._raw_input("StoragePoolName:")
        iSCSI_IP = self._raw_input("iSCSI IP:")

        replication_device = 'backend_id:' + backend_id + ',storage_pool:' + \
            storage_pool + ',san_address:' + san_address + ',san_user:' + \
            san_user + ',san_password:' + san_password + \
            ',iscsi_default_target_ip:' + iSCSI_IP
        conf.set(self.backend_name, "replication_device", replication_device)

    def config_fc_auto_zoning(self, conf):
        conf.set("DEFAULT", 'zoning_mode', 'fabric')
        fc_fabric_names = self._raw_input('FC fabric name:')
        if fc_fabric_names not in conf.sections():
            conf.add_section(fc_fabric_names)

        fc_fabric_user = self._raw_input('Switch UserName:')
        fc_fabric_password = getpass.getpass("Switch UserPassword:")
        fc_fabric_password_repeat = getpass.getpass("RepeatUserPassword:")

        while fc_fabric_password != fc_fabric_password_repeat:
            msg = 'Input password not consist, please enter again!'
            self.print_message(msg)
            fc_fabric_password = getpass.getpass("Switch UserPassword:")
            fc_fabric_password_repeat = getpass.getpass("RepeatUserPassword:")

        fc_fabric_port = self._raw_input("Switch port:")
        fc_fabric_address = self._raw_input("Switch IP:")
        principal_switch_wwn = self._raw_input("Switch WWN:")

        conf.set(fc_fabric_names, "fc_fabric_address", fc_fabric_address)
        conf.set(fc_fabric_names, "fc_fabric_password", fc_fabric_password)
        conf.set(fc_fabric_names, "fc_fabric_port", fc_fabric_port)
        conf.set(fc_fabric_names, "fc_fabric_user", fc_fabric_user)
        conf.set(fc_fabric_names, "principal_switch_wwn", principal_switch_wwn)
        conf.set(fc_fabric_names, "zone_activate", 'True')

        if "fc-zone-manager" not in conf.sections():
            conf.add_section("fc-zone-manager")

        brcd_sb_connector = ("cinder.zonemanager.drivers.brocade."
                             "brcd_fc_zone_client_cli.BrcdFCZoneClientCLI")
        fc_san_lookup_service = ("cinder.zonemanager.drivers.brocade.brcd_fc_"
                                 "san_lookup_service.BrcdFCSanLookupService")
        zone_driver = ("cinder.zonemanager.drivers.brocade."
                       "brcd_fc_zone_driver.BrcdFCZoneDriver")
        conf.set("fc-zone-manager", "fc_fabric_names", fc_fabric_names)
        conf.set("fc-zone-manager", "zoning_policy", "initiator")
        conf.set("fc-zone-manager", "brcd_sb_connector", brcd_sb_connector)
        conf.set("fc-zone-manager", "fc_san_lookup_service",
                 fc_san_lookup_service)
        conf.set("fc-zone-manager", "zone_driver", zone_driver)

    def config_backup_snapshot(self, conf):
        conf.set("DEFAULT", 'backup_use_same_host', 'True')
        conf.set(self.backend_name, 'backup_use_temp_snapshot', 'True')

    def build_cinder_conf(self):
        cf = ConfigParser.ConfigParser()
        cf.read(self.conf_name)
        items = cf.defaults()
        for item in dict(items):
            if item in Cinder_Conf_Table:
                cf.remove_option("DEFAULT", item)
        if self.backend_name not in cf.sections():
            cf.add_section(self.backend_name)
        driver_pre = 'cinder.volume.drivers.huawei.'
        if self.Product in ['V3', '18000']:
            if self.Protocol == 'FC':
                cf.set(self.backend_name, Cinder_Conf_Table[0],
                       driver_pre + 'huawei_driver.HuaweiFCDriver')
            if self.Protocol == 'iSCSI':
                cf.set(self.backend_name, Cinder_Conf_Table[0],
                       driver_pre + 'huawei_driver.HuaweiISCSIDriver')

        if self.Product == 'T':
            if self.Protocol == 'FC':
                cf.set(self.backend_name, Cinder_Conf_Table[0],
                       driver_pre + 'huawei_t.HuaweiTFCDriver')
            if self.Protocol == 'iSCSI':
                cf.set(self.backend_name, Cinder_Conf_Table[0],
                       driver_pre + 'huawei_t.HuaweiTISCSIDriver')
        cf.set(self.backend_name, Cinder_Conf_Table[1], self.xml_file_name)
        cf.set(self.backend_name, Cinder_Conf_Table[2], self.backend_name)
        old_backend = cf.get("DEFAULT", 'enabled_backends')
        new_backend = str(old_backend) + ',' + self.backend_name
        cf.set("DEFAULT", 'enabled_backends', new_backend)
        if self.Product in ['V3', '18000']:
            feature_num = None
            while feature_num != 5:
                feature_num = self._raw_input(CINDER_CONF_EXTRA_FEATURE)
                feature_num = int(feature_num)
                if feature_num and feature_num in range(1, 6):
                    if feature_num == 1:
                        self.config_replication(cf)
                    if feature_num == 2:
                        self.config_hypermetro(cf)
                    if feature_num == 3 and self.Protocol == 'FC':
                        self.config_fc_auto_zoning(cf)
                    if feature_num == 3 and self.Protocol != 'FC':
                        msg = 'Iscsi Protocol not support fc auto zoning!'
                        self.print_message(msg)

                    if feature_num == 4:
                        self.config_backup_snapshot(cf)
                        msg = 'Config Successfully!'
                        self.print_message(msg)

        cf.write(open(self.conf_name, "wb"))

    def build_manila_conf(self):
        cf = ConfigParser.ConfigParser()
        cf.read(self.conf_name)
        items = cf.defaults()
        for item in dict(items):
            if item in Manila_Conf_Table:
                cf.remove_option("DEFAULT", item)
        if self.backend_name not in cf.sections():
            cf.add_section(self.backend_name)
        driver = 'manila.share.drivers.huawei.huawei_nas.HuaweiNasDriver'
        cf.set(self.backend_name, Manila_Conf_Table[0], driver)
        cf.set(self.backend_name, Manila_Conf_Table[1], self.xml_file_name)
        cf.set(self.backend_name, Manila_Conf_Table[2], self.backend_name)
        cf.set(self.backend_name, Manila_Conf_Table[3], 'False')
        old_backend = cf.get("DEFAULT", 'enabled_share_backends')
        new_backend = str(old_backend) + ',' + self.backend_name
        cf.set("DEFAULT", 'enabled_share_backends', new_backend)
        feature_num = None
        while feature_num != 2:
            feature_num = self._raw_input(MANILA_EXTRA_FEATURE)
            feature_num = int(feature_num)
            if feature_num and feature_num in range(1, 3):
                if feature_num == 1:
                    cf.set(self.backend_name, Manila_Conf_Table[3], 'True')
                    msg = 'Config Successfully!'
                    self.print_message(msg)

        cf.write(open(self.conf_name, "wb"))

    @staticmethod
    def print_message(message):
        if not message:
            return
        count = len(message)
        print ('-' * count + '\n' + message + '\n' + '-' * count + '\n')  # noqa

    @staticmethod
    def _raw_input(text):
        result = raw_input(text).strip()
        return result

    @staticmethod
    def validate_backend_name(backend_name):
        valid = re.match('^[\w-]+$', backend_name)
        return True if valid else False

    @staticmethod
    def change_conf_file_owner(conf_file, xml_file):
        user = os.lstat(conf_file).st_uid
        user_group = os.lstat(conf_file).st_gid
        os.chown(xml_file, user, user_group)


if __name__ == '__main__':
    ConfFile = CreateConfFile()
    try:
        ConfFile.create_xmlfile()
    except Exception:
        msg = 'Input is invalid, please retry!'
        ConfFile.print_message(msg)