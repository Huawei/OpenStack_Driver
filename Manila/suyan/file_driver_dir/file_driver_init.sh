#!/bin/bash

sudo /usr/bin/rpm -Uvh /file_driver_dir/libxslt-1.1.34-3.oe1.x86_64.rpm
sudo /usr/bin/rpm -Uvh /file_driver_dir/python2-lxml-4.5.2-3.oe1.x86_64.rpm

sudo /usr/bin/cp -R /file_driver_dir/huawei /usr/lib/python2.7/site-packages/manila/share/drivers
