#!/bin/bash

sudo /usr/bin/ln -s /host/sbin/multipath /sbin/multipath
sudo /usr/bin/ln -s /host/sbin/multipathd /sbin/multipathd
sudo /usr/bin/ln -s /host/usr/lib64/libmultipath.so.0 /usr/lib64/libmultipath.so.0
sudo /usr/bin/ln -s /host/usr/lib64/libmpathpersist.so.0 /usr/lib64/libmpathpersist.so.0
sudo /usr/bin/ln -s /host/usr/lib64/libmpathcmd.so.0 /usr/lib64/libmpathcmd.so.0

sudo /usr/bin/rpm -Uvh /block_driver_dir/libxslt-1.1.34-3.oe1.x86_64.rpm
sudo /usr/bin/rpm -Uvh /block_driver_dir/python2-lxml-4.5.2-3.oe1.x86_64.rpm
sudo /usr/bin/rpm -Uvh /block_driver_dir/sg3_utils-1.45-2.oe1.x86_64.rpm

sudo /usr/bin/cp -R /block_driver_dir/huawei /usr/lib/python2.7/site-packages/cinder/volume/drivers
