# Databricks notebook source
#!/bin/bash

/usr/bin/python - <<EOF
print "Start python init script..."
import urllib2
import json
import boto.ec2

instance = json.loads(urllib2.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document').read())
instance

SPARK_MASTER_HOSTNAME = urllib2.urlopen('http://169.254.169.254/latest/meta-data/public-hostname').read()
SPARK_MASTER_HOSTNAME

EIP_ADDRESS = "100.xx.xx.xxx"
print EIP_ADDRESS

conn = boto.ec2.connect_to_region(instance['region'])

address = conn.get_all_addresses(filters={'public_ip': EIP_ADDRESS})[0]
address.association_id

conn.associate_address(instance_id=instance[u'instanceId'], allocation_id=address.allocation_id)

print "Finish python init script."
EOF