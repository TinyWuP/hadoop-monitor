#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import argparse
import subprocess
import traceback
import logging
import requests


class Job(object):

    def __init__(self, args):
        self.args = args

    def run(self):

        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            level=logging.INFO, stream=sys.stderr)

        try:
            getattr(self, 'collect_%s' % self.args.type)()
        except Exception:
            traceback.print_exc()

    def collect_hregionserver(self):
        hbase_list = []
        hbase_dir = {}
        r = requests.get('http://%s:%d/master-status' % (self.args.hmaster_host, self.args.hmaster_port))
        regionserver = re.search('<tr><td><a href="//(.*)/">', r.text)
        if regionserver:
            host_port = regionserver.group(1).split(':')
            regionserver_host = host_port[0]
            regionserver_port = host_port[1]
        r = requests.get('http://%s:%s/rs-status' % (regionserver_host, regionserver_port))
        se = re.sub('<[^>]*>', '|', r.text)
        hbase_metrics = re.search('\|\|Metrics\|\|(.*)\|\|RegionServer Metrics;', se)
        if hbase_metrics:
            rs_metrics = hbase_metrics.group(1)
        else:
            print "not found"
        hbase_list = rs_metrics.split(', ')
        for i in hbase_list:
            metrics = i.split('=')
            if metrics:
                metrics_key = metrics[0]
                metrics_value = metrics[1]
                hbase_dir[metrics_key] = metrics_value
        print hbase_dir
        self.send_result(hbase_dir)
        
    def collect_hmaster(self):
        hbase_dir = {}
        r = requests.get('http://%s:%d/master-status' % (self.args.hmaster_host, self.args.hmaster_port))
        output = r.text
        print output
        se = re.sub('<[^>]*>', '|', output)
        hbase_version = re.search('\|\|HBase Version\|\|([0-9\.]+),', se).group(1)
        hadoop_version = re.search('\|\|Hadoop Version\|\|([0-9\.]+),', se).group(1)
        hbase_root_directory = re.search('\|\|HBase Root Directory\|\|(.*)\|\|Location', se).group(1)
        zookeeper_quorum = re.search('\|\|Zookeeper Quorum\|\|(.*)\|\|Addresses', se).group(1)
        load_average = re.search('\|\|Load average\|\|([\d]+)\|\|Average', se).group(1)
        coprocessors = re.search('Coprocessors\|\|\[(.*)\]\|\|', se)
        if coprocessors:
            hbase_dir['coprocessors'] = coprocessors
        else:
            print "not found"
        regionserver_num = re.search('\|\|Total: \|\|servers: (\d+)\|', se).group(1)
        table_num = re.search(r'(\d*)\s*table\(s\) in set', se).group(1)
        print table_num
        hbase_dir['hbase-version'] = hbase_version
        hbase_dir['hadoop-version'] = hadoop_version
        hbase_dir['hbase-root-directory'] = hbase_root_directory
        hbase_dir['zookeeper-quorum'] = zookeeper_quorum
        hbase_dir['load-average'] = load_average
        hbase_dir['regionserver-num'] = regionserver_num
        hbase_dir['table-num'] = table_num
        print hbase_dir
        self.send_result(hbase_dir)

    def send_result(self, result):

        result = self.format_result(result)
        print "*********************************"
        print result

        logging.info('Result:\n%s' % result)

        cmd = ['zabbix_sender']
        cmd.extend(['-c', '%s/etc/zabbix/zabbix_agentd.conf' % self.args.zabbix_home])
        cmd.extend(['-s', self.args.host])
        cmd.extend(['-i', '-'])

        logging.info('Command: %s' % ' '.join(str(s) for s in cmd))
        p = subprocess.Popen((str(s) for s in cmd),
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        logging.info('Output:\n%s' % p.communicate(result)[0])

    def format_result(self, result):
        lines = []
        for k, v in result.iteritems():
            lines.append('- hbase.%s.%s %s' % (self.args.type, k, v))
        return '\n'.join(lines)

    def regulate_size(self, size):

        try:
            size, unit = size.split()
            size = float(size)
        except ValueError:
            return 0

        if unit == 'KB':
            size = size * 1024
        elif unit == 'MB':
            size = size * 1024 ** 2
        elif unit == 'GB':
            size = size * 1024 ** 3
        elif unit == 'TB':
            size = size * 1024 ** 4
        elif unit == 'PB':
            size = size * 1024 ** 5

        return int(round(size))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='HBase metrics collector for Zabbix.')
    parser.add_argument('-t', '--type', required=True, help='collector type',
                        choices=['hmaster', 'hregionserver'])
    parser.add_argument('--hmaster-host', default='172.24.20.161')
    parser.add_argument('--hmaster-port', type=int, default=60010)
#    parser.add_argument('--hregionserver-host', default='172.24.20.161')
#    parser.add_argument('--hregionserver-port', type=int, default=55565)
    parser.add_argument('-z', '--zabbix-home', default='')
    parser.add_argument('-s', '--host', required=True, help='hostname recognized by zabbix')
    args = parser.parse_args()
    Job(args).run()
