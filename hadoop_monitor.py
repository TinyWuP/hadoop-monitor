#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
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

    def collect_namenode(self):

        r = requests.get('http://%s:%d/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo' % (self.args.namenode_host, self.args.namenode_port))
        result = {}
        output = r.json()
        result['file_count'] = output["beans"][0]["TotalFiles"]
        result['block_count'] = output["beans"][0]["TotalBlocks"]
        result['dfs_capacity'] = output["beans"][0]["Total"]
        result['dfs_used'] = output["beans"][0]["Used"]
        result['dfs_used_other'] = output["beans"][0]["NonDfsUsedSpace"]
        result['dfs_remaining'] = output["beans"][0]["Free"]
        result['node_decom'] = len(output["beans"][0]["DecomNodes"])
        # result['block_under'] = dfsmap['Number of Under-Replicated Blocks']
        print result

        self.send_result(result)

    def collect_datanode(self):
        r = requests.get('http://%s:%d/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo' % (self.args.namenode_host, self.args.namenode_port))
        result = {}
        output = r.json()
        result['block_count'] = output["beans"][0]["TotalBlocks"]
        result['dfs_capacity'] = output["beans"][0]["Total"]
        result['dfs_used'] = output["beans"][0]["Used"]
        result['dfs_remaining'] = output["beans"][0]["Free"]
        result['dfs_used_other'] = output["beans"][0]["NonDfsUsedSpace"]
        print result

        self.send_result(result)

    def collect_resourcemanager(self):
        r = requests.get('http://%s:%d/ws/v1/cluster' % (self.args.resourcemanager_host, self.args.resourcemanager_port))
        result = {}
        output = r.json()
        result['id'] = output["clusterInfo"]["id"]
        result['startedOn'] = output["clusterInfo"]["startedOn"]
        result['state'] = output["clusterInfo"]["state"]
        result['haState'] = output["clusterInfo"]["haState"]

        r = requests.get('http://%s:%d/ws/v1/cluster/metrics' % (self.args.resourcemanager_host, self.args.resourcemanager_port))
        output = r.json()
        result['appsSubmitted'] = output["clusterMetrics"]["appsSubmitted"]
        result['appsCompleted'] = output["clusterMetrics"]["appsCompleted"]
        result['appsPending'] = output["clusterMetrics"]["appsPending"]
        result['appsRunning'] = output["clusterMetrics"]["appsRunning"]
        result['appsFailed'] = output["clusterMetrics"]["appsFailed"]
        result['appsKilled'] = output["clusterMetrics"]["appsKilled"]
        result['reservedMB'] = output["clusterMetrics"]["reservedMB"]
        result['availableMB'] = output["clusterMetrics"]["availableMB"]
        result['allocatedMB'] = output["clusterMetrics"]["allocatedMB"]
        result['reservedVirtualCores'] = output["clusterMetrics"]["reservedVirtualCores"]
        result['availableVirtualCores'] = output["clusterMetrics"]["availableVirtualCores"]
        result['allocatedVirtualCores'] = output["clusterMetrics"]["allocatedVirtualCores"]
        result['containersAllocated'] = output["clusterMetrics"]["containersAllocated"]
        result['containersReserved'] = output["clusterMetrics"]["containersReserved"]
        result['containersPending'] = output["clusterMetrics"]["containersPending"]
        result['totalMB'] = output["clusterMetrics"]["totalMB"]
        result['totalVirtualCores'] = output["clusterMetrics"]["totalVirtualCores"]
        result['totalNodes'] = output["clusterMetrics"]["totalNodes"]
        result['lostNodes'] = output["clusterMetrics"]["lostNodes"]
        result['unhealthyNodes'] = output["clusterMetrics"]["unhealthyNodes"]
        result['decommissionedNodes'] = output["clusterMetrics"]["decommissionedNodes"]
        result['rebootedNodes'] = output["clusterMetrics"]["rebootedNodes"]
        result['activeNodes'] = output["clusterMetrics"]["activeNodes"]

        r = requests.get('http://%s:%d/ws/v1/cluster/scheduler' % (self.args.resourcemanager_host, self.args.resourcemanager_port))
        output = r.json()
        schedulerInfo = output["scheduler"]["schedulerInfo"]
        queue = schedulerInfo["queues"]["queue"][0]
        result['numActiveApplications'] = queue["numActiveApplications"]
        result['numPendingApplications'] = queue["numPendingApplications"]
        result['numContainers'] = queue["numContainers"]
        result['maxApplications'] = queue['maxApplications']
        result['maxApplicationsPerUser'] = queue['maxApplicationsPerUser']
        result['maxActiveApplications'] = queue['maxActiveApplications']
        result['maxActiveApplicationsPerUser'] = queue['maxActiveApplicationsPerUser']
        result['userLimit'] = queue['userLimit']
        result['users'] = queue['users']
        result['userLimitFactor'] = queue['userLimitFactor']

#         r = requests.get('http://%s:%d/ws/v1/cluster/apps' % (self.args.resourcemanager_host, self.args.resourcemanager_port))
#         output = r.json()
#         app_list = output["apps"]["app"]
#         for i in app_list:
        print result

        self.send_result(result)

    def collect_nodemanager(self):
        r = requests.get('http://%s:%d/ws/v1/node/info' % (self.args.nodemanager_host, self.args.nodemanager_port))
        result = {}
        output = r.json()
        nodeInfo = output["nodeInfo"]
        result['totalVmemAllocatedContainersMB'] = nodeInfo["totalVmemAllocatedContainersMB"]
        result['totalVmemAllocatedContainersMB'] = nodeInfo["totalVmemAllocatedContainersMB"]
        result['totalVCoresAllocatedContainers'] = nodeInfo["totalVCoresAllocatedContainers"]
        result['nodeHealthy'] = nodeInfo["nodeHealthy"]

        self.send_result(result)

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
            lines.append('- hadoop.%s.%s %s' % (self.args.type, k, v))
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

    parser = argparse.ArgumentParser(description='Hadoop metrics collector for Zabbix.')
    parser.add_argument('-t', '--type', required=True, help='collector type',
                        choices=['namenode', 'datanode', 'resourcemanager', 'nodemanager'])
    parser.add_argument('--namenode-host', default='172.24.20.161')
    parser.add_argument('--namenode-port', type=int, default=50070)
    parser.add_argument('--resourcemanager-host', default='172.24.20.161')
    parser.add_argument('--resourcemanager-port', type=int, default=8088)
    parser.add_argument('--nodemanager-host', default='172.24.20.161')
    parser.add_argument('--nodemanager-port', type=int, default=8042)
    parser.add_argument('-z', '--zabbix-home', default='')
    parser.add_argument('-s', '--host', required=True, help='hostname recognized by zabbix')
    args = parser.parse_args()
    Job(args).run()
