#!/usr/bin/env python3
"""Run on the monitored Linux host; compare real exporter output with /proc."""
import argparse
import csv
import json
import os
import re
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

def metrics(url):
    with urllib.request.urlopen(url, timeout=20) as response:
        text = response.read().decode()
    result = {}
    for line in text.splitlines():
        match = re.match(r'^(\w+)(?:\{(.*)\})?\s+([^ ]+)', line)
        if match:
            labels = {key: json.loads(value) for key, value in re.findall(r'(\w+)=("(?:[^"\\]|\\.)*")', match[2] or '')}
            result.setdefault(match[1], []).append((labels, float(match[3])))
    return result

def net():
    return {name.strip(): (int(data.split()[0]), int(data.split()[8]))
            for line in Path('/proc/net/dev').read_text().splitlines()[2:]
            for name, data in [line.split(':', 1)]}

def disk():
    return {v[2]: (int(v[5])*512, int(v[9])*512) for line in Path('/proc/diskstats').read_text().splitlines() if len(v := line.split()) >= 14}

def idle():
    ticks = os.sysconf('SC_CLK_TCK')
    return {v[0][3:]: int(v[4])/ticks for line in Path('/proc/stat').read_text().splitlines() if (v := line.split()) and re.fullmatch(r'cpu\d+',v[0])}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--alloy', default='http://127.0.0.1:12345')
    parser.add_argument('--network-component', default='host_network')
    parser.add_argument('--gpu', action='store_true', help='also verify the existing Docker GPU probe')
    args = parser.parse_args()
    before_disk, before_cpu = disk(), idle()
    host = metrics(args.alloy+'/api/v0/component/prometheus.exporter.unix.host/metrics')
    after_disk, after_cpu = disk(), idle()
    mem = {v[0].rstrip(':'): int(v[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines() if len(v := line.split()) == 3}
    assert host['node_memory_MemTotal_bytes'][0][1] == mem['MemTotal'], 'memory total is not the host total'
    # Available memory can change during the short collector request.
    assert abs(host['node_memory_MemAvailable_bytes'][0][1]-mem['MemAvailable']) <= mem['MemTotal']*.01, 'available memory differs by more than 1% of host capacity'
    cores = {labels['cpu'] for labels, _ in host['node_cpu_seconds_total']}
    assert len(cores) == os.cpu_count(), 'CPU count mismatch'
    for labels, value in host['node_cpu_seconds_total']:
        if labels['mode'] == 'idle':
            cpu = labels['cpu']
            assert before_cpu[cpu]-.02 <= value <= after_cpu[cpu]+.02, 'CPU counter mismatch: '+cpu
    checked_disks = set()
    for index, metric in enumerate(['node_disk_read_bytes_total', 'node_disk_written_bytes_total']):
        for labels, value in host[metric]:
            name = labels['device']
            assert before_disk[name][index] <= value <= after_disk[name][index], 'disk byte counter mismatch: '+name
            checked_disks.add(name)
    before = net()
    network = metrics(args.alloy+'/api/v0/component/prometheus.exporter.unix.'+args.network_component+'/metrics')
    after = net()
    ignore = ('lo', 'veth', 'br-', 'docker', 'virbr')
    expected = {name for name in before if not name.startswith(ignore)}
    reported = {labels['device'] for labels, _ in network.get('node_network_receive_bytes_total', []) if not labels['device'].startswith(ignore)}
    assert reported == expected, f'host interfaces {sorted(expected)} != reported {sorted(reported)}'
    for index, metric in enumerate(['node_network_receive_bytes_total', 'node_network_transmit_bytes_total']):
        for labels, value in network[metric]:
            name = labels['device']
            if name in expected:
                assert before[name][index] <= value <= after[name][index], 'network counter mismatch: '+name
    filesystem = next(value for labels, value in host['node_filesystem_size_bytes'] if labels['mountpoint']=='/')
    fs = os.statvfs('/')
    assert filesystem == fs.f_blocks*fs.f_frsize, 'root filesystem capacity mismatch'
    result = {'host': socket.gethostname(), 'cpu_cores': len(cores), 'memory_total_bytes': mem['MemTotal'], 'memory_used_percent':100*(1-mem['MemAvailable']/mem['MemTotal']), 'root_filesystem_bytes':filesystem, 'disk_counters_checked':sorted(checked_disks), 'host_interfaces':sorted(reported)}
    if args.gpu:
        container = json.loads(subprocess.check_output(['docker','inspect','kb-observability-capability-probe'], text=True))[0]
        ip = container['NetworkSettings']['Networks']['mwe-observability']['IPAddress']
        probe = metrics('http://'+ip+':9477/metrics')
        assert probe['kb_probe_snapshot_stale'][0][1] == 0, 'GPU probe is stale'
        output = subprocess.check_output(['nvidia-smi','--query-gpu=index,uuid,memory.total,memory.used,utilization.gpu,temperature.gpu,power.draw','--format=csv,noheader,nounits'],text=True)
        actual = {row[1]: row for row in csv.reader(output.splitlines(), skipinitialspace=True)}
        gpus = []
        for labels, total in probe['kb_gpu_memory_total_bytes']:
            row = actual[labels['uuid']]
            assert labels['gpu'] == row[0] and total == float(row[2])*1024**2, 'GPU identity or capacity mismatch'
            sample_time = next(v for key,v in probe['kb_gpu_sample_timestamp_seconds'] if key['uuid']==labels['uuid'])
            age = time.time()-sample_time
            assert 0 <= age <= 90, 'GPU sample expired'
            gpus.append({'gpu':labels['gpu'],'total_bytes':total,'sample_age_seconds':round(age,2)})
        assert len(gpus) == len(actual)
        result['gpus'] = gpus
    print(json.dumps({'passed':True, **result}))

if __name__ == '__main__':
    main()
