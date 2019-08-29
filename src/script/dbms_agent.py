#!/usr/bin/python2
# coding:utf-8
# https://www.cnblogs.com/yuyue2014/p/3679628.html


import subprocess
import socket
import re
import math
import json
import platform
import urllib2
import psutil
import sys
import time
from pprint import pprint
from datetime import datetime
reload(sys)
sys.setdefaultencoding("utf-8")


def bytes_convert(n, lst=None):
    if lst is None:
        lst = ['Bytes', 'K', 'M', 'G', 'TB', 'PB', 'EB']
    idx = int(math.floor(         # 舍弃小数点，取小
        math.log(n + 1, 1024)   # 求对数(对数：若 a**b = N 则 b 叫做以 a 为底 N 的对数)
    ))
    if idx >= len(lst):
        idx = len(lst) - 1
    return ('%.0f' + lst[idx]) % (n / math.pow(1024, idx))


def get_os_version():
    return platform.platform()


def get_hostname():
    return socket.gethostname()


def get_cpu_count():
    return psutil.cpu_count()


def get_cpu_used():
    return '%.1f' % psutil.cpu_percent(interval=3, percpu=False)


def get_memory():
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    return {
        'mem': bytes_convert(memory.total),
        'mem_used': '%.1f' % memory.percent,
        'swap': swap.total,
        'swap_used': '%.1f' % swap.percent,
    }


def get_load_avg():
    # 0.38 0.75 0.81 1/494 45052
    # 1/494: 分子是正在运行的进程数，分母是进程总数；
    # 45052: 最近运行的进程ID号
    with open("/proc/loadavg") as f:
        con = f.read().split()
    return '/'.join(con[:3])


def get_ip():
    try:
        csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        csock.connect(('8.8.8.8', 80))
        (addr, _) = csock.getsockname()
        csock.close()
        return addr
    except socket.error:
        return "127.0.0.1"


def get_disk_used():
    ret = list()
    for mnt in psutil.disk_partitions(all=False):
        if mnt.fstype == "":
            continue
        disk_used = psutil.disk_usage(mnt.mountpoint)
        ret.append("%s：%s/%.1f%%" % (mnt.mountpoint, bytes_convert(disk_used.total), disk_used.percent))
    return ret


def get_net_io():
    net_io_1 = psutil.net_io_counters(pernic=False, nowrap=True)
    time.sleep(3)
    net_io_2 = psutil.net_io_counters(pernic=False, nowrap=True)
    return "S: {0}/s, R: {1}/s".format(bytes_convert(net_io_2.bytes_sent - net_io_1.bytes_sent),
                                       bytes_convert(net_io_2.bytes_recv - net_io_1.bytes_recv))


def get_device_io():
    ret = dict()
    disk_io_1 = dict()
    for dev, obj in psutil.disk_io_counters(perdisk=True).items():
        disk_io_1[dev] = [obj.read_bytes, obj.write_bytes]

    time.sleep(3)
    for dev, obj in psutil.disk_io_counters(perdisk=True).items():
        (io_1_read, io_1_write) = disk_io_1[dev]
        ret[dev] = "W: {0}/s, R: {1}/s".format(bytes_convert(int((obj.write_bytes - io_1_write) / 3)),
                                               bytes_convert(int((obj.read_bytes - io_1_read) / 3)))
    return ret


def get_device_mount():
    ret = dict()
    for mnt in psutil.disk_partitions(all=False):
        dev_path = mnt.device
        cmd = "sudo lvdisplay %s |grep 'VG Name' |awk '{print $NF}'" % dev_path
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = p.stdout.read().strip(), p.stderr.read().strip()
        if stdout != "":
            cmd = "sudo pvscan |awk '/%s/{print $2}'" % stdout
            r = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            dev_path = r.stdout.read().strip()
        ret[mnt.mountpoint] = dev_path.split("/")[-1]
    return ret


def get_mysql():
    ret = dict()
    device_mount = get_device_mount()
    device_io = get_device_io()
    cmd = "sudo ps -ef|grep -w mysqld|grep '\-\-datadir='|grep '\-\-port='"
    ports = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line1 in ports.stdout.readlines():
        base_path = re.findall(r'--basedir=(\S+)', line1, re.I)[0]
        conf_path = re.findall(r'--defaults-file=(\S+)', line1, re.I)
        data_path = re.findall(r'--datadir=(\S+)', line1, re.I)[0]
        err_log_path = re.findall(r'--log-error=(\S+)/\S+', line1, re.I)[0]
        port = re.findall(r'--port=([0-9]*)', line1, re.I)[0]
        socket_path = re.findall(r'--socket=(\S+)', line1, re.I)[0]
        ret[port], gs = dict(), dict()
        status_cmd = """/app/mysql/dist/bin/mysqladmin -uwddbms -pWDdbms0412\! --port={0} --socket={1} \
        extended-status -i 1 -c 2 -r 2>/dev/null \
        |grep -E 'Com|Threads|Key|Innodb|Questions|Uptime' |sed 's/|//g'""".format(port, socket_path)
        status = subprocess.Popen(status_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line2 in status.stdout.readlines():
            if len(line2.split()) != 2:
                continue
            (var, value) = line2.split()
            gs[var] = value if not value.isdigit() else int(value)
        if not gs:
            continue
        # Query数
        ret[port]['com_select'] = gs['Com_select']
        # TPS，每秒的事务量（commit与rollback的之和）
        ret[port]['tps'] = gs['Com_commit'] + gs['Com_rollback']
        # I/O
        ret[port]['io'] = (gs['Key_reads'] + gs['Key_writes']) * 2 + gs['Key_read_requests'] + \
                          gs['Innodb_data_reads'] + gs['Innodb_data_writes'] + \
                          gs['Innodb_dblwr_writes'] + gs['Innodb_log_writes']
        # QPS，每秒增删改查量
        ret[port]['qps'] = round(gs['Questions'] / gs['Uptime'], 2)
        # 线程总连接数
        ret[port]['threads_connected'] = gs['Threads_connected']
        # 活跃会话数
        ret[port]['threads_running'] = gs['Threads_running']
        # 慢查询累计
        # ret[port]['slow_queries'] = gs['Slow_queries']
        # 慢查询
        cmd = """
        /app/mysql/dist/bin/mysql -uwddbms -pWDdbms0412\! --port={0} --socket={1} -e \
        "select count(*) from information_schema.INNODB_TRX where trx_started<SUBDATE(now(),interval 3 second)\G" \
        2>/dev/null |tail -n1 |awk '{{print $NF}}'
        """.format(port, socket_path)
        r = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ret[port]['slow_queries'] = int(r.stdout.read().strip())

        cmd = "sudo df -P {0} |tail -n1 |awk '{{print $NF}}'".format(data_path)
        r = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        mount_point = r.stdout.read().strip()
        disk_used = psutil.disk_usage(mount_point)
        ret[port]["disk_used"] = "%s/%.1f%%" % (bytes_convert(disk_used.total), disk_used.percent)
        ret[port]["disk_io"] = device_io[device_mount[mount_point]]
        ret[port]["base_path"] = base_path
        ret[port]["conf_path"] = conf_path[0] if conf_path else "/etc/my.cnf"
        ret[port]["data_path"] = data_path
        ret[port]["err_log_path"] = ret[port]["slow_log_path"] = err_log_path
    return ret


def send(post_data):
    try:
        pprint(post_data)
        url = "http://dbms.weidai.com.cn/api/v1/db_agent/"
        # url = "http://192.168.21.241:9123/api/v1/db_agent/"
        req = urllib2.Request(url, json.dumps(post_data))
        req.add_header("Content-Type", "application/json")
        resp = urllib2.urlopen(req)
        print(resp.read())
    except urllib2.HTTPError as e:
        print(e)

if __name__ == "__main__":
    data = dict()
    data['os'] = get_os_version()
    data['hostname'] = get_hostname()
    data['cpu'] = get_cpu_count()
    data['cpu_used'] = get_cpu_used()
    data['memory'] = get_memory()
    data['load_avg'] = get_load_avg()
    data['disk_used'] = get_disk_used()
    data['net_io'] = get_net_io()
    data['ip'] = get_ip()
    data["mysql"] = get_mysql()
    data["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    send(data)
