#!/usr/bin/env python
# -*- coding: utf-8 -*-
# https://www.zabbix.com/documentation/3.4/zh/manual/api
# https://www.iyunv.com/thread-665618-1-1.html

import json
import time
import re
import traceback
from sql.utils.api import HttpRequests

http_request = HttpRequests()
zabbix_user, zabbix_password = "yukai", "redhat"
zabbix_url = "http://zabbix.weidai.work/zabbix/api_jsonrpc.php"


def get_access_token():
    data = {
        "jsonrpc": "2.0",
        "method": "user.login",
        "params": {
            "user": zabbix_user,
            "password": zabbix_password
        },
        "id": 1,
        "auth": None
    }
    status, ret = http_request.post(zabbix_url, data)
    if status is True:
        s = json.loads(ret)
        return s["result"]
    print(ret)
    return


def get_host_ids(token, ip_list):
    data = {
        "jsonrpc": "2.0",
        "method": "host.get",
        "params": {
            "output": ["hostid"],
            "filter": {
                "ip": ip_list
            }
        },
        "auth": token,
        "id": 2
    }
    status, ret = http_request.post(zabbix_url, data)
    if status is True:
        s = json.loads(ret)
        # [{'hostid': '11103'}, {'hostid': '10639'}]
        return [r["hostid"] for r in s["result"]]
    print(ret)
    return


def get_monitor_item_ids(token, host_id_list, key_list):
    # https://www.zabbix.com/documentation/3.4/zh/manual/api/reference/item/get
    data = {
        "jsonrpc": "2.0",
        "method": "item.get",
        "params": {
            "output": "extend",
            "hostids": host_id_list,
            "search": {
                "key_": key_list
            },
            "sortfield": "name"
        },
        "auth": token,
        "id": 3
    }
    status, ret = http_request.post(zabbix_url, data)
    if status is True:
        s = json.loads(ret)
        ret = dict()
        for r in s["result"]:
            if r["hostid"] in ret:
                ret[r["hostid"]].append(r["itemid"])
            else:
                ret[r["hostid"]] = [r["itemid"]]
        return ret
    print(ret)
    return


def get_history_data(token, host_id, item_id_list, time_start=None, time_end=None):
    # https://www.zabbix.com/documentation/3.4/zh/manual/api/reference/history/get
    if time_start and time_end:
        data = {
            "jsonrpc": "2.0",
            "method": "history.get",
            "params": {
                "output": "extend",
                "hostids": host_id,
                "itemids": item_id_list,
                "sortfield": "clock",
                "sortorder": "DESC",
                "time_from": time_start,
                "time_till": time_end
            },
            "auth": token,
            "id": 4
        }
    else:
        # 获取最后一个值
        data = {
            "jsonrpc": "2.0",
            "method": "history.get",
            "params": {
                "output": "extend",
                "hostids": host_id,
                "itemids": item_id_list,
                "sortfield": "clock",
                "sortorder": "DESC",
                "limit": 1
            },
            "auth": token,
            "id": 4
        }

    status, ret = http_request.post(zabbix_url, data)
    if status is True:
        s = json.loads(ret)
        # '{"jsonrpc":"2.0","result":[{"itemid":"78248","clock":"1560505088","value":"1","ns":"159446572"},
        # {"itemid":"78248","clock":"1560505028","value":"1","ns":"865098001"},
        # {"itemid":"78248","clock":"1560504968","value":"1","ns":"318736807"}],"id":1}'
        return s["result"]
    print(ret)
    return

key_list = ["system.cpu.util[,user,avg15]", "vm.memory.size[used]", "vm.memory.size[available]"]
