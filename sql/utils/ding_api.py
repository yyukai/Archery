#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import traceback
from django.db import transaction
from sql.models import Users, Config
from sql.utils.api import HttpRequests
from sql.utils.config import SysConfig


FORCE_UPDATE = False
http_request = HttpRequests()


def get_access_token():
    sys_conf = SysConfig().sys_config
    token = sys_conf.get('ding_access_token')
    expire_time = sys_conf.get('ding_expires_time')
    now_time = int(time.time())
    if expire_time and (int(expire_time) - now_time) > 60:
        # 还没到超时时间
        return token
    else:
        # token 已过期
        corp_id = sys_conf.get('ding_corp_id')
        corp_secret = sys_conf.get('ding_corp_secret')
        url = "https://oapi.dingtalk.com/gettoken?corpid={0}&corpsecret={1}".format(corp_id, corp_secret)
        status, ret = http_request.get(url)
        if status is True:
            s = json.loads(ret)
            print(s["access_token"], s["expires_in"])
            Config.objects.filter(item="ding_access_token").update(value=s["access_token"])
            Config.objects.filter(item="ding_expires_time").update(value=str(int(now_time + s["expires_in"])))
            return s["access_token"]
        else:
            print(ret)
            return


def get_dept_list_id_fetch_child(token, parent_dept_id):
    ids = [int(parent_dept_id)]
    url = 'https://oapi.dingtalk.com/department/list_ids?id={0}&access_token={1}'.format(parent_dept_id, token)
    print(url)
    status, ret = http_request.get(url)
    if status is True:
        s = json.loads(ret)
        if s["errcode"] == 0:
            for dept_id in s["sub_dept_id_list"]:
                ids.extend(get_dept_list_id_fetch_child(token, dept_id))
    return ids


def set_ding_userid(work_no):
    try:
        # 工号work_no对应钉钉jobnumber，查询user_id
        user = Users.objects.get(username=work_no)
        # 非强制查询user_id，且user_id已存在，则直接退出
        if FORCE_UPDATE is False and user.user_id != "":
            return
        root_id = 2925013
        token = get_access_token()
        dept_id_list = get_dept_list_id_fetch_child(token, root_id)
        for dept_id in dept_id_list:
            url = 'https://oapi.dingtalk.com/user/list?access_token={0}&department_id={1}'.format(token, dept_id)
            status, ret = http_request.get(url)
            if status is True:
                s = json.loads(ret)
                if s["errcode"] == 0:
                    for u in s["userlist"]:
                        if u["jobnumber"] == work_no:
                            user.user_id = u["userid"]
                            user.save()
                            return
                else:
                    print(ret)
            else:
                print(ret)
    except Exception as e:
        traceback.print_exc()


class DingSender(object):
    def __init__(self):
        self.app_id = SysConfig().sys_config.get('ding_agent_id', None)

    def send_msg(self, user_id, content):
        if self.app_id is None:
            return "No app id."
        data = {
            "touser": user_id,
            "agentid": self.app_id,
            "msgtype": "text",
            "text": {
                "content": "{}".format(content)
            },
        }
        url = 'https://oapi.dingtalk.com/message/send?access_token=' + get_access_token()
        json_request = HttpRequests()
        status, ret = json_request.post(url, data)
        if status is not True:
            print(u'请求失败：%s' % ret)
        else:
            print('success. ', ret)