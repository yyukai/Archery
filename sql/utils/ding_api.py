#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import traceback
from sql.models import Config, Users
from sql.utils.api import HttpRequests

FORCE_UPDATE = False
http_request = HttpRequests()


def get_access_token():
    token = Config.objects.get(item='ding_access_token').ding_access_token
    expire_time = Config.objects.get(item='ding_expires_time').ding_expires_time
    now_time = int(time.time())
    if expire_time and (expire_time - now_time) > 60:
        # 还没到超时时间
        return token
    else:
        # token 已过期
        corp_id = Config.objects.get(item='ding_corp_id').ding_corp_id
        corp_secret = Config.objects.get(item='ding_corp_secret').ding_corp_secret
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
    ids = list()
    url = 'https://oapi.dingtalk.com/department/list_ids?id={0}&access_token={1}'.format(parent_dept_id, token)
    status, ret = http_request.get(url)
    if status is True:
        s = json.loads(ret)
        if s["errcode"] == 0:
            for dept_id in s["sub_dept_id_list"]:
                ids.append(get_dept_list_id_fetch_child(dept_id))
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
