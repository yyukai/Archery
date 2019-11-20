#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import re
import traceback
import redis
from sql.models import Users, Config
from sql.utils.api import HttpRequests, async_func
from common.config import SysConfig

# 是否每次登陆都要去钉钉获取一下 user_id。因为钉钉里的 user_id 是固定的，所以没必要配置成 True。
FORCE_UPDATE = False

http_request = HttpRequests()


def get_access_token():
    sys_conf = SysConfig().sys_config
    token = sys_conf.get('ding_access_token', '')
    expire_time = sys_conf.get('ding_expires_time', 0)
    now_time = int(time.time())
    if expire_time and (int(expire_time) - now_time) > 60:
        # 还没到超时时间
        return token
    else:
        # token 已过期
        app_key = sys_conf.get('ding_app_key')
        app_secret = sys_conf.get('ding_app_secret')
        url = "https://oapi.dingtalk.com/gettoken?appkey={0}&appsecret={1}".format(app_key, app_secret)
        status, ret = http_request.get(url)
        if status is True:
            # 钉钉推荐加锁更新token，这里暂时未实现
            # from django.db import transaction
            s = json.loads(ret)

            updated_values = {"item": "ding_access_token", "value": s["access_token"]}
            Config.objects.update_or_create(item="ding_access_token", defaults=updated_values)

            updated_values = {"item": "ding_expires_time", "value": str(int(now_time + s["expires_in"]))}
            Config.objects.update_or_create(item="ding_expires_time", defaults=updated_values)

            return s["access_token"]
        else:
            print(ret)
            return


def get_dept_list_id_fetch_child(token, parent_dept_id):
    ids = [int(parent_dept_id)]
    url = 'https://oapi.dingtalk.com/department/list_ids?id={0}&access_token={1}'.format(parent_dept_id, token)
    status, ret = http_request.get(url)
    if status is True:
        s = json.loads(ret)
        if s["errcode"] == 0:
            for dept_id in s["sub_dept_id_list"]:
                ids.extend(get_dept_list_id_fetch_child(token, dept_id))
    return ids


def get_ding_user_id(username):
    try:
        rs = redis.StrictRedis(host="127.0.0.1", port=6379, password="archerPass", db=15)
        ding_user_id = rs.execute_command('GET {}'.format(username.upper()))
        if ding_user_id:
            user = Users.objects.get(username=username)
            if user.ding_user_id != str(ding_user_id, encoding="utf8"):
                user.ding_user_id = str(ding_user_id, encoding="utf8")
                user.save(update_fields=['ding_user_id'])
    except Exception as e:
        traceback.print_exc()


@async_func
def get_ding_user_id1(username):
    """
    本公司使用工号（username）登陆archer，并且工号对应钉钉系统中字段 "jobnumber"。
    所以可根据钉钉中 jobnumber 查到该用户的 ding_user_id。
    """
    try:
        # archer 的用户名，对应钉钉系统中 jobnumber值。
        key = SysConfig().sys_config.get('ding_archer_username', '')

        user = Users.objects.get(username=username)
        # 非强制每次登陆查询 user_id，且archer中 ding_user_id 已存在
        if FORCE_UPDATE is False and user.ding_user_id is not None and user.ding_user_id != '':
            return
        token = get_access_token()
        """
        线下销售中心：部门ID：3031405
        线上销售中心：部门ID：87855807
        车消费金融事业部：42082913
        易起投事业部：19928483
        微易融事业部：36049564
        金融资信部：3281417
        销售管理部：22545014
        供应链事业部：66589427
        平台运营部：62935266
        客户体验部：88588108
        综合管理部：30030483
        业务管理部：62269218
        商务推广部：58131518
        市场品牌部：62134556
        研发中心：2925013
        贷后管理中心：63560181
        金融产品中心：58136494
        线上运营中心：15993333
        风险管理中心：3112102
        """
        ding_dept_ids = [2925013, 3031405, 87855807, 3112102, 15993333, 63560181, 58136494, 42082913, 19928483,
                         36049564, 3281417, 22545014, 66589427, 62935266, 88588108, 30030483, 62269218,
                         58131518, 62134556]
        for dept_id in ding_dept_ids:
            dept_id_list = get_dept_list_id_fetch_child(token, dept_id)
            for di in dept_id_list:
                url = 'https://oapi.dingtalk.com/user/list?access_token={0}&department_id={1}'.format(token, di)
                status, ret = http_request.get(url)
                if status is True:
                    # print('user_list_by_dept_id:', ret)
                    s = json.loads(ret)
                    if s["errcode"] == 0:
                        for u in s["userlist"]:
                            if re.match(u[key], username, re.I):
                                user = Users.objects.get(username=username)
                                user.ding_user_id = u["userid"]
                                user.save()
                                return
                    else:
                        print(ret)
                else:
                    print(ret)
    except Exception as e:
        traceback.print_exc()


def get_ding_user_id2(username):
    # 该方法只适用研发中心人员，所以废弃
    try:
        user = Users.objects.get(username=username)
        # 非强制每次登陆查询 user_id，且archer中 ding_user_id 已存在
        if FORCE_UPDATE is False and user.ding_user_id is not None and user.ding_user_id != '':
            return
        url = "http://magicbox.nw.weidai.com.cn/user/getUserByWorkNo?workNo=" + username
        status, ret = http_request.get(url)
        if status is True:
            s = json.loads(ret)
            if s["code"] == 0:
                user.ding_user_id = s["data"]["dingDingId"]
                user.save(update_fields=['ding_user_id'])
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

    @async_func
    def send_msg(self, ding_user_id, content):
        if self.app_id is None:
            return "No app id."
        data = {
            "touser": ding_user_id,
            "agentid": self.app_id,
            "msgtype": "text",
            "text": {
                "content": "{}".format(content)
            },
        }
        url = 'https://oapi.dingtalk.com/message/send?access_token=' + get_access_token()
        # print(url, data)
        json_request = HttpRequests()
        status, ret = json_request.post(url, data)
        if status is not True:
            print(u'请求失败：%s' % ret)
        else:
            print('success. ', ret)

    def send_msg_sync(self, ding_user_id, content):
        if self.app_id is None:
            return "No app id."
        data = {
            "touser": ding_user_id,
            "agentid": self.app_id,
            "msgtype": "text",
            "text": {
                "content": "{}".format(content)
            },
        }
        url = 'https://oapi.dingtalk.com/message/send?access_token=' + get_access_token()
        # print(url, data)
        json_request = HttpRequests()
        status, ret = json_request.post(url, data)
        if status is not True:
            print(u'请求失败：%s' % ret)
        else:
            print('success. ', ret)
