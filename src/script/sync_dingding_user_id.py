#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import traceback
import requests
import redis

corp_id = "dingnooxqmszb5kzvyq1"
corp_secret = "KCa8kaXE7y-bZMxRQVhSzxOELKd_PArwp4ci2NTlFGgObwfjJsLxMmixnX0-EOcB"
rs = redis.StrictRedis(host="127.0.0.1", port=6379, password="archerPass", db=15)


def get_access_token():
    now_time = int(time.time())
    expire_time = rs.execute_command('TTL token')
    if expire_time and (int(expire_time) - now_time) > 60:
        # 还没到超时时间
        return rs.execute_command('GET token').decode()
    else:
        # token 已过期
        url = "https://oapi.dingtalk.com/gettoken?corpid={0}&corpsecret={1}".format(corp_id, corp_secret)
        resp = requests.get(url, timeout=3)

        ret = str(resp.content, encoding="utf8")
        s = json.loads(ret)
        rs.execute_command('SETEX token {} {}'.format(s["expires_in"], s["access_token"]))
        return s["access_token"]


class Ding(object):
    def __init__(self):
        self.token = get_access_token()
        self.key = "jobnumber"

    def get_dept_list_id_fetch_child(self, parent_dept_id):
        ids = [int(parent_dept_id)]
        url = 'https://oapi.dingtalk.com/department/list_ids?id={0}&access_token={1}'.format(parent_dept_id, self.token)
        resp = requests.get(url, timeout=3)
        ret = str(resp.content, encoding="utf8")
        s = json.loads(ret)
        if s["errcode"] == 0:
            for dept_id in s["sub_dept_id_list"]:
                ids.extend(self.get_dept_list_id_fetch_child(dept_id))
        return ids

    def sync_ding_user_id(self):
        """
        本公司使用工号（username）登陆archer，并且工号对应钉钉系统中字段 "jobnumber"。
        所以可根据钉钉中 jobnumber 查到该用户的 ding_user_id。
        """
        try:
            token = get_access_token()
            ding_dept_ids = [2925013, 3031405, 87855807, 3112102, 15993333, 63560181, 58136494, 42082913, 19928483,
                             36049564, 3281417, 22545014, 66589427, 62935266, 88588108, 30030483, 62269218,
                             58131518, 62134556]
            for dept_id in ding_dept_ids:
                dept_id_list = self.get_dept_list_id_fetch_child(dept_id)
                for di in dept_id_list:
                    url = 'https://oapi.dingtalk.com/user/list?access_token={0}&department_id={1}'.format(token, di)
                    try:
                        resp = requests.get(url, timeout=3)
                        ret = str(resp.content, encoding="utf8")
                        # print('user_list_by_dept_id:', ret)
                        s = json.loads(ret)
                        if s["errcode"] == 0:
                            for u in s["userlist"]:
                                try:
                                    cmd = """SETEX {} 86400 {}""".format(u[self.key], u["userid"])
                                    rs.execute_command(cmd)
                                except:
                                    pass
                        else:
                            print(ret)
                    except:
                        pass
                    time.sleep(1)
        except Exception as e:
            traceback.print_exc()

if __name__ == "__main__":
    Ding().sync_ding_user_id()
