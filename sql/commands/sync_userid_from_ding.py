#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import sys
import django

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

os.environ['DJANGO_SETTINGS_MODULE'] = 'archer.settings'
django.setup()

from sql.models import Users
from sql.utils.ding_api import get_access_token, get_dept_list_id_fetch_child
from sql.utils.api import HttpRequests

if __name__ == '__main__':
    http_request = HttpRequests()
    ding_root_dept_id = 2925013
    token = get_access_token()
    dept_id_list = get_dept_list_id_fetch_child(token, ding_root_dept_id)
    print('ids:', dept_id_list)
    for dept_id in dept_id_list:
        url = 'https://oapi.dingtalk.com/user/list?access_token={0}&department_id={1}'.format(token, dept_id)
        status, ret = http_request.get(url)
        if status is True:
            s = json.loads(ret)
            if s["errcode"] == 0:
                for u in s["userlist"]:
                    print(u)
                    Users.objects.filter(username=u["jobnumber"]).update(ding_user_id=u["userid"])
