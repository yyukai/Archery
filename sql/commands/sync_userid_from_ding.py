#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from sql.models import Users
from sql.utils.ding_api import get_access_token, get_dept_list_id_fetch_child
from sql.utils.api import HttpRequests

if __name__ == '__main__':
    http_request = HttpRequests()
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
                    Users.objects.filter(username=u["jobnumber"]).update(user_id=u["userid"])
