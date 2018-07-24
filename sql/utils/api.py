#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import json
import traceback
import datetime
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class HttpRequests(object):
    def __init__(self, timeout=None):
        self.timeout = 3 if timeout is None else timeout

    def post(self, url, params):
        try:
            headers = {"Content-Type": "application/json"}
            resp = requests.post(url, headers=headers, json=params, timeout=self.timeout)
            status = True if resp.status_code == 200 else False

            print(resp.content)
            if resp.apparent_encoding != 'utf-8':
                return status, str(resp.content, encoding="utf8")
            else:
                return status, resp.content
        except Exception as e:
            return False, str(e)

    def get(self, url):
        try:
            resp = requests.get(url, timeout=self.timeout)
            status = True if resp.status_code == 200 else False

            if resp.apparent_encoding != 'utf-8':
                return status, str(resp.content, encoding="utf8")
            else:
                return status, resp.content
        except Exception as e:
            return False, str(e)


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def send_mail(contents, mail_to, sub=u'运维告警邮件'):
    try:
        contents = contents.replace('\n', '<br>')
        my_dict = {'to': ','.join(mail_to), 'subject': sub, 'contents': contents}
        http_request = HttpRequests()
        status, ret = http_request.post("http://172.20.1.47/mailsender.php", my_dict)
        if status is not True:
            print("邮件发送失败：%s" % ret)
    except Exception as e:
        print("邮件发送失败：%s" % str(e))


def bash(cmd):
    """执行bash命令"""
    return subprocess.call(cmd, shell=True)
