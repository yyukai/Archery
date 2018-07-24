#!/usr/bin/env python
# -*- coding: utf-8 -*-

from multiprocessing import Process
import requests
import traceback


class HttpRequests(object):
    def __init__(self, headers=None):
        self.timeout = 3
        if headers is None:
            self.headers = {"Content-Type": "application/text"}
        else:
            self.headers = headers

    def post(self):
        pass

    def get(self, url):
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.timeout)
            status = True if resp.status_code == 200 else False

            if resp.apparent_encoding != 'utf-8':
                return status, resp.text.encode('utf-8')
            else:
                return status, resp.text
        except Exception as e:
            return False, str(e)


class DingSender(object):
    def __init__(self):
        pass

    def send_msg(self, user_id):
        pass
