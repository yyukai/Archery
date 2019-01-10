#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import datetime
import requests
import hashlib
import os
import traceback
import simplejson as json


class WPan(object):
    def __init__(self, file=None, uuid=None):
        self.token = "eabd63159b004eaa9e9a9c383a61713b"
        self.headers = {
            "Content-Type": "application/json",
            "w-pan-token": self.token
        }
        self.file = file
        if os.path.exists(self.file):
            self.filename = os.path.basename(self.file)
            self.file_size = os.path.getsize(self.file)
        self.uuid = uuid
        self.surl = 'http://wpan2.admin.weidai.com.cn:8080/api/'

    # 上传小文件
    def upload_small_files(self, parent_folder_id=-1):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/files/uploadSmallFile'
        url = self.surl + 'files/uploadSmallFile'
        # 计算文件md5值
        md5file = open(self.file, 'rb')
        fmd5 = hashlib.md5(md5file.read()).hexdigest()
        md5file.close()
        # 要上传的文件
        m = {'file': open(self.file, 'rb').read()}
        # 需要携带的参数
        data = {
            "token": self.token,
            "md5Digest": fmd5,
            "name": self.filename,
            "dirParentId": parent_folder_id
        }
        # 使用m和data上传文件
        try:
            ret = requests.post(url, headers=self.headers, files=m, data=data)
            result = ret.json()
            if result['code'] == 0 or result['code'] == 8:
                return result['message']
            else:
                return '小文件上传接口调用失败!'
        except:
            traceback.print_exc()
            return '小文件上传接口调用出错！'

    def before_upload_big_file(self, parent_folder_id=-1):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/files/beforeUploadBigFile'
        url = self.surl + 'files/beforeUploadBigFile'
        headers = {
            "w-pan-token": self.token,
            "Content-Type": "application/json"
        }
        md5file = open(self.file, 'rb')
        fmd5 = hashlib.md5(md5file.read()).hexdigest()
        md5file.close()
        data_dic = {
            "folderPath": None,
            "fileSize": self.file_size,
            "dirParentId": parent_folder_id,
            "md5Digest": fmd5,
            "name": self.filename,
            "token": self.token
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            result = ret.json()
            if result['code'] == 0 or result['code'] == 8:
                return result['message']
            else:
                return '大文件预上传接口调用失败!'
        except:
            traceback.print_exc()
            return '大文件预上传接口调用出错！'

    def upload_big_file(self, parent_folder_id=-1):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/files/uploadBigFile'
        # url = 'http://wpan2.admin.weidai.com.cn:8080/api/files/uploadBigFile'
        url = self.surl + 'files/uploadBigFile'
        chunk_size = 30 * 1024 * 1024
        chunks = self.file_size // chunk_size + 1
        md5file = open(self.file, 'rb')
        fmd5 = hashlib.md5(md5file.read()).hexdigest()
        md5file.close()

        try:
            fo = open(self.file, 'rb')
            for chunk in range(1, chunks + 1):
                cs = (chunk - 1) * chunk_size
                ce = cs + chunk_size
                m = {'file': fo.read(chunk_size)}
                data = {
                    "fileSize": self.file_size,
                    "chunks": chunks,
                    "dirParentId": parent_folder_id,
                    "md5Digest": fmd5,
                    "name": self.filename,
                    "chunk": chunk,
                    "chunkStart": cs,
                    "chunkEnd": ce,
                    "token": self.token
                }
                ret = requests.post(url, headers=self.headers, files=m, data=data)
                result = ret.json()
                if result['code'] != 0 and result['code'] != 8:
                    return '大文件上传接口调用失败! %s' % result['message']
        except:
            traceback.print_exc()
            return '大文件上传接口调用出错!'

    # 文件下载统计
    def download_count(self, file_id):
        url = self.surl + 'auditLog/queryFileDownloadAuditLog'
        headers = {
            "w-pan-token": self.token,
            "Content-Type": "application/json"
        }
        data_dic = {
            "fileId": file_id,    # 文件id
            "token": self.token,  # 认证token
            "page": 0,            # 分页参数，第几页，从0开始
            "pageSize": 20        # 每页返回的数据条数
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            return ret.json()
        except:
            traceback.print_exc()
            return '下载统计接口调用出错！'

    def delete_files(self, file_id):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/files/delete'
        url = self.surl + 'files/delete'
        data_dic = {
            "sourceFileIds": file_id.split(','),
            "token": self.token
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            result = ret.json()
            if result['code'] == 0:
                return result['message']
            else:
                return '删除接口调用失败！%s' % result['message']
        except:
            traceback.print_exc()
            return '删除接口调用出错！'

    def create_folder(self, parent_folder_id=-1):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/folder/create'
        url = self.surl + 'folder/create'
        # 需要携带的参数
        data_dic = {
            "dirName": self.folder_name,
            "token": self.token,
            "dirParentId": parent_folder_id
        }
        data_json = json.dumps(data_dic)
        # 使用m和data上传文件
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            result = ret.json()
            if result['code'] == 0:
                return result['message']
            else:
                return '创建文件夹接口调用失败！%s' % result['message']
        except:
            traceback.print_exc()
            return '创建文件夹接口调用失败！'

    def create_user_share(self, file_id=None, expired=None, user_ding_id=None):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/share/createUserShare'
        url = self.surl + 'share/createUserShare'
        if expired is None:
            expired_type = 2  # 永久有效
        else:
            expired_type = 1  # 有限过期
        data_dic = {
            "expiredType": expired_type,
            "shareFileList": file_id.split(','),
            "expiredTime": expired,
            "shareDeptList": [""],
            "shareUserList": user_ding_id.split(','),
            "timeUnit": 1,
            "token": self.token
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            result = ret.json()
            if result['code'] == 0:
                share_link = result['data']['link']
                uuid = share_link.split('#/s')[0].split('?u=')[-1]
                expired = result['data']['expiredTime']
                return uuid, expired, result['message']
            else:
                return None, None, '文件分享失败！%s' % result['message']
        except:
            traceback.print_exc()
            return '分享接口调用出错！'

    def cancel_share(self, uuid):
        url = self.surl + 'share/cancelShare'
        data_dic = {
            "shareUUIds": [uuid],
            "token": self.token
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            result = ret.json()
            if result['code'] == 0:
                return result['message']
            else:
                return '取消分享接口调用失败！%s' % result['message']
        except:
            traceback.print_exc()
            print('取消分享接口调用出错！')
