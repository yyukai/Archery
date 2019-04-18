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
        # self.token = '387adb0d06dd4c3486d29683b99228a7'
        # self.surl = 'http://wpan2.admin.weidai.com.cn:8080/api/'

        self.token = 'ce92463b5cb54779a8c4c07ce15bb3ac'
        self.surl = 'http://172.20.100.244:8080/api/'

        self.headers = {
            "Content-Type": "application/json",
            "w-pan-token": self.token
        }
        self.file = file
        if os.path.exists(self.file):
            self.filename = os.path.basename(self.file)
            self.file_size = os.path.getsize(self.file)
        self.uuid = uuid

    # 上传小文件
    def upload_small_files(self, username, parent_folder_id=-1):
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
            "dirParentId": parent_folder_id,
            "folderPath": "archer_upload/%s/%s" % (username, self.filename),
        }
        # 使用m和data上传文件
        try:
            headers = {
                "w-pan-token": self.token
            }
            ret = requests.post(url, headers=headers, files=m, data=data)
            result = ret.json()
            print("000000", result)
            # {'code': 8, 'data': {'id': 194724, 'fileSize': '10KB', 'fileSpecificType': 4, 'ifSupportPreview': False}, 'success': False}
            if result['code'] == 0:
                return {"code": 0, "result": {"id": result['data']}}
            if result['code'] == 8:
                return {"code": 0, "result": {"id": result['data']['id']}}
            else:
                return {"code": -1, "errmsg": json.dumps(result)}
        except Exception as e:
            print('小文件上传接口调用出错！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

    def before_upload_big_file(self, username, parent_folder_id=-1):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/files/beforeUploadBigFile'
        url = self.surl + 'files/beforeUploadBigFile'
        md5file = open(self.file, 'rb')
        fmd5 = hashlib.md5(md5file.read()).hexdigest()
        md5file.close()
        data_dic = {
            "fileSize": self.file_size,
            "dirParentId": parent_folder_id,
            "md5Digest": fmd5,
            "name": self.filename,
            "folderPath": "archer_upload/%s/%s" % (username, self.filename),
            "token": self.token
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            print("11111", ret.text)
            result = ret.json()
            if result['code'] == 0 or result['code'] == 8:
                return {"code": 0, "result": {"id": result['data']['id']}}
            else:
                return {"code": -1, "errmsg": json.dumps(result)}
        except Exception as e:
            print('大文件预上传接口调用出错！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

    def upload_big_file(self, username, parent_folder_id=-1):
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
                    "folderPath": "archer_upload/%s/%s" % (username, self.filename),
                    "chunk": chunk,
                    "chunkStart": cs,
                    "chunkEnd": ce,
                    "token": self.token
                }
                headers = {
                    "w-pan-token": self.token
                }
                ret = requests.post(url, headers=headers, files=m, data=data)
                print("222222", ret.text)
                result = ret.json()
                if result['code'] != 0 and result['code'] != 8:
                    return {"code": -1, "errmsg": json.dumps(result)}
            return {"code": 0, "result": "complete."}
        except Exception as e:
            print('大文件上传接口调用出错!', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

    def upload_file(self, username):
        if self.file_size > 31457280:
            # 文件大于30M，则使用大文件上传接口
            res = self.before_upload_big_file(username)
            # {"code":8,"data":{"id":194575,"fileSize":"30.6M","fileSpecificType":4,"ifSupportPreview":false},"success":false}
            info = self.upload_big_file(username)
            # {"code":0,"data":194578,"success":true}
            if info['code'] == -1:
                return info
        else:
            # 调用小文件上传接口
            res = self.upload_small_files(username)
            # {'code': 0, 'result': 194582}
        return res

    # 文件下载统计
    def download_count(self, file_id):
        url = self.surl + 'auditLog/queryFileDownloadAuditLog'
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
        except Exception as e:
            print('下载统计接口调用出错！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

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
            return ret.json()
        except Exception as e:
            print('删除接口调用出错！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

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
            return ret.json()
        except Exception as e:
            print('创建文件夹接口调用失败！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

    def create_user_share(self, file_id, expired=None, user_ding_id=None):
        # url = 'http://wpan.admin.weidai.com.cn/api/api/share/createUserShare'
        url = self.surl + 'share/createUserShare'
        if expired is None:
            expired_type = 2  # 永久有效
        else:
            expired_type = 1  # 有限过期
        data_dic = {
            "expiredType": expired_type,
            "shareFileList": str(file_id).split(','),
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
            print(result)
            if result['code'] == 0:
                share_link = result['data']['link']
                uuid = share_link.split('#/s')[0].split('?u=')[-1]
                expired = result['data']['expiredTime']
                return {
                    "code": 0,
                    "result": {
                        "share_link": share_link,
                        "uuid": uuid,
                        "expired": expired
                    }
                }
            else:
                return {"code": -1, "errmsg": result['message']}
        except Exception as e:
            print('分享接口调用出错！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}

    def cancel_share(self, uuid):
        url = self.surl + 'share/cancelShare'
        data_dic = {
            "shareUUIds": [uuid],
            "token": self.token
        }
        data_json = json.dumps(data_dic)
        try:
            ret = requests.post(url, headers=self.headers, data=data_json)
            return ret.json()
        except Exception as e:
            print('取消分享接口调用出错！', traceback.print_exc())
            return {"code": -1, "errmsg": str(e)}
