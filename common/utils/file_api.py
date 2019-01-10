#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import subprocess
import re


def escape_filename(file_list):
    """
    将文件名中空格，括号，引号等符号加转义符
    :param file_list:
    :return:
    """
    if "'" in file_list:
        file_list = file_list.replace("'", "\\'")
    if " " in file_list:
        file_list = file_list.replace(" ", "\ ")
    if "(" in file_list:
        file_list = file_list.replace("(", "\(")
    if ")" in file_list:
        file_list = file_list.replace(")", "\)")
    return file_list


def get_file_type(file_name):
    """
    获取文件类型,返回file命令的结果
    """
    file_type = str(subprocess.getstatusoutput('file -b --mime-type ' + file_name)[1])
    return file_type


def get_file_content(file_name, username, overview=True):
    file_content = ''
    file_name = escape_filename(file_name)
    if re.match(r'text/.*|application/xml', get_file_type(file_name)):
        if overview:
            cmd = """if [ `cat {0}|wc -l` -gt 10 ];then
                        head {0} |enca -L zh_CN.UTF-8 -c;echo "......省略";
                        tail {0} |enca -L zh_CN.UTF-8 -c;
                      else
                        cat {0} |enca -L zh_CN.UTF-8 -c;
                     fi""".format(file_name)
            file_content += str(subprocess.getstatusoutput(cmd)[1])
        else:
            file_content += str(subprocess.getstatusoutput('cat ' + file_name))
        print(file_content)
    elif re.match(r'application/vnd.ms-excel', get_file_type(file_name)):
        file_content += '暂时只支持预览文本类型的文件！\n'
    elif re.match(r'application/msword', get_file_type(file_name)):
        file_content += '暂时只支持预览文本类型的文件！\n'
    else:
        file_content += '暂时只支持预览文本类型的文件！\n'
        # file_content += '除文本、word、excel类型外的文件暂时无法预览！\n'
    pattern = '.*' + username + '/'
    return '================== 文件：' + re.sub(pattern, '', file_name) + ' ==================\n' + file_content
