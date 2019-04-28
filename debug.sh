#!/bin/bash

nohup python3 manage.py runserver 0.0.0.0:9123  --insecure &

# 编译翻译文件
nohup python3 manage.py compilemessages &

# 启动Django Q cluster
nohup python3 manage.py qcluster &
