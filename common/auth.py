import datetime

import simplejson as json
from django.conf import settings
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import Group
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse

from common.config import SysConfig
from sql.models import Users
from sql.views import logger
from sql.sql_workflow import login_failure_counter, logger


def loginAuthenticate(username, password):
    """登录认证，包含一个登录失败计数器，5分钟内连续失败5次的账号，会被锁定5分钟"""
    sys_config = SysConfig().sys_config
    if sys_config.get('lock_cnt_threshold'):
        lockCntThreshold = int(sys_config.get('lock_cnt_threshold'))
    else:
        lockCntThreshold = 5
    if sys_config.get('lock_time_threshold'):
        lockTimeThreshold = int(sys_config.get('lock_time_threshold'))
    else:
        lockTimeThreshold = 300

    # 服务端二次验证参数
    if username == "" or password == "" or username is None or password is None:
        result = {'status': 2, 'msg': '登录用户名或密码为空，请重新输入!', 'data': ''}
    elif username in login_failure_counter and login_failure_counter[username]["cnt"] >= lockCntThreshold and (
            datetime.datetime.now() - login_failure_counter[username][
        "last_failure_time"]).seconds <= lockTimeThreshold:
        result = {'status': 3, 'msg': '登录失败超过5次，该账号已被锁定5分钟!', 'data': ''}
    else:
        # 登录
        user = authenticate(username=username, password=password)
        # 登录成功
        if user:
            # 如果登录失败计数器中存在该用户名，则清除之
            if username in login_failure_counter:
                login_failure_counter.pop(username)
            result = {'status': 0, 'msg': 'ok', 'data': user}
        # 登录失败
        else:
            if username not in login_failure_counter:
                # 第一次登录失败，登录失败计数器中不存在该用户，则创建一个该用户的计数器
                login_failure_counter[username] = {"cnt": 1, "last_failure_time": datetime.datetime.now()}
            else:
                if (datetime.datetime.now() - login_failure_counter[username][
                    "last_failure_time"]).seconds <= lockTimeThreshold:
                    login_failure_counter[username]["cnt"] += 1
                else:
                    # 上一次登录失败时间早于5分钟前，则重新计数。以达到超过5分钟自动解锁的目的。
                    login_failure_counter[username]["cnt"] = 1
                login_failure_counter[username]["last_failure_time"] = datetime.datetime.now()
            result = {'status': 1, 'msg': '用户名或密码错误，请重新输入！', 'data': ''}
    return result


# ajax接口，登录页面调用，用来验证用户名密码
def authenticateEntry(request):
    """接收http请求，然后把请求中的用户名密码传给loginAuthenticate去验证"""
    username = request.POST.get('username')
    password = request.POST.get('password')

    result = loginAuthenticate(username, password)
    if result['status'] == 0:
        # 开启LDAP的认证通过后更新用户密码
        if settings.ENABLE_LDAP:
            try:
                Users.objects.get(username=username)
            except Exception:
                insert_info = Users()
                insert_info.password = make_password(password)
                insert_info.save()
            else:
                replace_info = Users.objects.get(username=username)
                replace_info.password = make_password(password)
                replace_info.save()
        # 添加到默认组
        default_auth_group = SysConfig().sys_config.get('default_auth_group', '')
        try:
            user = Users.objects.get(username=username)
            group = Group.objects.get(name=default_auth_group)
            user.groups.add(group)
        except Exception:
            logger.error('无name={}的权限组，无法默认添加'.format(default_auth_group))

        # 调用了django内置登录方法，防止管理后台二次登录
        user = authenticate(username=username, password=password)
        if user:
            login(request, user)

        result = {'status': 0, 'msg': 'ok', 'data': None}

    return HttpResponse(json.dumps(result), content_type='application/json')


# 注册用户
def sign_up(request):
    username = request.POST.get('username')
    password = request.POST.get('password')
    password2 = request.POST.get('password2')
    display = request.POST.get('display')
    email = request.POST.get('email')

    if username is None or password is None:
        context = {'errMsg': '用户名和密码不能为空'}
        return render(request, 'error.html', context)
    if len(Users.objects.filter(username=username)) > 0:
        context = {'errMsg': '用户名已存在'}
        return render(request, 'error.html', context)
    if password != password2:
        context = {'errMsg': '两次输入密码不一致'}
        return render(request, 'error.html', context)

    # 添加用户并且添加到默认组
    Users.objects.create_user(username=username,
                              password=password,
                              display=display,
                              email=email,
                              is_active=1,
                              is_staff=1)
    default_auth_group = SysConfig().sys_config.get('default_auth_group', '')
    try:
        user = Users.objects.get(username=username)
        group = Group.objects.get(name=default_auth_group)
        user.groups.add(group)
    except Exception:
        logger.error('无name={}的权限组，无法默认添加'.format(default_auth_group))
    return render(request, 'login.html')


# 退出登录
def sign_out(request):
    logout(request)
    return HttpResponseRedirect(reverse('sql:login'))
