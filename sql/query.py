# -*- coding: UTF-8 -*-
import logging
import re
import time
import traceback

import simplejson as json
import os
import shutil
from wsgiref.util import FileWrapper
import datetime
import xlwt

from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import permission_required
from django.core import serializers
from django.db import connection, OperationalError
from django.db.models import Q
from django.http import HttpResponse
from django_q.tasks import async_task, fetch

from common.config import SysConfig
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.query_privileges import query_priv_check
from .models import QueryLog, Instance, QueryExport, Users
from sql.utils.api import BASE_DIR, async_func
from sql.engines import get_engine, ResultSet

logger = logging.getLogger('default')


@permission_required('sql.query_submit', raise_exception=True)
def query(request):
    """
    获取SQL查询结果
    :param request:
    :return:
    """
    instance_name = request.POST.get('instance_name')
    sql_content = request.POST.get('sql_content')
    db_name = request.POST.get('db_name')
    limit_num = int(request.POST.get('limit_num', 0))
    user = request.user

    result = {'status': 0, 'msg': 'ok', 'data': {}}
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result['status'] = 1
        result['msg'] = '实例不存在'
        return result

    # 服务器端参数验证
    if None in [sql_content, db_name, instance_name, limit_num]:
        result['status'] = 1
        result['msg'] = '页面提交参数可能为空'
        return HttpResponse(json.dumps(result), content_type='application/json')

    try:
        config = SysConfig()
        # 查询前的检查，禁用语句检查，语句切分
        query_engine = get_engine(instance=instance)
        query_check_info = query_engine.query_check(db_name=db_name, sql=sql_content)
        if query_check_info.get('bad_query'):
            # 引擎内部判断为 bad_query
            result['status'] = 1
            result['msg'] = query_check_info.get('msg')
            return HttpResponse(json.dumps(result), content_type='application/json')
        if query_check_info.get('has_star') and config.get('disable_star') is True:
            # 引擎内部判断为有 * 且禁止 * 选项打开
            result['status'] = 1
            result['msg'] = query_check_info.get('msg')
            return HttpResponse(json.dumps(result), content_type='application/json')
        sql_content = query_check_info['filtered_sql']

        # 查询权限校验，并且获取limit_num
        priv_check_info = query_priv_check(user, instance, db_name, sql_content, limit_num)
        if priv_check_info['status'] == 0:
            limit_num = priv_check_info['data']['limit_num']
            priv_check = priv_check_info['data']['priv_check']
        else:
            result['status'] = 1
            result['msg'] = priv_check_info['msg']
            return HttpResponse(json.dumps(result), content_type='application/json')
        # explain的limit_num设置为0
        limit_num = 0 if re.match(r"^explain", sql_content.lower()) else limit_num

        # 对查询sql增加limit限制或者改写语句
        sql_content = query_engine.filter_sql(sql=sql_content, limit_num=limit_num)

        # 执行查询语句，timeout=max_execution_time
        max_execution_time = int(config.get('max_execution_time', 60))
        query_task_id = async_task(query_engine.query, db_name=str(db_name), sql=sql_content, limit_num=limit_num,
                                   timeout=max_execution_time, cached=60)
        # 等待执行结果，max_execution_time后还没有返回结果代表将会被终止
        query_task = fetch(query_task_id, wait=max_execution_time * 1000, cached=True)
        # 在max_execution_time内执行结束
        if query_task:
            if query_task.success:
                query_result = query_task.result
                query_result.query_time = query_task.time_taken()
            else:
                query_result = ResultSet(full_sql=sql_content)
                query_result.error = query_task.result
        # 等待超时，async_task主动关闭连接
        else:
            query_result = ResultSet(full_sql=sql_content)
            query_result.error = f'查询时间超过 {max_execution_time} 秒，已被主动终止，请优化语句或者联系管理员。'

        # 查询异常
        if query_result.error:
            result['status'] = 1
            result['msg'] = query_result.error
        # 数据脱敏，仅对查询无错误的结果集进行脱敏，并且按照query_check配置是否返回
        elif config.get('data_masking'):
            query_masking_task_id = async_task(query_engine.query_masking, db_name=db_name, sql=sql_content,
                                               resultset=query_result, cached=60)
            query_masking_task = fetch(query_masking_task_id, wait=60 * 1000, cached=True)
            if query_masking_task.success:
                masking_result = query_masking_task.result
                masking_result.mask_time = query_masking_task.time_taken()
                # 脱敏出错
                if masking_result.error:
                    # 开启query_check，直接返回异常，禁止执行
                    if config.get('query_check'):
                        result['status'] = 1
                        result['msg'] = masking_result.error
                    # 关闭query_check，忽略错误信息，返回未脱敏数据，权限校验标记为跳过
                    else:
                        query_result.error = None
                        priv_check = False
                        result['data'] = query_result.__dict__
                    logger.error(f'数据脱敏异常，查询语句：{sql_content}\n，错误信息：{masking_result.error}')
                # 正常脱敏
                else:
                    result['data'] = masking_result.__dict__
            else:
                logger.error(f'数据脱敏异常，查询语句：{sql_content}\n，错误信息：{query_masking_task.result}')
                # 抛出未定义异常，并且开启query_check，直接返回异常，禁止执行
                if config.get('query_check'):
                    result['status'] = 1
                    result['msg'] = f'数据脱敏异常，请联系管理员，错误信息：{query_masking_task.result}'
                # 关闭query_check，忽略错误信息，返回未脱敏数据，权限校验标记为跳过
                else:
                    query_result.error = None
                    priv_check = False
                    result['data'] = query_result.__dict__
        # 无需脱敏的语句
        else:
            result['data'] = query_result.__dict__

        # 仅将成功的查询语句记录存入数据库
        if not query_result.error:
            if int(limit_num) == 0:
                limit_num = int(query_result.affected_rows)
            else:
                limit_num = min(int(limit_num), int(query_result.affected_rows))
            query_log = QueryLog(
                username=user.username,
                user_display=user.display,
                db_name=db_name,
                instance_name=instance.instance_name,
                sqllog=sql_content,
                effect_row=limit_num,
                cost_time=query_result.query_time,
                priv_check=priv_check,
                hit_rule=query_result.mask_rule_hit,
                masking=query_result.is_masked
            )
            # 防止查询超时
            try:
                query_log.save()
            except OperationalError:
                connection.close()
                query_log.save()
    except Exception as e:
        logger.error(f'查询异常报错，查询语句：{sql_content}\n，错误信息：{traceback.format_exc()}')
        result['status'] = 1
        result['msg'] = f'查询异常报错，错误信息：{e}'
        return HttpResponse(json.dumps(result), content_type='application/json')
    # 返回查询结果
    try:
        return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                            content_type='application/json')
    # 虽然能正常返回，但是依然会乱码
    except UnicodeDecodeError:
        return HttpResponse(json.dumps(result, default=str, bigint_as_string=True, encoding='latin1'),
                            content_type='application/json')


@permission_required('sql.menu_sqlquery', raise_exception=True)
def querylog(request):
    """
    获取sql查询记录
    :param request:
    :return:
    """
    # 获取用户信息
    user = request.user

    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit
    search = request.POST.get('search', '')

    sql_log = QueryLog.objects.all()
    # 过滤搜索信息
    sql_log = sql_log.filter(Q(sqllog__icontains=search) | Q(user_display__icontains=search))
    # 管理员查看全部数据
    if user.is_superuser:
        sql_log = sql_log
    # 普通用户查看自己的数据
    else:
        sql_log = sql_log.filter(username=user.username)

    sql_log_count = sql_log.count()
    sql_log_list = sql_log.order_by('-id')[offset:limit]
    # QuerySet 序列化
    sql_log_list = serializers.serialize("json", sql_log_list)
    sql_log_list = json.loads(sql_log_list)
    sql_log = [log_info['fields'] for log_info in sql_log_list]

    result = {"total": sql_log_count, "rows": sql_log}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


# 异步获取SQL查询结果
@csrf_exempt
@permission_required('sql.query_submit', raise_exception=True)
def add_async_query(request):
    instance_name = request.POST.get('instance_name')
    sql_content = request.POST.get('sql_content')
    db_name = request.POST.get('db_name')
    schema_name = request.POST.get('schema_name')
    limit_num = int(request.POST.get('limit_num'))
    auditor = request.POST.get('auditor')
    user = request.user

    result = {'status': 0, 'msg': 'ok', 'data': {}}
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result['status'] = 1
        result['msg'] = '实例不存在'
        return result

    # 服务器端参数验证
    if None in [sql_content, instance_name, limit_num]:
        result['status'] = 1
        result['msg'] = '页面提交参数可能为空'
        return HttpResponse(json.dumps(result), content_type='application/json')

    config = SysConfig()
    # 查询前的检查，禁用语句检查，语句切分
    query_engine = get_engine(instance=instance)
    query_check_info = query_engine.query_check(db_name=db_name, sql=sql_content)
    if query_check_info.get('bad_query'):
        # 引擎内部判断为 bad_query
        result['status'] = 1
        result['msg'] = query_check_info.get('msg')
        return HttpResponse(json.dumps(result), content_type='application/json')
    if query_check_info.get('has_star') and config.get('disable_star') is True:
        # 引擎内部判断为有 * 且禁止 * 选项打开
        result['status'] = 1
        result['msg'] = query_check_info.get('msg')
        return HttpResponse(json.dumps(result), content_type='application/json')
    sql_content = query_check_info['filtered_sql']

    # 查询权限校验，并且获取limit_num
    priv_check_info = query_priv_check(user, instance, db_name, schema_name, sql_content, limit_num)
    if priv_check_info['status'] == 0:
        limit_num = priv_check_info['data']['limit_num']
        priv_check = priv_check_info['data']['priv_check']
    else:
        result['status'] = 1
        result['msg'] = priv_check_info['msg']
        return HttpResponse(json.dumps(result), content_type='application/json')
    # explain的limit_num设置为0
    limit_num = 0 if re.match(r"^explain", sql_content.lower()) else limit_num

    # 对查询sql增加limit限制或者改写语句
    sql_content = query_engine.filter_sql(sql=sql_content, limit_num=limit_num)

    if instance.db_type == "oracle":
        query_log = QueryLog.objects.create(username=user.username, user_display=user.display,
                                            schema_name=schema_name,
                                            instance_name=instance_name, sqllog=sql_content, effect_row=0,
                                            priv_check=priv_check)
    else:
        query_log = QueryLog.objects.create(username=user.username, user_display=user.display, db_name=db_name,
                                            instance_name=instance_name, sqllog=sql_content, effect_row=0,
                                            priv_check=priv_check)
    qe = QueryExport.objects.create(query_log=query_log, auditor=Users.objects.get(username=auditor), status=0)

    do_async_query(request, qe, instance_name, db_name, schema_name, sql_content, limit_num)
    result['msg'] = '任务提交成功！后台拼命跑数据中... 请耐心等待钉钉或邮件通知！'
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


@async_func
def do_async_query(request, query_export, instance_name, db_name, schema_name, sql_content, limit_num):
    query_log = query_export.query_log
    instance = Instance.objects.get(instance_name=instance_name)
    query_engine = get_engine(instance=instance)
    t_start = int(time.time())
    if instance.db_type == "oracle":
        sql_result = query_engine.query(schema_name=schema_name, sql=sql_content, limit_num=limit_num)
    else:
        sql_result = query_engine.query(db_name=db_name, sql=sql_content, limit_num=limit_num)
    t_end = int(time.time())
    query_log.cost_time = t_end - t_start
    query_log.effect_row = sql_result.affected_rows
    query_log.save()

    def write_result_to_excel(res):
        try:
            file_dir = SysConfig().sys_config.get('query_result_dir', BASE_DIR)
            time_suffix = datetime.datetime.now().strftime("%m%d%H%M%S")
            if instance.db_type == "oracle":
                file_name = '{}-{}-{}-{}'.format(query_export.query_log.username, instance_name, schema_name,
                                                 time_suffix)
            else:
                file_name = '{}-{}-{}-{}'.format(query_export.query_log.username, instance_name, db_name,
                                                 time_suffix)
            template_file = os.path.join(file_dir, file_name)

            workbook = xlwt.Workbook(encoding='utf-8')
            sheet = workbook.add_sheet('Sheet1', cell_overwrite_ok=True)
            # 写入字段信息
            for field in range(0, len(res.column_list)):
                sheet.write(0, field, res.column_list[field])
            # 写入数据段信息
            for row in range(1, int(res.affected_rows) + 1):
                for col in range(0, len(res.column_list)):
                    print(type(res.rows[row - 1][col]), res.rows[row - 1][col])
                    value = '' if res.rows[row - 1][col] is None else res.rows[row - 1][col]
                    sheet.write(row, col, value)
            workbook.save(template_file)
        except Exception as e:
            traceback.print_exc()
            query_export.error_msg = str(e)
            query_export.save(update_fields=['error_msg'])
            return str(e)
        return template_file

    try:
        file_path = ''
        if SysConfig().sys_config.get('data_masking'):
            try:
                if instance.db_type == "oracle":
                    masking_result = query_engine.query_masking(schema_name, sql_content, sql_result)
                else:
                    masking_result = query_engine.query_masking(db_name, sql_content, sql_result)
            except Exception as e:
                if SysConfig().sys_config.get('query_check'):
                    query_export.status = 1
                    query_export.error_msg = '脱敏数据报错,请联系管理员。报错：%s' % str(e)
            else:
                if masking_result.status is None or not SysConfig().sys_config.get('query_check'):
                    file_path = write_result_to_excel(sql_result)
                    query_export.status = 2
        else:
            file_path = write_result_to_excel(sql_result)
            query_export.status = 2
        query_export.result_file = file_path
    except Exception as e:
        traceback.print_exc()
        query_export.error_msg = str(e)
        query_export.status = 1
    query_export.save()

    # 通知审核人审核
    audit_url = "{}://{}/query_export/".format(request.scheme, request.get_host())
    if instance.db_type == "oracle":
        msg_content = '''导出查询（提取大量数据）下载申请等待您审批：\n发起人：{}\n实例名称：{}\n模式：{}\n执行的sql查询：{}\n提取条数：{}\n操作时间：{}\n审批地址：{}\n'''. \
            format(query_log.user_display, query_log.instance_name, query_log.schema_name, query_log.sqllog,
                   query_log.effect_row, query_log.create_time, audit_url)
    else:
        msg_content = '''导出查询（提取大量数据）下载申请等待您审批：\n发起人：{}\n实例名称：{}\n数据库：{}\n执行的sql查询：{}\n提取条数：{}\n操作时间：{}\n审批地址：{}\n'''.\
            format(query_log.user_display, query_log.instance_name, query_log.db_name, query_log.sqllog,
                   query_log.effect_row, query_log.create_time, audit_url)
    from sql.utils.ding_api import DingSender
    DingSender().send_msg(query_export.auditor.ding_user_id, msg_content)


@csrf_exempt
@permission_required('sql.query_submit', raise_exception=True)
def query_export_audit(request):
    query_export_id = request.POST.get("id")
    is_allow = request.POST.get("is_allow", '')
    audit_msg = request.POST.get("audit_msg", '')
    qe = QueryExport.objects.get(id=query_export_id)
    ql = qe.query_log
    applicant = Users.objects.get(username=ql.username)
    if qe.status == 1:
        # 执行失败
        msg = qe.error_msg
    elif qe.status == 2:
        # 审核
        user = request.user
        if not user.has_perm('sql.query_export_review'):
            msg = "你没有审核权限！"
        else:
            if is_allow == "yes":
                qe.status = 3
                msg = "已通过！"
                if os.path.exists(qe.result_file):
                    if os.path.getsize(qe.result_file) > 0:
                        shutil.copy2(qe.result_file, "{}.xls".format(qe.result_file.split('/')[-1]))
            else:
                qe.status = 4
                msg = "已拒绝！"
            from sql.utils.ding_api import DingSender
            msg_content = '''您的导出查询提取数据申请 {}：\n审核理由：{}\n实例名称：{}\n数据库：{}\n执行的sql查询：{}\n提取条数：{}\n操作时间：{}\n'''.\
                format(msg, audit_msg, ql.instance_name, ql.db_name, ql.sqllog, ql.effect_row, ql.create_time)
            DingSender().send_msg(applicant.ding_user_id, msg_content)

        qe.audit_msg = audit_msg
        qe.auditor = user
        qe.save()
    elif qe.status == 4:
        # 审核人拒绝
        msg = qe.audit_msg
    else:
        msg = "未知状态！"
    return HttpResponse(msg)


@csrf_exempt
@permission_required('sql.query_submit', raise_exception=True)
def query_export_cancel(request):
    query_export_id = request.POST.get("id")
    if QueryExport.objects.filter(id=query_export_id).exists():
        QueryExport.objects.filter(id=query_export_id).update(status=5)
        msg = "取消成功！"
    else:
        msg = "未找到该申请！"
    return HttpResponse(msg)


@csrf_exempt
@permission_required('sql.query_submit', raise_exception=True)
def query_result_export(request):
    query_export_id = request.GET.get("id")
    qe = QueryExport.objects.get(id=query_export_id)

    if qe.result_file:
        wrapper = FileWrapper(open(qe.result_file, "rb"))
        response = HttpResponse(wrapper, content_type='application/vnd.ms-excel')
        response['Content-Length'] = os.path.getsize(qe.result_file)
        response['Content-Disposition'] = 'attachment; filename="result.xls"'
        return response
    else:
        return HttpResponse(qe.error_msg)


# 获取导出查询记录
@csrf_exempt
@permission_required('sql.menu_query_export', raise_exception=True)
def query_export_log(request):
    # 获取用户信息
    user = request.user

    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit

    # 获取搜索参数
    search = request.POST.get('search', '')

    # 查询个人记录，超管查看所有数据
    if user.is_superuser or user.has_perm('sql.query_export_review'):
        log_count = QueryExport.objects.filter(Q(query_log__sqllog__contains=search) |
                                                   Q(query_log__username__contains=search) |
                                                   Q(query_log__db_name__contains=search)).count()
        qe_list = QueryExport.objects.filter(Q(query_log__sqllog__contains=search) |
                                                  Q(query_log__username__contains=search) |
                                                  Q(query_log__db_name__contains=search)).order_by('-id')[offset:limit]
    else:
        log_count = QueryExport.objects.filter(query_log__username=user.username).filter(
                                                    Q(query_log__sqllog__contains=search) |
                                                    Q(query_log__db_name__contains=search)).count()
        qe_list = QueryExport.objects.filter(query_log__username=user.username).filter(
                                                    Q(query_log__sqllog__contains=search) |
                                                    Q(query_log__db_name__contains=search)).order_by('-id')[offset:limit]

    sql_log_list = list()
    for qe in qe_list:
        ql = qe.query_log
        sql_log_list.append({"user_display": ql.user_display, "instance_name": ql.instance_name, "db_name": ql.db_name,
                             "create_time": ql.create_time, "sqllog": ql.sqllog, "effect_row": ql.effect_row,
                             "cost_time": ql.cost_time, "reason": qe.reason, "status": qe.status,
                             "audit_msg": qe.audit_msg, "auditor": qe.auditor.display, "id": qe.id})

    result = {"total": log_count, "rows": sql_log_list}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')
