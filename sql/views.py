# -*- coding: UTF-8 -*-
import traceback
import datetime
import simplejson as json

from django.contrib.auth.decorators import permission_required
from django.contrib.auth.models import Group, Permission
from django.shortcuts import render, get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponseRedirect
from django.db.models import Q
from django.urls import reverse

from sql.utils.inception import InceptionDao
from common.utils.permission import superuser_required
from sql.utils.jobs import job_info

from .models import Users, SqlWorkflow, QueryPrivileges, SqlGroup, \
    QueryPrivilegesApply, Config, DataBase, Replication
from sql.utils.workflow import Workflow
from sql.utils.sql_review import can_execute, can_timingtask, can_cancel
from common.utils.const import Const, WorkflowDict
from sql.utils.group import user_groups, user_instances

import logging

logger = logging.getLogger('default')


# 登录页面
def login(request):
    return render(request, 'login.html')


# SQL上线工单列表页面
@permission_required('sql.menu_sqlworkflow', raise_exception=True)
def sqlworkflow(request):
    return render(request, 'sqlworkflow.html')


# 提交SQL的页面
@permission_required('sql.sql_submit', raise_exception=True)
def submitSql(request):
    user = request.user
    # 获取组信息
    group_list = user_groups(user)

    # 获取所有有效用户，通知对象
    active_user = Users.objects.filter(is_active=1)

    context = {'active_user': active_user, 'group_list': group_list}
    return render(request, 'submitSql.html', context)


# 展示SQL工单详细页面
def detail(request, workflowId):
    workflowDetail = get_object_or_404(SqlWorkflow, pk=workflowId)
    if workflowDetail.status in (Const.workflowStatus['finish'], Const.workflowStatus['exception']) \
            and workflowDetail.is_manual == 0:
        listContent = json.loads(workflowDetail.execute_result)
    else:
        listContent = json.loads(workflowDetail.review_content)

    # 获取当前审批和审批流程
    audit_auth_group, current_audit_auth_group = Workflow.review_info(workflowId, 2)

    # 是否可审核
    is_can_review = Workflow.can_review(request.user, workflowId, 2)
    # 是否可执行
    is_can_execute = can_execute(request.user, workflowId)
    # 是否可定时执行
    is_can_timingtask = can_timingtask(request.user, workflowId)
    # 是否可取消
    is_can_cancel = can_cancel(request.user, workflowId)

    # 获取定时执行任务信息
    if workflowDetail.status == Const.workflowStatus['timingtask']:
        job_id = Const.workflowJobprefix['sqlreview'] + '-' + str(workflowId)
        job = job_info(job_id)
        if job:
            run_date = job.next_run_time
        else:
            run_date = ''
    else:
        run_date = ''

    # sql结果
    column_list = ['ID', 'stage', 'errlevel', 'stagestatus', 'errormessage', 'SQL', 'Affected_rows', 'sequence',
                   'backup_dbname', 'execute_time', 'sqlsha1']
    rows = []
    for row_index, row_item in enumerate(listContent):
        row = {}
        row['ID'] = row_index + 1
        row['stage'] = row_item[1]
        row['errlevel'] = row_item[2]
        row['stagestatus'] = row_item[3]
        row['errormessage'] = row_item[4]
        row['SQL'] = row_item[5]
        row['Affected_rows'] = row_item[6]
        row['sequence'] = row_item[7]
        row['backup_dbname'] = row_item[8]
        row['execute_time'] = row_item[9]
        # row['sqlsha1'] = row_item[10]
        rows.append(row)

        if workflowDetail.status == '执行中':
            row['stagestatus'] = ''.join(
                ["<div id=\"td_" + str(row['ID']) + "\" class=\"form-inline\">",
                 "   <div class=\"progress form-group\" style=\"width: 80%; height: 18px; float: left;\">",
                 "       <div id=\"div_" + str(row['ID']) + "\" class=\"progress-bar\" role=\"progressbar\"",
                 "            aria-valuenow=\"60\"",
                 "            aria-valuemin=\"0\" aria-valuemax=\"100\">",
                 "           <span id=\"span_" + str(row['ID']) + "\"></span>",
                 "       </div>",
                 "   </div>",
                 "   <div class=\"form-group\" style=\"width: 10%; height: 18px; float: right;\">",
                 "       <form method=\"post\">",
                 "           <input type=\"hidden\" name=\"workflowid\" value=\"" + str(workflowDetail.id) + "\">",
                 "           <button id=\"btnstop_" + str(row['ID']) + "\" value=\"" + str(row['ID']) + "\"",
                 "                   type=\"button\" class=\"close\" style=\"display: none\" title=\"停止pt-OSC进程\">",
                 "               <span class=\"glyphicons glyphicons-stop\">&times;</span>",
                 "           </button>",
                 "       </form>",
                 "   </div>",
                 "</div>"])
    context = {'workflowDetail': workflowDetail, 'column_list': column_list, 'rows': rows,
               'is_can_review': is_can_review, 'is_can_execute': is_can_execute, 'is_can_timingtask': is_can_timingtask,
               'is_can_cancel': is_can_cancel, 'audit_auth_group': audit_auth_group,
               'current_audit_auth_group': current_audit_auth_group, 'run_date': run_date}
    return render(request, 'detail.html', context)


# 展示回滚的SQL页面
def rollback(request):
    workflowId = request.GET['workflowid']
    if workflowId == '' or workflowId is None:
        context = {'errMsg': 'workflowId参数为空.'}
        return render(request, 'error.html', context)
    workflowId = int(workflowId)
    try:
        listBackupSql = InceptionDao().getRollbackSqlList(workflowId)
    except Exception as msg:
        logger.error(traceback.format_exc())
        context = {'errMsg': msg}
        return render(request, 'error.html', context)
    workflowDetail = SqlWorkflow.objects.get(id=workflowId)
    workflowName = workflowDetail.workflow_name
    rollbackWorkflowName = "【回滚工单】原工单Id:%s ,%s" % (workflowId, workflowName)
    context = {'listBackupSql': listBackupSql, 'workflowDetail': workflowDetail,
               'rollbackWorkflowName': rollbackWorkflowName}
    return render(request, 'rollback.html', context)


# SQL文档页面
@permission_required('sql.menu_document', raise_exception=True)
def dbaprinciples(request):
    return render(request, 'dbaprinciples.html')


# dashboard页面
@permission_required('sql.menu_dashboard', raise_exception=True)
def dashboard(request):
    return render(request, 'dashboard.html')


# SQL在线查询页面
@permission_required('sql.menu_query', raise_exception=True)
def sqlquery(request):
    # 获取用户关联从库列表
    instances = [slave.instance_name for slave in user_instances(request.user, 'slave')]

    context = {'instances': instances}
    return render(request, 'sqlquery.html', context)


# SQL导出查询（大数据异步查询）
@permission_required('sql.menu_query_export', raise_exception=True)
def query_export(request):
    # 获取用户关联从库列表
    listAllClusterName = [slave.instance_name for slave in user_instances(request.user, 'slave')]
    # 获取导出查询审核人
    auditors = list()
    for p in Permission.objects.filter(codename='query_export_review'):
        for g in p.group_set.all():
            auditors.extend(g.user_set.all())
    context = {'listAllClusterName': listAllClusterName, 'auditors': auditors}
    return render(request, 'query_export.html', context)


# SQL慢日志页面
@permission_required('sql.menu_slowquery', raise_exception=True)
def slowquery(request):
    # 获取用户关联实例列表
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]

    context = {'tab': 'slowquery', 'instances': instances}
    return render(request, 'slowquery.html', context)


# SQL优化工具页面
@permission_required('sql.menu_sqladvisor', raise_exception=True)
def sqladvisor(request):
    # 获取用户关联实例列表
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]

    context = {'instances': instances}
    return render(request, 'sqladvisor.html', context)


# 查询权限申请列表页面
@permission_required('sql.menu_queryapplylist', raise_exception=True)
def queryapplylist(request):
    user = request.user
    # 获取项目组
    group_list = user_groups(user)

    context = {'group_list': group_list}
    return render(request, 'queryapplylist.html', context)


# 查询权限申请详情页面
def queryapplydetail(request, apply_id):
    workflowDetail = QueryPrivilegesApply.objects.get(apply_id=apply_id)
    # 获取当前审批和审批流程
    audit_auth_group, current_audit_auth_group = Workflow.review_info(apply_id, 1)

    # 是否可审核
    is_can_review = Workflow.can_review(request.user, apply_id, 1)

    context = {'workflowDetail': workflowDetail, 'audit_auth_group': audit_auth_group,
               'current_audit_auth_group': current_audit_auth_group, 'is_can_review': is_can_review}
    return render(request, 'queryapplydetail.html', context)


# 用户的查询权限管理页面
def queryuserprivileges(request):
    # 获取所有用户
    user_list = QueryPrivileges.objects.filter(is_deleted=0).values('user_name').distinct()
    context = {'user_list': user_list}
    return render(request, 'queryuserprivileges.html', context)


# 会话管理页面
@permission_required('sql.menu_dbdiagnostic', raise_exception=True)
def dbdiagnostic(request):
    # 获取用户关联实例列表
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]

    context = {'tab': 'process', 'instances': instances}
    return render(request, 'dbdiagnostic.html', context)


@permission_required('sql.menu_database', raise_exception=True)
def database(request):
    # 获取用户关联实例列表
    instances = [ins.instance_name for ins in user_instances(request.user, 'all')]
    return render(request, 'database.html', {'instances': instances})


# 主从复制
@permission_required('sql.menu_instance', raise_exception=True)
def replication(request):
    ins_names = [ins.instance_name for ins in user_instances(request.user, 'master', 'mysql')]
    replication_info = list()
    for rep in Replication.objects.filter(Q(master__in=ins_names)|Q(slave__in=ins_names))[:10]:
        if rep.delay > 600:
            replication_info.append({"red": rep.created.strftime("%Y-%m-%d %H:%M:%S") + " " + rep.master + " -> " + rep.slave + " 延迟：" + str(rep.delay)})
        elif rep.delay > 300:
            replication_info.append({"yellow": rep.created.strftime("%Y-%m-%d %H:%M:%S") + " " + rep.master + " -> " + rep.slave + " 延迟：" + str(rep.delay)})
        else:
            replication_info.append({"green": rep.created.strftime("%Y-%m-%d %H:%M:%S") + " " + rep.master + " -> " + rep.slave + " 延迟：" + str(rep.delay)})
    return render(request, 'replication.html', locals())


@permission_required('sql.menu_instance', raise_exception=True)
def replication_echart(request):
    from pyecharts import Page, Line
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]
    begin_date = (datetime.datetime.now() - datetime.timedelta(minutes=+29))
    ins_name = request.GET.get('name', '')
    dt_s = request.GET.get('stime', begin_date)
    dt_e = request.GET.get('etime', datetime.datetime.now())

    attr = dict()
    if ins_name:
        for rep in Replication.objects.filter(Q(master=ins_name)|Q(slave=ins_name)).filter(created__range=[dt_s, dt_e]):
            if rep.master + "--" + rep.slave in attr:
                attr[rep.master + "--" + rep.slave].append([rep.delay, rep.created])
            else:
                attr[rep.master + "--" + rep.slave] = [[rep.delay, rep.created]]
    else:
        for rep in Replication.objects.filter(created__range=[dt_s, dt_e]):
            if rep.master + "--" + rep.slave in attr:
                attr[rep.master + "--" + rep.slave].append([rep.delay, rep.created])
            else:
                attr[rep.master + "--" + rep.slave] = [[rep.delay, rep.created]]
    page = Page()
    for k, v in attr.items():
        time_attr, value_attr = list(), list()
        for vv in v:
            value_attr.append(vv[0])
            time_attr.append(vv[1])
        line1 = Line("%s -> %s 同步延迟统计" % (k.split("--")[0], k.split("--")[1]), width="100%")
        line1.add("Seconds_Behind_Master", time_attr, value_attr, is_stack=False, legend_selectedmode='single', mark_point=["average"])
        page.add(line1)
    myechart = page.render_embed()  # 渲染配置
    host = 'https://pyecharts.github.io/assets/js'  # js文件源地址
    script_list = page.get_js_dependencies()  # 获取依赖的js文件名称（只获取当前视图需要的js）
    return render(request, "replication_echart.html", {"myechart": myechart, "host": host, "script_list": script_list, "instances": instances})


# 参数管理
@permission_required('sql.menu_param', raise_exception=True)
def param(request):
    # 获取用户关联实例列表
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]

    context = {'tab': 'param_tab', 'instances': instances}
    return render(request, 'param_setting.html', context)


def masking_field(request):
    return render(request, 'masking_field.html')


def query_audit(request):
    obj_list = user_instances(request.user, 'all')
    ins_name_list = [n.instance_name for n in obj_list]
    db_name_list = [db.db_name for db in DataBase.objects.filter(instance_name__in=ins_name_list)]
    return render(request, 'query_audit.html', locals())


def ip_white(request, instance_id):
    return render(request, "ip_white.html", {'instance_id': instance_id})


def host(request):
    return render(request, "host.html")


# 工作流审核列表页面
def workflows(request):
    return render(request, "workflow.html")


# 工作流审核详情页面
def workflowsdetail(request, audit_id):
    # 按照不同的workflow_type返回不同的详情
    auditInfo = Workflow.auditinfo(audit_id)
    if auditInfo.workflow_type == WorkflowDict.workflow_type['query']:
        return HttpResponseRedirect(reverse('sql:queryapplydetail', args=(auditInfo.workflow_id,)))
    elif auditInfo.workflow_type == WorkflowDict.workflow_type['sqlreview']:
        return HttpResponseRedirect(reverse('sql:detail', args=(auditInfo.workflow_id,)))


# 配置管理页面
@superuser_required
def config(request):
    # 获取所有项目组名称
    group_list = SqlGroup.objects.all()

    # 获取所有权限组
    auth_group_list = Group.objects.all()
    # 获取所有配置项
    all_config = Config.objects.all().values('item', 'value')
    sys_config = {}
    for items in all_config:
        sys_config[items['item']] = items['value']

    context = {'group_list': group_list, 'auth_group_list': auth_group_list,
               'config': sys_config, 'WorkflowDict': WorkflowDict}
    return render(request, 'config.html', context)


# 资源组管理页面
@superuser_required
def group(request):
    return render(request, 'group.html')


@superuser_required
def binlog(request):
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]
    return render(request, 'binlog.html', {'instances': instances})


@superuser_required
def backup(request):
    return render(request, 'backup.html')


@superuser_required
def backup_detail(request, db_cluster):
    return render(request, 'backup_detail.html', {'db_cluster': db_cluster})


# 资源组组关系管理页面
@superuser_required
def groupmgmt(request, group_id):
    group = SqlGroup.objects.get(group_id=group_id)
    return render(request, 'groupmgmt.html', {'group': group})


# 实例管理页面
@permission_required('sql.menu_instance', raise_exception=True)
def instance(request):
    return render(request, 'instance.html')


# 实例用户管理页面
@permission_required('sql.menu_instance', raise_exception=True)
def instanceuser(request, instance_id):
    return render(request, 'instanceuser.html', {'instance_id': instance_id})


# binlog2sql页面
@permission_required('sql.menu_binlog2sql', raise_exception=True)
def binlog2sql(request):
    # 获取实例列表
    instances = [instance.instance_name for instance in user_instances(request.user, 'all')]
    return render(request, 'binlog2sql.html', {'instances': instances})


# 数据库差异对比页面
@permission_required('sql.menu_schemasync', raise_exception=True)
def schemasync(request):
    # 获取实例列表
    instances = [instance.instance_name for instance in user_instances(request.user, 'master')]
    return render(request, 'schemasync.html', {'instances': instances})
