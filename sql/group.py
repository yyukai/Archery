# -*- coding: UTF-8 -*-
import logging
import traceback

import simplejson as json
from django.contrib.auth.models import Group
from django.db.models import F
from django.http import HttpResponse

from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.permission import superuser_required
from sql.models import SqlGroup, GroupRelations, Users, Instance
from sql.utils.workflow import Workflow

logger = logging.getLogger('default')


# 获取资源组列表
@superuser_required
def group(request):
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit
    search = request.POST.get('search', '')

    # 全部工单里面包含搜索条件
    group_list = SqlGroup.objects.filter(group_name__contains=search)[offset:limit].values("group_id",
                                                                                           "group_name",
                                                                                           "ding_webhook")
    group_count = SqlGroup.objects.filter(group_name__contains=search).count()

    # QuerySet 序列化
    rows = [row for row in group_list]

    result = {"total": group_count, "rows": rows}
    # 返回查询结果
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
                        content_type='application/json')


# 获取资源组已关联对象信息
def associated_objects(request):
    '''
    type：(0, '用户'), (1, '实例')
    '''
    group_id = int(request.POST.get('group_id'))
    object_type = request.POST.get('type')
    limit = int(request.POST.get('limit'))
    offset = int(request.POST.get('offset'))
    limit = offset + limit
    search = request.POST.get('search', '')

    if object_type:
        rows = GroupRelations.objects.filter(group_id=group_id, object_type=object_type, object_name__contains=search)[
               offset:limit].values('id', 'object_id', 'object_name', 'group_id', 'group_name', 'object_type',
                                    'create_time')
        count = GroupRelations.objects.filter(group_id=group_id, object_type=object_type,
                                              object_name__contains=search).count()
    else:
        rows = GroupRelations.objects.filter(group_id=group_id, object_name__contains=search)[offset:limit].values(
            'id', 'object_id', 'object_name', 'group_id', 'group_name', 'object_type', 'create_time')
        count = GroupRelations.objects.filter(group_id=group_id, object_name__contains=search).count()
    rows = [row for row in rows]
    result = {'status': 0, 'msg': 'ok', "total": count, "rows": rows}
    return HttpResponse(json.dumps(result, cls=ExtendJSONEncoder), content_type='application/json')


# 获取资源组未关联对象信息
def unassociated_objects(request):
    """
    type：(0, '用户'), (1, '实例')
    """
    group_id = int(request.POST.get('group_id'))
    object_type = int(request.POST.get('object_type'))

    associated_object_ids = [object_id['object_id'] for object_id in
                             GroupRelations.objects.filter(group_id=group_id,
                                                           object_type=object_type).values('object_id')]

    if object_type == 0:
        unassociated_objects = Users.objects.exclude(pk__in=associated_object_ids
                                                     ).annotate(object_id=F('pk'),
                                                                object_name=F('display')
                                                                ).values('object_id', 'object_name')
    elif object_type == 1:
        unassociated_objects = Instance.objects.exclude(pk__in=associated_object_ids
                                                        ).annotate(object_id=F('pk'),
                                                                   object_name=F('instance_name')
                                                                   ).values('object_id', 'object_name')
    else:
        unassociated_objects = []

    rows = [row for row in unassociated_objects]

    result = {'status': 0, 'msg': 'ok', "rows": rows, "total": len(rows)}
    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取资源组关联实例列表
def instances(request):
    group_name = request.POST.get('group_name')
    group_id = SqlGroup.objects.get(group_name=group_name).group_id
    type = request.POST.get('type')
    # 先获取资源组关联所有实例列表
    instance_ids = [group['object_id'] for group in
                    GroupRelations.objects.filter(group_id=group_id, object_type=1).values('object_id')]

    # 获取实例信息
    instances_ob = Instance.objects.filter(pk__in=instance_ids, type=type).values('id', 'instance_name')
    rows = [row for row in instances_ob]
    result = {'status': 0, 'msg': 'ok', "data": rows}
    return HttpResponse(json.dumps(result), content_type='application/json')


# 添加资源组关联对象
@superuser_required
def addrelation(request):
    """
    type：(0, '用户'), (1, '实例')
    """
    group_id = int(request.POST.get('group_id'))
    object_type = request.POST.get('object_type')
    object_list = json.loads(request.POST.get('object_info'))
    group_name = SqlGroup.objects.get(group_id=group_id).group_name
    try:
        GroupRelations.objects.bulk_create(
            [GroupRelations(object_id=int(object.split(',')[0]),
                            object_type=object_type,
                            object_name=object.split(',')[1],
                            group_id=group_id,
                            group_name=group_name) for object in object_list])
        result = {'status': 0, 'msg': 'ok'}
    except Exception as e:
        logger.error(traceback.format_exc())
        result = {'status': 1, 'msg': str(e)}
    return HttpResponse(json.dumps(result), content_type='application/json')


# 获取资源组的审批流程
def auditors(request):
    group_name = request.POST.get('group_name')
    workflow_type = request.POST['workflow_type']
    result = {'status': 0, 'msg': 'ok', 'data': {'auditors': '', 'auditors_display': ''}}
    if group_name:
        group_id = SqlGroup.objects.get(group_name=group_name).group_id
        audit_auth_groups = Workflow.audit_settings(group_id=group_id, workflow_type=workflow_type)
    else:
        result['status'] = 1
        result['msg'] = '参数错误'
        return HttpResponse(json.dumps(result), content_type='application/json')

    # 获取权限组名称
    if audit_auth_groups:
        # 校验配置
        for auth_group_id in audit_auth_groups.split(','):
            try:
                Group.objects.get(id=auth_group_id)
            except Exception:
                result['status'] = 1
                result['msg'] = '审批流程权限组不存在，请重新配置！'
                return HttpResponse(json.dumps(result), content_type='application/json')
        audit_auth_groups_name = '->'.join(
            [Group.objects.get(id=auth_group_id).name for auth_group_id in audit_auth_groups.split(',')])
        result['data']['auditors'] = audit_auth_groups
        result['data']['auditors_display'] = audit_auth_groups_name

    return HttpResponse(json.dumps(result), content_type='application/json')


# 资源组审批流程配置
@superuser_required
def changeauditors(request):
    auth_groups = request.POST.get('audit_auth_groups')
    group_name = request.POST.get('group_name')
    workflow_type = request.POST.get('workflow_type')
    result = {'status': 0, 'msg': 'ok', 'data': []}

    # 调用工作流修改审核配置
    group_id = SqlGroup.objects.get(group_name=group_name).group_id
    audit_auth_groups = [str(Group.objects.get(name=auth_group).id) for auth_group in auth_groups.split(',')]
    try:
        Workflow.change_settings(group_id, workflow_type, ','.join(audit_auth_groups))
    except Exception as msg:
        logger.error(traceback.format_exc())
        result['msg'] = str(msg)
        result['status'] = 1

    # 返回结果
    return HttpResponse(json.dumps(result), content_type='application/json')
