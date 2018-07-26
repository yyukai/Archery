import re

from sql.const import Const
from sql.models import MasterConfig, SqlWorkflow
from sql.utils.aes_decryptor import Prpcrypt
from sql.utils.config import SysConfig
from sql.utils.group import user_groups
from sql.utils.inception import InceptionDao


# 获取当前请求url
def getDetailUrl(request):
    scheme = request.scheme
    host = request.META['HTTP_HOST']
    return "%s://%s/detail/" % (scheme, host)


# 根据实例名获取主库连接字符串，并封装成一个dict
def getMasterConnStr(clusterName):
    listMasters = MasterConfig.objects.filter(cluster_name=clusterName)

    masterHost = listMasters[0].master_host
    masterPort = listMasters[0].master_port
    masterUser = listMasters[0].master_user
    masterPassword = Prpcrypt().decrypt(listMasters[0].master_password)
    dictConn = {'masterHost': masterHost, 'masterPort': masterPort, 'masterUser': masterUser,
                'masterPassword': masterPassword}
    return dictConn


# 判断SQL上线是否无需审批
def is_autoreview(workflowid):
    workflowDetail = SqlWorkflow.objects.get(id=workflowid)
    sql_content = workflowDetail.sql_content
    cluster_name = workflowDetail.cluster_name
    db_name = workflowDetail.db_name
    is_manual = workflowDetail.is_manual

    # 删除注释语句
    sql_content = ''.join(
        map(lambda x: re.compile(r'(^--\s+.*|^/\*.*\*/;\s*$)').sub('', x, count=1),
            sql_content.splitlines(1))).strip()

    # 获取正则表达式
    auto_review_regex = SysConfig().sys_config.get('auto_review_regex',
                                                   '^alter|^create|^drop|^truncate|^rename|^delete')
    p = re.compile(auto_review_regex)

    # 判断是否匹配到需要手动审核的语句
    is_autoreview = True
    for row in sql_content.strip(';').split(';'):
        if p.match(row.strip().lower()):
            is_autoreview = False
            break
        if is_autoreview:
            # 更新影响行数加测,单条更新语句影响行数超过指定数量则需要人工审核
            inception_review = InceptionDao().sqlautoReview(sql_content, cluster_name, db_name)
            for review_result in inception_review:
                SQL = review_result[5]
                Affected_rows = review_result[6]
                if re.match(r"^update", SQL.strip().lower()):
                    if int(Affected_rows) > int(SysConfig().sys_config.get('auto_review_max_update_rows', 0)):
                        is_autoreview = False
                        break
    # inception不支持语法都需要审批
    if is_manual == 1:
        is_autoreview = False
    return is_autoreview


# 判断用户当前是否可执行
def can_execute(user, workflow_id):
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # 只有审核通过和定时执行的数据才可以立即执行
    if workflow_detail.status in [Const.workflowStatus['pass'], Const.workflowStatus['timingtask']]:
        # 当前登录用户必须为有执行权限的组内用户
        group_ids = [group.group_id for group in user_groups(user)]
        if workflow_detail.group_id in group_ids and user.has_perm('sql.sql_execute'):
            return True
    return result


# 判断用户当前是否可定时执行
def can_timingtask(user, workflow_id):
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # 只有审核通过和定时执行的数据才可以执行
    if workflow_detail.status in [Const.workflowStatus['pass'], Const.workflowStatus['timingtask']]:
        # 当前登录用户必须为有执行权限的组内用户
        group_ids = [group.group_id for group in user_groups(user)]
        if workflow_detail.group_id in group_ids and user.has_perm('sql.sql_execute'):
            result = True
    return result


# 判断用户当前是否是可终止
def can_cancel(user, workflow_id):
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # 结束的工单不可终止
    if workflow_detail.status == Const.workflowStatus['manreviewing']:
        from sql.utils.workflow import Workflow
        if Workflow.can_review(user, workflow_id, 2) or user.username == workflow_detail.engineer:
            result = True
    elif workflow_detail.status in [Const.workflowStatus['pass'], Const.workflowStatus['timingtask']]:
        # 当前登录用户必须为有审核权限的组内用户
        group_ids = [group.group_id for group in user_groups(user)]
        if workflow_detail.group_id in group_ids and user.has_perm('sql.sql_review'):
            result = True
    return result
