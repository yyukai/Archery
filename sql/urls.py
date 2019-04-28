# -*- coding: UTF-8 -*- 

from django.urls import path

from common import auth, config, workflow, dashboard, check
from sql import views, sql_workflow, query, slowlog, instance, db_diagnostic, sql_tuning, group, \
    sql_advisor, binlog2sql, soar
from sql.utils import jobs

urlpatterns = [
    path('', views.sqlworkflow),
    path('index/', views.sqlworkflow),
    path('login/', views.login, name='login'),
    path('logout/', auth.sign_out),
    path('signup/', auth.sign_up),
    path('sqlworkflow/', views.sqlworkflow),
    path('submitsql/', views.submit_sql),
    path('editsql/', views.submit_sql),
    path('submitotherinstance/', views.submit_sql),
    path('detail/<int:workflow_id>/', views.detail, name='detail'),
    path('autoreview/', sql_workflow.autoreview),
    path('passed/', sql_workflow.passed),
    path('execute/', sql_workflow.execute),
    path('timingtask/', sql_workflow.timingtask),
    path('cancel/', sql_workflow.cancel),
    path('rollback/', views.rollback),
    path('sqlquery/', views.sqlquery),
    path('slowquery/', views.slowquery),
    path('sqladvisor/', views.sqladvisor),
    path('slowquery_advisor/', views.sqladvisor),
    path('queryapplylist/', views.queryapplylist),
    path('queryapplydetail/<int:apply_id>/', views.queryapplydetail, name='queryapplydetail'),
    path('queryuserprivileges/', views.queryuserprivileges),
    path('dbdiagnostic/', views.dbdiagnostic),
    path('workflow/', views.workflows),
    path('workflow/<int:audit_id>/', views.workflowsdetail),
    path('dbaprinciples/', views.dbaprinciples),
    path('dashboard/', dashboard.pyecharts),
    path('group/', views.group),
    path('grouprelations/<int:group_id>/', views.groupmgmt),
    path('instance/', views.instance),
    path('instanceuser/<int:instance_id>/', views.instanceuser),
    path('binlog2sql/', views.binlog2sql),
    path('schemasync/', views.schemasync),
    path('config/', views.config),

    path('authenticate/', auth.authenticate_entry),
    path('sqlworkflow_list/', sql_workflow.sqlworkflow_list),
    path('simplecheck/', sql_workflow.simplecheck),
    path('getOscPercent/', sql_workflow.get_osc_percent),
    path('getWorkflowStatus/', sql_workflow.get_workflow_status),
    path('stopOscProgress/', sql_workflow.stop_osc_progress),
    path('del_sqlcronjob/', jobs.del_sqlcronjob),

    path('workflow/list/', workflow.lists),
    path('workflow/log/', workflow.log),
    path('config/change/', config.changeconfig),

    path('check/inception/', check.inception),
    path('check/email/', check.email),
    path('check/instance/', check.instance),

    path('group/group/', group.group),
    path('group/addrelation/', group.addrelation),
    path('group/relations/', group.associated_objects),
    path('group/instances/', group.instances),
    path('group/unassociated/', group.unassociated_objects),
    path('group/auditors/', group.auditors),
    path('group/changeauditors/', group.changeauditors),

    path('instance/list/', instance.lists),
    path('instance/users/', instance.users),
    path('instance/schemasync/', instance.schemasync),
    path('instance/getdbNameList/', instance.get_db_name_list),
    path('instance/getTableNameList/', instance.get_table_name_list),
    path('instance/getColumnNameList/', instance.get_column_name_list),

    path('query/', query.query),
    path('query/querylog/', query.querylog),
    path('query/explain/', query.explain),
    path('query/applylist/', query.getqueryapplylist),
    path('query/userprivileges/', query.getuserprivileges),
    path('query/applyforprivileges/', query.applyforprivileges),
    path('query/modifyprivileges/', query.modifyqueryprivileges),
    path('query/privaudit/', query.queryprivaudit),

    path('binlog2sql/sql/', binlog2sql.binlog2sql),
    path('binlog2sql/binlog_list/', binlog2sql.binlog_list),

    path('slowquery/review/', slowlog.slowquery_review),
    path('slowquery/review_history/', slowlog.slowquery_review_history),
    path('slowquery/sqladvisor/', sql_advisor.sqladvisor),
    path('slowquery/sqltuning/', sql_tuning.tuning),
    path('slowquery/soar/', soar.soar),

    path('db_diagnostic/process/', db_diagnostic.process),
    path('db_diagnostic/create_kill_session/', db_diagnostic.create_kill_session),
    path('db_diagnostic/kill_session/', db_diagnostic.kill_session),
    path('db_diagnostic/tablesapce/', db_diagnostic.tablesapce),
    path('db_diagnostic/trxandlocks/', db_diagnostic.trxandlocks),
]
