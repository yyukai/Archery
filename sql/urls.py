# -*- coding: UTF-8 -*- 

from django.urls import path
from django.views.i18n import JavaScriptCatalog

import sql.query_privileges
import sql.sql_optimize
from common import auth, config, workflow, dashboard, check
from sql import views, sql_workflow, sql_analyze, query, slowlog, instance, db_diagnostic, resource_group, binlog, \
    backup, data_safe, query_audit, api, host, database, data_dictionary, wpan_upload, bg_table, tools
from sql.utils import tasks

urlpatterns = [
    path('', views.sqlworkflow),
    path('jsi18n/', JavaScriptCatalog.as_view(), name='javascript-catalog'),
    path('index/', views.index),
    path('login/', views.login, name='login'),
    path('logout/', auth.sign_out),
    path('signup/', auth.sign_up),
    path('sqlworkflow/', views.sqlworkflow),
    path('submitsql/', views.submit_sql),
    path('editsql/', views.submit_sql),
    path('submitotherinstance/', views.submit_sql),
    path('detail/<int:workflow_id>/', views.detail, name='detail'),
    path('autoreview/', sql_workflow.submit),
    path('passed/', sql_workflow.passed),
    path('execute/', sql_workflow.execute),
    path('timingtask/', sql_workflow.timing_task),
    path('cancel/', sql_workflow.cancel),
    path('rollback/', views.rollback),
    path('sqlanalyze/', views.sqlanalyze),
    path('sqlquery/', views.sqlquery),
    path('query_export/', views.query_export),
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
    path('instance/host/<ip>/', views.instance),
    path('instanceuser/<int:instance_id>/', views.instanceuser),
    path('instanceparam/', views.instance_param),
    path('binlog2sql/', views.binlog2sql),
    path('schemasync/', views.schemasync),
    path('config/', views.config),

    path('authenticate/', auth.authenticate_entry),
    path('sqlworkflow_list/', sql_workflow.sql_workflow_list),
    path('simplecheck/', sql_workflow.check),
    path('getWorkflowStatus/', sql_workflow.get_workflow_status),
    path('del_sqlcronjob/', tasks.del_schedule),
    path('inception/osc_control/', sql_workflow.osc_control),

    path('sql_analyze/generate/', sql_analyze.generate),
    path('sql_analyze/analyze/', sql_analyze.analyze),

    path('workflow/list/', workflow.lists),
    path('workflow/log/', workflow.log),
    path('config/change/', config.change_config),

    path('check/inception/', check.inception),
    path('check/go_inception/', check.go_inception),
    path('check/email/', check.email),
    path('check/instance/', check.instance),

    path('group/group/', resource_group.group),
    path('group/addrelation/', resource_group.addrelation),
    path('group/relations/', resource_group.associated_objects),
    path('group/instances/', resource_group.instances),
    path('group/unassociated/', resource_group.unassociated_objects),
    path('group/auditors/', resource_group.auditors),
    path('group/changeauditors/', resource_group.changeauditors),

    path('instance/list/', instance.lists),
    path('instance/users/', instance.users),
    path('instance/schemasync/', instance.schemasync),
    path('instance/instance_resource/', instance.instance_resource),
    path('instance/describetable/', instance.describe),

    path('api/instance/edit/', api.api_instance_edit),
    path('api/host/edit/', api.api_host_edit),

    path('database/', views.database),
    path('database_list/', database.db_list),
    path('database_detail/', database.db_detail),

    path('data_dictionary/', views.data_dictionary),
    path('data_dictionary/table_list/', data_dictionary.table_list),
    path('data_dictionary/table_info/', data_dictionary.table_info),

    path('bg_table/', views.bg_table),
    path('bg_table_list/', bg_table.bg_table_list),

    # path('redis/', views.redis),
    # path('redis_query/', redis.redis_query),
    # path('redis_apply/', views.redis_apply),
    # path('redis_apply_list/', redis.redis_apply_list),
    # path('redis_apply_audit/', redis.redis_apply_audit),

    path('replication/', views.replication),
    path('replication_echart/', views.replication_echart),
    path('replication_delay/', database.replication_delay),

    path('param/list/', instance.param_list),
    path('param/history/', instance.param_history),
    path('param/edit/', instance.param_edit),

    path('query/', query.query),
    path('query/querylog/', query.querylog),
    path('add_async_query/', query.add_async_query),
    path('query_result_export/', query.query_result_export),
    path('query_export_audit/', query.query_export_audit),
    path('query_export_cancel/', query.query_export_cancel),
    path('query/query_export_log/', query.query_export_log),
    path('query/explain/', sql.sql_optimize.explain),
    path('query/applylist/', sql.query_privileges.query_priv_apply_list),
    path('query/userprivileges/', sql.query_privileges.user_query_priv),
    path('query/applyforprivileges/', sql.query_privileges.query_priv_apply),
    path('query/modifyprivileges/', sql.query_privileges.query_priv_modify),
    path('query/privaudit/', sql.query_privileges.query_priv_audit),

    path('binlog/', views.binlog),
    path('binlog/list/', binlog.binlog_list),
    path('binlog/list_total/', binlog.binlog_list_total),
    path('binlog/binlog2sql/', binlog.binlog2sql),
    path('binlog/del_log/', binlog.del_binlog),

    path('slowquery/review/', slowlog.slowquery_review),
    path('slowquery/review_history/', slowlog.slowquery_review_history),
    path('slowquery/optimize_sqladvisor/', sql.sql_optimize.optimize_sqladvisor),
    path('slowquery/optimize_sqltuning/', sql.sql_optimize.optimize_sqltuning),
    path('slowquery/optimize_soar/', sql.sql_optimize.optimize_soar),

    path('db_diagnostic/process/', db_diagnostic.process),
    path('db_diagnostic/create_kill_session/', db_diagnostic.create_kill_session),
    path('db_diagnostic/kill_session/', db_diagnostic.kill_session),
    path('db_diagnostic/tablesapce/', db_diagnostic.tablesapce),
    path('db_diagnostic/trxandlocks/', db_diagnostic.trxandlocks),

    path('backup/', views.backup),
    path('backup/list/', backup.backup_list),
    path('backup_detail/<db_cluster>/', views.backup_detail),
    path('backup_detail/list/<db_cluster>/', backup.backup_detail_list),

    path('masking_field/', views.masking_field),
    path('masking_field/list/', data_safe.masking_field_list),

    path('query_audit/', views.query_audit),
    path('query_audit/list/', query_audit.query_audit),

    path('host/', views.host),
    path('host/list/', host.host_list),

    path('wpan_upload/', views.wpan_upload),
    path('wpan_upload/dir_list/', wpan_upload.wpan_upload_dir_list),
    path('wpan_upload/file_list/', wpan_upload.wpan_upload_list),
    path('wpan_upload/file_cont/', wpan_upload.wpan_upload_file_cont),
    path('wpan_upload/apply/', wpan_upload.wpan_upload_apply),
    path('wpan_upload/download/', wpan_upload.wpan_upload_download),
    path('wpan_upload_audit/', views.wpan_upload_audit),
    path('wpan_upload_audit/audit/', wpan_upload.wpan_upload_audit),
    path('wpan_upload_audit/cancel/', wpan_upload.wpan_upload_cancel),

    path('tools/loan_update/', tools.tools_loan_update),
    path('tools/loan_update/search/', tools.loan_update_search),
    path('tools/loan_update/apply/', tools.loan_update_apply),
    path('tools/loan_update_audit/', tools.tools_loan_update_audit),
    path('tools/loan_update_audit/list/', tools.loan_update_audit_list),
    path('tools/loan_update/edit/', tools.loan_update_audit),
    path('tools/loan_update/pic_list/', tools.loan_update_pic_list),
    path('tools/loan_update/audit/', tools.loan_update_audit),
]
