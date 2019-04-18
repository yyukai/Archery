# -*- coding: UTF-8 -*-
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

# Register your models here.
from .models import Users, Instance, SqlWorkflow, QueryLog, DataMaskingColumns, DataMaskingRules, \
    AliyunAccessKey, AliyunRdsConfig, ResourceGroup, ResourceGroupRelations, QueryPrivileges, \
    WorkflowAudit, WorkflowLog, Backup, ParamTemp, ParamHistory, Host, \
    DataBase, Replication, BGTable


# 用户管理
@admin.register(Users)
class UsersAdmin(UserAdmin):
    def __init__(self, *args, **kwargs):
        super(UserAdmin, self).__init__(*args, **kwargs)
        self.list_display = ('id', 'ding_user_id', 'username', 'display', 'email', 'is_superuser', 'is_staff', 'is_active')
        self.search_fields = ('id', 'username', 'display', 'email')

    def changelist_view(self, request, extra_context=None):
        # 这个方法在源码的admin/options.py文件的ModelAdmin这个类中定义，我们要重新定义它，以达到不同权限的用户，返回的表单内容不同
        if request.user.is_superuser:
            # 此字段定义UserChangeForm表单中的具体显示内容，并可以分类显示
            self.fieldsets = (
                ('认证信息', {'fields': ('username', 'password')}),
                ('个人信息', {'fields': ('ding_user_id', 'display', 'email')}),
                ('权限信息', {'fields': ('is_superuser', 'is_active', 'is_staff', 'groups', 'user_permissions')}),
                ('其他信息', {'fields': ('last_login', 'date_joined')}),
            )
            # 此字段定义UserCreationForm表单中的具体显示内容
            self.add_fieldsets = (
                (None, {'fields': ('username', 'display', 'email', 'password1', 'password2'), }),
            )
        return super(UserAdmin, self).changelist_view(request, extra_context)


# 资源组管理
@admin.register(ResourceGroup)
class ResourceGroupAdmin(admin.ModelAdmin):
    list_display = ('group_id', 'group_name', 'ding_webhook', 'is_deleted')
    exclude = ('group_parent_id', 'group_sort', 'group_level',)


# 资源组关系管理
@admin.register(ResourceGroupRelations)
class ResourceGroupRelationsAdmin(admin.ModelAdmin):
    list_display = ('object_type', 'object_id', 'object_name', 'group_id', 'group_name', 'create_time')


# 实例管理
@admin.register(Instance)
class InstanceAdmin(admin.ModelAdmin):
    list_display = ('id', 'instance_name', 'db_type', 'type', 'host', 'port', 'user', 'password', 'osn', 'create_time')
    search_fields = ['instance_name', 'host', 'port', 'user']
    list_filter = ('db_type', 'type',)


@admin.register(Replication)
class ReplicationAdmin(admin.ModelAdmin):
    list_display = ('id', 'master', 'slave', 'delay', 'created')


# 工单管理
@admin.register(SqlWorkflow)
class SqlWorkflowAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'workflow_name', 'group_name', 'instance_name', 'engineer_display', 'create_time', 'status', 'is_backup')
    search_fields = ['id', 'workflow_name', 'engineer_display', 'sql_content']
    list_filter = ('group_name', 'instance_name', 'status', 'sql_syntax',)


# SQL查询日志
@admin.register(QueryLog)
class QueryLogAdmin(admin.ModelAdmin):
    list_display = (
        'instance_name', 'db_name', 'sqllog', 'effect_row', 'cost_time', 'user_display', 'create_time')
    search_fields = ['sqllog', 'user_display']
    list_filter = ('instance_name', 'db_name', 'user_display', 'priv_check', 'hit_rule', 'masking',)


# 查询权限记录
@admin.register(QueryPrivileges)
class QueryPrivilegesAdmin(admin.ModelAdmin):
    list_display = (
        'user_display', 'instance_name', 'db_name', 'table_name', 'valid_date', 'limit_num', 'create_time')
    search_fields = ['user_display', 'instance_name']
    list_filter = ('user_display', 'instance_name', 'db_name', 'table_name',)


# 脱敏字段页面定义
@admin.register(DataMaskingColumns)
class DataMaskingColumnsAdmin(admin.ModelAdmin):
    list_display = (
        'column_id', 'rule_type', 'active', 'instance_name', 'table_schema', 'table_name', 'column_name',
        'create_time',)
    search_fields = ['table_name', 'column_name']
    list_filter = ('rule_type', 'active', 'instance_name')


# 脱敏规则页面定义
@admin.register(DataMaskingRules)
class DataMaskingRulesAdmin(admin.ModelAdmin):
    list_display = (
        'rule_type', 'rule_regex', 'hide_group', 'rule_desc', 'sys_time',)


# 工作流审批列表
@admin.register(WorkflowAudit)
class WorkflowAuditAdmin(admin.ModelAdmin):
    list_display = (
        'workflow_title', 'group_name', 'workflow_type', 'current_status', 'create_user_display', 'create_time')
    search_fields = ['workflow_title', 'create_user_display']
    list_filter = ('create_user_display', 'group_name', 'workflow_type', 'current_status')


# 工作流日志表
@admin.register(WorkflowLog)
class WorkflowLogAdmin(admin.ModelAdmin):
    list_display = (
        'operation_type_desc', 'operation_info', 'operator_display', 'operation_time',)
    list_filter = ('operation_type_desc', 'operator_display')


# 阿里云的认证信息
@admin.register(AliyunAccessKey)
class AliAccessKeyAdmin(admin.ModelAdmin):
    list_display = ('ak', 'secret', 'is_enable', 'remark',)
    search_fields = ['ak']


# 阿里云实例配置信息
@admin.register(AliyunRdsConfig)
class AliRdsConfigAdmin(admin.ModelAdmin):
    list_display = ('instance_name', 'rds_dbinstanceid', 'is_enable')
    search_fields = ['instance_name', 'rds_dbinstanceid']


# 备份管理
@admin.register(Backup)
class BackupAdmin(admin.ModelAdmin):
    list_display = ("bk_ip", "db_cluster", "db_type", "bk_path", "bk_size", "bk_state", "check_man", "bk_start_time",
                    "bk_end_time", "create_time")


@admin.register(ParamTemp)
class ParamTempAdmin(admin.ModelAdmin):
    list_display = ("db_type", "param", "default_var", "is_reboot", "available_var", "description")


@admin.register(ParamHistory)
class ParamHistoryAdmin(admin.ModelAdmin):
    list_display = ("instance", "param", "old_var", "new_var", "is_active", "create_time")


@admin.register(Host)
class HostAdmin(admin.ModelAdmin):
    list_display = ("ip", "memory", "cpu", "type", "create_time", "update_time")


@admin.register(DataBase)
class DatabaseAdmin(admin.ModelAdmin):
    list_display = ("host", "ip", "port", "instance_name", "db_name", "app_type", "db_application", "db_person",
                    "create_time", "update_time")


@admin.register(BGTable)
class BGTableAdmin(admin.ModelAdmin):
    list_display = ("db_name", "table_name", "create_time")
