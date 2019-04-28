-- 增加权限
set @content_type_id=(select id from django_content_type where app_label='sql' and model='permission');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('菜单 参数配置', @content_type_id, 'menu_param');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('查看实例参数列表', @content_type_id, 'param_view');
INSERT INTO auth_permission (name, content_type_id, codename) VALUES ('修改实例参数', @content_type_id, 'param_edit');


-- 用户名和密码增加默认值
ALTER TABLE sql_instance
  MODIFY `user` VARCHAR(200) NOT NULL DEFAULT '' COMMENT '用户名',
  MODIFY `password` VARCHAR(200)  NOT NULL DEFAULT '' COMMENT '密码';

-- 用户权限表增加索引
ALTER TABLE query_privileges
  ADD INDEX  idx_user_name_instance_id_db_name_valid_date(user_name,instance_id,db_name,valid_date);

-- 实例参数配置功能
CREATE TABLE param_template(
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
  db_type VARCHAR(10) NOT NULL COMMENT '数据库类型，mysql、mssql、redis、pgsql',
  variable_name VARCHAR(64) NOT NULL COMMENT '参数名',
  default_value VARCHAR(1024) NOT NULL COMMENT '默认参数值',
  editable TINYINT NOT NULL COMMENT '是否支持修改',
  valid_values VARCHAR(1024) NOT NULL COMMENT '有效参数值，',
  description VARCHAR(1024) NOT NULL COMMENT '参数描述',
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  sys_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  UNIQUE uniq_db_type_variable_name(db_type, variable_name)
) COMMENT '实例参数配置表';



CREATE TABLE param_history(
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ,
  instance_id INT NOT NULL COMMENT '实例ID',
  variable_name VARCHAR(64) NOT NULL COMMENT '参数名',
  old_var VARCHAR(1024) NOT NULL COMMENT '修改前参数值',
  new_var VARCHAR(1024) NOT NULL COMMENT '修改后参数值',
  set_sql VARCHAR(1024) NOT NULL COMMENT '在线变更配置执行的SQL语句',
  user_name VARCHAR(30) NOT NULL COMMENT '修改人',
  user_display VARCHAR(50) NOT NULL COMMENT '修改人中文名',
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  INDEX idx_variable_name(variable_name),
  CONSTRAINT fk_param_instance FOREIGN KEY fk_param_instance (instance_id) REFERENCES sql_instance (id) ON DELETE RESTRICT ON UPDATE RESTRICT
) COMMENT '实例参数修改历史';
