-- 表结构变更
ALTER TABLE sql_group
  ADD ding_webhook VARCHAR(255) NOT NULL DEFAULT ''
  AFTER group_level;
ALTER TABLE sql_workflow
  ADD engineer_display VARCHAR(50) NOT NULL DEFAULT ''
  AFTER engineer;
ALTER TABLE workflow_audit
  ADD create_user_display VARCHAR(50) NOT NULL DEFAULT ''
  AFTER create_user;
ALTER TABLE query_privileges_apply
  ADD user_display VARCHAR(50) NOT NULL DEFAULT ''
  AFTER user_name;
ALTER TABLE query_privileges
  ADD user_display VARCHAR(50) NOT NULL DEFAULT ''
  AFTER user_name;
ALTER TABLE query_log
  ADD user_display VARCHAR(50) NOT NULL DEFAULT ''
  AFTER username;

-- 数据清洗
UPDATE sql_workflow, sql_users
SET engineer_display = display
WHERE engineer = username;
UPDATE workflow_audit, sql_users
SET create_user_display = display
WHERE create_user = username;
UPDATE query_privileges_apply, sql_users
SET user_display = display
WHERE user_name = username;
UPDATE query_privileges, sql_users
SET user_display = display
WHERE user_name = username;
UPDATE query_log, sql_users
SET user_display = display
WHERE query_log.username = sql_users.username;


ALTER TABLE sql_users
  ADD user_id VARCHAR(50) NULL comment ''
  AFTER id;
-- CREATE TABLE ding_config (
--   `agent_id` varchar(20) NOT NULL,
--   `corp_id` varchar(30) NOT NULL,
--   `corp_secret` varchar(100) NOT NULL,
--   `access_token` varchar(50) NULL,
--   `expires_in` int(11) NULL
-- );

INSERT INTO sql_config(item,value) VALUES
('ding_agent_id', '184600001'),
('ding_corp_id', 'ding19d30468566deffd'),
('ding_corp_secret', 'l02Eg6CcSPbSGO6Il8obQJ_HD2NCGLnoBNc4jmnvXj1aiF4THJV0MW8T-2ihjYe7'),
('ding_root_dept_id', 2925013),
('ding_access_token', ''),
('ding_expires_time', 0)
;
