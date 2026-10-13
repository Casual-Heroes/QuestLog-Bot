-- Bind automatic role grants to the administrator who approved them.
-- Existing NULL rows fail closed until an administrator reconfigures them.
ALTER TABLE react_roles
    ADD COLUMN IF NOT EXISTS approved_by BIGINT NULL AFTER role_name;

ALTER TABLE welcome_configs
    ADD COLUMN IF NOT EXISTS auto_role_approved_by BIGINT NULL AFTER auto_role_id;
