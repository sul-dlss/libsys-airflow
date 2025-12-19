SELECT jsonb->'selectField'->'options'->'values'
FROM sul_mod_users.custom_fields 
WHERE jsonb->>'name' = 'Usergroup';
