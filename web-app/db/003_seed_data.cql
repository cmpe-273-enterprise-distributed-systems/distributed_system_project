INSERT INTO web_app.users (
    user_id,
    username,
    email,
    password_hash,
    role,
    created_at
) VALUES (
    uuid(),
    'Admin',
    'admin@cluster.local',
    'admin',
    'admin',
    toTimestamp(now())
);
