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
    '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918',
    'admin',
    toTimestamp(now())
);
