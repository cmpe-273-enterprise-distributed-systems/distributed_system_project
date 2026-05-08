-- 1. USERS
CREATE TABLE IF NOT EXISTS web_app.users (
    user_id uuid PRIMARY KEY,
    username text,
    email text,
    password_hash text,
    role text,
    created_at timestamp
);

-- Allow backend to query: SELECT * FROM users WHERE email = '...'
CREATE INDEX IF NOT EXISTS users_email_idx ON web_app.users (email);

-- 2. USER CHAT HISTORY
-- Used by the Client Chat Screen to load their personal history
CREATE TABLE IF NOT EXISTS web_app.requests (
    user_id uuid,
    created_at timestamp,
    request_id uuid,
    prompt text,
    response text,
    worker_node text,
    duration text,
    status text,
    PRIMARY KEY (user_id, created_at, request_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- 3. ADMIN DASHBOARD HISTORY
-- Used by the Admin Dashboard to see recent activity across ALL nodes
-- We group requests by a time_bucket (e.g., '2026-05') to keep partitions manageable
CREATE TABLE IF NOT EXISTS web_app.cluster_requests_by_month (
    time_bucket text, 
    created_at timestamp,
    request_id uuid,
    user_id uuid,
    username text,
    prompt text,
    worker_node text,
    duration text,
    status text,
    PRIMARY KEY (time_bucket, created_at, request_id)
) WITH CLUSTERING ORDER BY (created_at DESC);
