CREATE TABLE IF NOT EXISTS wecom_chat_logs (
    msgid VARCHAR(255) PRIMARY KEY,
    action VARCHAR(50),
    msgtype VARCHAR(50),
    msgtime TIMESTAMP WITH TIME ZONE,
    from_user_id VARCHAR(255),
    roomid VARCHAR(255),
    tolist TEXT[],
    content_payload JSONB,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_logs_from ON wecom_chat_logs(from_user_id);
CREATE INDEX IF NOT EXISTS idx_logs_room ON wecom_chat_logs(roomid);
CREATE INDEX IF NOT EXISTS idx_logs_time ON wecom_chat_logs(msgtime);
CREATE INDEX IF NOT EXISTS idx_logs_payload ON wecom_chat_logs USING gin (content_payload);
