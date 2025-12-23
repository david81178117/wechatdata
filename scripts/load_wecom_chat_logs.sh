#!/usr/bin/env bash
set -euo pipefail

JSONL_PATH="${JSONL_PATH:-/opt/wecom/decrypted_database_ready.jsonl}"

if [[ ! -f "$JSONL_PATH" ]]; then
  echo "JSONL file not found: $JSONL_PATH" >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" && -z "${PGHOST:-}" ]]; then
  echo "Database connection not set. Use DATABASE_URL or PG* env vars." >&2
  exit 1
fi

PSQL_BASE=(psql -v ON_ERROR_STOP=1)

if [[ -n "${DATABASE_URL:-}" ]]; then
  PSQL_BASE+=("$DATABASE_URL")
fi

"${PSQL_BASE[@]}" -c "
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
"

python3 /workspaces/wechatdata/scripts/wecom_etl.py --jsonl "$JSONL_PATH" --conflict update
