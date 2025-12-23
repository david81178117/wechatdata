#!/usr/bin/env bash
set -euo pipefail

JSONL_PATH="${JSONL_PATH:-/workspaces/wechatdata/decrypted_database_ready.jsonl}"

if [[ ! -f "$JSONL_PATH" ]]; then
  echo "JSONL file not found: $JSONL_PATH" >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" && -z "${PGHOST:-}" ]]; then
  echo "Database connection not set. Use DATABASE_URL or PG* env vars." >&2
  exit 1
fi

PSQL_BASE=(psql -v ON_ERROR_STOP=1 -q -At)

if [[ -n "${DATABASE_URL:-}" ]]; then
  PSQL_BASE+=("$DATABASE_URL")
fi

PSQL_BASE+=(-v jsonl_path="$JSONL_PATH")

inserted_count="$("${PSQL_BASE[@]}" <<'SQL'
BEGIN;
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

CREATE TEMP TABLE wecom_chat_logs_stage (raw_line JSONB) ON COMMIT DROP;

\copy wecom_chat_logs_stage(raw_line) FROM :'jsonl_path';

WITH ins AS (
  INSERT INTO wecom_chat_logs (
      msgid,
      action,
      msgtype,
      msgtime,
      from_user_id,
      roomid,
      tolist,
      content_payload,
      raw_data
  )
  SELECT
      raw_line->>'msgid' AS msgid,
      raw_line->>'action' AS action,
      raw_line->>'msgtype' AS msgtype,
      to_timestamp((raw_line->>'msgtime')::double precision / 1000.0) AS msgtime,
      raw_line->>'from' AS from_user_id,
      raw_line->>'roomid' AS roomid,
      ARRAY(
          SELECT jsonb_array_elements_text(raw_line->'tolist')
      ) AS tolist,
      raw_line->(raw_line->>'msgtype') AS content_payload,
      raw_line AS raw_data
  FROM wecom_chat_logs_stage
  ON CONFLICT (msgid) DO NOTHING
  RETURNING 1
)
SELECT COUNT(*) FROM ins;
COMMIT;
SQL
)"

ts="$(TZ=Asia/Shanghai date +'%Y-%m-%d %H:%M:%S %Z')"
echo "$ts inserted_rows=$inserted_count"
