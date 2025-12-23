#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load WeCom JSONL logs into PostgreSQL.")
    parser.add_argument(
        "--jsonl",
        default=os.getenv("JSONL_PATH", "/opt/wecom/decrypted_database_ready.jsonl"),
        help="Path to JSONL file",
    )
    parser.add_argument(
        "--conflict",
        choices=["skip", "update"],
        default="skip",
        help="Conflict strategy for msgid",
    )
    return parser.parse_args()


def to_timestamptz_ms(ms_value):
    if ms_value is None:
        return None
    try:
        ms = int(ms_value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def extract_payload(msg):
    msgtype = msg.get("msgtype")
    if not msgtype:
        return None
    return msg.get(msgtype)


def build_upsert_sql(conflict_mode: str) -> str:
    if conflict_mode == "update":
        return """
            INSERT INTO wecom_chat_logs (
                msgid, action, msgtype, msgtime, from_user_id, roomid,
                tolist, content_payload, raw_data
            ) VALUES (
                %(msgid)s, %(action)s, %(msgtype)s, %(msgtime)s, %(from_user_id)s,
                %(roomid)s, %(tolist)s, %(content_payload)s, %(raw_data)s
            )
            ON CONFLICT (msgid) DO UPDATE SET
                action = EXCLUDED.action,
                msgtype = EXCLUDED.msgtype,
                msgtime = EXCLUDED.msgtime,
                from_user_id = EXCLUDED.from_user_id,
                roomid = EXCLUDED.roomid,
                tolist = EXCLUDED.tolist,
                content_payload = EXCLUDED.content_payload,
                raw_data = EXCLUDED.raw_data
        """
    return """
        INSERT INTO wecom_chat_logs (
            msgid, action, msgtype, msgtime, from_user_id, roomid,
            tolist, content_payload, raw_data
        ) VALUES (
            %(msgid)s, %(action)s, %(msgtype)s, %(msgtime)s, %(from_user_id)s,
            %(roomid)s, %(tolist)s, %(content_payload)s, %(raw_data)s
        )
        ON CONFLICT (msgid) DO NOTHING
    """


def ensure_schema(cursor) -> None:
    cursor.execute(
        """
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
        """
    )


def main() -> int:
    args = parse_args()

    jsonl_path = args.jsonl
    if not os.path.isfile(jsonl_path):
        print(f"JSONL file not found: {jsonl_path}", file=sys.stderr)
        return 1

    database_url = os.getenv("DATABASE_URL") or "postgresql://postgres:password@localhost/wechat_db"
    if not database_url:
        missing = [k for k in ["PGHOST", "PGUSER", "PGDATABASE"] if not os.getenv(k)]
        if missing:
            print(
                "Database connection not set. Use DATABASE_URL or PG* env vars.",
                file=sys.stderr,
            )
            return 1

    conn = psycopg2.connect(database_url) if database_url else psycopg2.connect()
    conn.autocommit = False
    cursor = conn.cursor()

    upsert_sql = build_upsert_sql(args.conflict)

    inserted = 0
    try:
        ensure_schema(cursor)
        with open(jsonl_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                msg = json.loads(line)

                payload = extract_payload(msg)
                row = {
                    "msgid": msg.get("msgid"),
                    "action": msg.get("action"),
                    "msgtype": msg.get("msgtype"),
                    "msgtime": to_timestamptz_ms(msg.get("msgtime")),
                    "from_user_id": msg.get("from"),
                    "roomid": msg.get("roomid"),
                    "tolist": msg.get("tolist"),
                    "content_payload": psycopg2.extras.Json(payload),
                    "raw_data": psycopg2.extras.Json(msg),
                }

                cursor.execute(upsert_sql, row)
                if cursor.rowcount == 1:
                    inserted += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    print(inserted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
