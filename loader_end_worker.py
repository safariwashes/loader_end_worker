# loader_end_worker.py
# Hardened Render Worker (psycopg v3)

import os
import sys
import time
import logging
from typing import Optional, Tuple, Set

import psycopg
from psycopg.rows import dict_row

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
TENANT_ID = (os.getenv("TENANT_ID") or "").strip() or None
LOCATION_ID = (os.getenv("LOCATION_ID") or "").strip() or None
REQUIRE_SUPER = (os.getenv("REQUIRE_SUPER", "false").strip().lower() in {"1", "true", "yes", "y"})

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

SOURCE = "loader_end_worker"

logging.basicConfig(
    stream=sys.stdout,
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(SOURCE)

SELECT_TUNNEL_ROWS_SQL = """
SELECT
    t.tenant_id,
    t.location_id,
    t.location,
    t.bill,
    t.created_on,
    t.load_time,
    l.length_sec,
    (
        t.created_on::timestamp
        + t.load_time
        + make_interval(secs => l.length_sec)
    ) AS exit_due_ts
FROM tunnel t
JOIN tunnel_length l
  ON l.tenant_id = t.tenant_id
 AND l.location_id = t.location_id
WHERE COALESCE(t.exit, false) = false
  AND t.load_time IS NOT NULL
  AND (%s::uuid IS NULL OR t.tenant_id = %s::uuid)
  AND (%s::uuid IS NULL OR t.location_id = %s::uuid)
ORDER BY
    (
        t.created_on::timestamp
        + t.load_time
        + make_interval(secs => l.length_sec)
    ) ASC,
    t.created_on ASC,
    t.load_time ASC,
    t.bill ASC
FOR UPDATE OF t SKIP LOCKED
LIMIT %s
"""

COUNT_ELIGIBLE_SQL = """
SELECT COUNT(*) AS eligible
FROM tunnel t
JOIN tunnel_length l
  ON l.tenant_id = t.tenant_id
 AND l.location_id = t.location_id
WHERE COALESCE(t.exit, false) = false
  AND t.load_time IS NOT NULL
  AND (%s::uuid IS NULL OR t.tenant_id = %s::uuid)
  AND (%s::uuid IS NULL OR t.location_id = %s::uuid)
"""

UPDATE_TUNNEL_EXIT_SQL = """
UPDATE tunnel
SET
    exit = true,
    exit_time = (
        (
            %s::date::timestamp
            + %s::time
            + make_interval(secs => %s)
        )::time
    )
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
  AND COALESCE(exit, false) = false
"""

UPDATE_VEHICLE_SQL = """
UPDATE vehicle
SET
    status = '4',
    status_desc = 'Dry & Shine'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
  AND (
        COALESCE(status, '0') <> '4'
        OR status_desc IS DISTINCT FROM 'Dry & Shine'
      )
"""

UPDATE_SUPER_SQL = """
UPDATE super
SET
    status = 4,
    status_desc = 'Dry & Shine'
WHERE tenant_id = %s
  AND location_id = %s
  AND created_on = %s
  AND bill = %s
  AND (
        COALESCE(status, 0) <> 4
        OR status_desc IS DISTINCT FROM 'Dry & Shine'
      )
"""

CHECK_TUNNEL_OK_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM tunnel
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
      AND exit = true
      AND exit_time = (
            (
                %s::date::timestamp
                + %s::time
                + make_interval(secs => %s)
            )::time
      )
) AS ok
"""

CHECK_VEHICLE_OK_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM vehicle
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
      AND status = '4'
      AND status_desc = 'Dry & Shine'
) AS ok
"""

CHECK_SUPER_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM super
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
) AS exists_flag
"""

CHECK_SUPER_OK_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM super
    WHERE tenant_id = %s
      AND location_id = %s
      AND created_on = %s
      AND bill = %s
      AND status = 4
      AND status_desc = 'Dry & Shine'
) AS ok
"""

INSERT_HEARTBEAT_SQL = """
INSERT INTO heartbeat (source, tenant_id, location_id)
VALUES (%s, %s, %s)
"""

def connect():
    return psycopg.connect(DATABASE_URL, connect_timeout=10)

def fetch_bool(cur, sql: str, params: tuple) -> bool:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return False
    return bool(row.get("ok", row.get("exists_flag", False)))

def write_heartbeat(conn, touched_pairs: Set[Tuple[Optional[str], Optional[str]]]) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            if touched_pairs:
                for tenant_id, location_id in sorted(touched_pairs):
                    cur.execute(INSERT_HEARTBEAT_SQL, (SOURCE, tenant_id, location_id))
            else:
                cur.execute(INSERT_HEARTBEAT_SQL, (SOURCE, TENANT_ID, LOCATION_ID))

def process_batch(conn) -> Tuple[int, Set[Tuple[str, str]]]:
    processed_count = 0
    touched_pairs: Set[Tuple[str, str]] = set()

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(COUNT_ELIGIBLE_SQL, (TENANT_ID, TENANT_ID, LOCATION_ID, LOCATION_ID))
            eligible = cur.fetchone()["eligible"]
            log.debug(
                "Eligible tunnel rows=%s tenant_filter=%s location_filter=%s",
                eligible,
                TENANT_ID,
                LOCATION_ID,
            )

            cur.execute(
                SELECT_TUNNEL_ROWS_SQL,
                (TENANT_ID, TENANT_ID, LOCATION_ID, LOCATION_ID, BATCH_SIZE),
            )
            rows = cur.fetchall()

            if not rows:
                log.info("No eligible tunnel rows this cycle.")
                return 0, touched_pairs

            cur.execute("SELECT NOW() AS now_ts")
            now_ts = cur.fetchone()["now_ts"]

            log.info("Fetched %s tunnel rows", len(rows))

            for r in rows:
                tenant_id = r["tenant_id"]
                location_id = r["location_id"]
                location_code = r["location"]
                bill = r["bill"]
                created_on = r["created_on"]
                load_time = r["load_time"]
                length_sec = r["length_sec"]
                exit_due_ts = r["exit_due_ts"]

                if exit_due_ts is None:
                    log.warning(
                        "Skipping row with null exit_due_ts tenant=%s location=%s bill=%s created_on=%s",
                        tenant_id, location_id, bill, created_on
                    )
                    continue

                if now_ts < exit_due_ts:
                    log.debug(
                        "Not due yet tenant=%s location=%s bill=%s due=%s now=%s",
                        tenant_id, location_id, bill, exit_due_ts, now_ts
                    )
                    continue

                log.info(
                    "Exit triggered tenant=%s location=%s/%s bill=%s created_on=%s load_time=%s length_sec=%s due=%s now=%s",
                    tenant_id,
                    location_code,
                    location_id,
                    bill,
                    created_on,
                    load_time,
                    length_sec,
                    exit_due_ts,
                    now_ts,
                )

                try:
                    with conn.transaction():
                        with conn.cursor(row_factory=dict_row) as row_cur:
                            row_cur.execute(
                                UPDATE_TUNNEL_EXIT_SQL,
                                (created_on, load_time, length_sec, tenant_id, location_id, created_on, bill),
                            )
                            t_rc = row_cur.rowcount

                            row_cur.execute(
                                UPDATE_VEHICLE_SQL,
                                (tenant_id, location_id, created_on, bill),
                            )
                            v_rc = row_cur.rowcount

                            row_cur.execute(
                                UPDATE_SUPER_SQL,
                                (tenant_id, location_id, created_on, bill),
                            )
                            s_rc = row_cur.rowcount

                            tunnel_ok = fetch_bool(
                                row_cur,
                                CHECK_TUNNEL_OK_SQL,
                                (tenant_id, location_id, created_on, bill, created_on, load_time, length_sec),
                            )

                            vehicle_ok = fetch_bool(
                                row_cur,
                                CHECK_VEHICLE_OK_SQL,
                                (tenant_id, location_id, created_on, bill),
                            )

                            super_exists = fetch_bool(
                                row_cur,
                                CHECK_SUPER_EXISTS_SQL,
                                (tenant_id, location_id, created_on, bill),
                            )
                            super_ok = fetch_bool(
                                row_cur,
                                CHECK_SUPER_OK_SQL,
                                (tenant_id, location_id, created_on, bill),
                            ) if super_exists else (not REQUIRE_SUPER)

                            final_super_ok = super_ok if super_exists else (not REQUIRE_SUPER)

                            log.debug(
                                "Result bill=%s t_rc=%s v_rc=%s s_rc=%s tunnel_ok=%s vehicle_ok=%s super_exists=%s super_ok=%s",
                                bill, t_rc, v_rc, s_rc, tunnel_ok, vehicle_ok, super_exists, final_super_ok
                            )

                            if not (tunnel_ok and vehicle_ok and final_super_ok):
                                reasons = []
                                if not tunnel_ok:
                                    reasons.append("tunnel not in desired exit state")
                                if not vehicle_ok:
                                    reasons.append("vehicle not in desired status 4 state")
                                if not final_super_ok:
                                    reasons.append("super not in desired status 4 state")
                                raise RuntimeError("; ".join(reasons))

                    processed_count += 1
                    touched_pairs.add((str(tenant_id), str(location_id)))
                    log.info(
                        "Confirmed exit tenant=%s location=%s bill=%s",
                        tenant_id, location_id, bill
                    )

                except Exception as row_err:
                    log.warning(
                        "Leaving row pending tenant=%s location=%s bill=%s reason=%s",
                        tenant_id,
                        location_id,
                        bill,
                        row_err,
                    )

    return processed_count, touched_pairs

def main():
    log.info(
        "Starting %s poll=%ss batch=%s tenant_filter=%s location_filter=%s require_super=%s",
        SOURCE,
        POLL_SECONDS,
        BATCH_SIZE,
        TENANT_ID,
        LOCATION_ID,
        REQUIRE_SUPER,
    )

    conn = None

    while True:
        try:
            if conn is None or conn.closed:
                conn = connect()
                conn.autocommit = False
                log.info("DB connected")

            processed, touched = process_batch(conn)

            if processed:
                log.info("Processed exits=%s", processed)

            write_heartbeat(conn, touched)

            time.sleep(POLL_SECONDS)

        except psycopg.OperationalError as e:
            log.warning("DB operational error (reconnecting): %s", e)
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

        except Exception as e:
            log.exception("Worker error: %s", e)
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)

if __name__ == "__main__":
    main()