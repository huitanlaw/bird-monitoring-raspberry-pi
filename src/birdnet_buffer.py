#!/usr/bin/env python3

from pathlib import Path
import hashlib
import sqlite3
from datetime import datetime, timezone


DB_PATH = (
    Path.home()
    / "bird_monitor_data"
    / "birdnet_buffer.db"
)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():

    DB_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    conn = sqlite3.connect(
        DB_PATH,
        timeout=10,
    )

    conn.row_factory = sqlite3.Row

    # Wait briefly if another process is writing.
    conn.execute(
        "PRAGMA busy_timeout = 5000"
    )

    # Enforce relational constraints if added later.
    conn.execute(
        "PRAGMA foreign_keys = ON"
    )

    # FULL prioritizes durability for detection records.
    conn.execute(
        "PRAGMA synchronous = FULL"
    )

    return conn


# ============================================================
# DATABASE INITIALIZATION / MIGRATION
# ============================================================

def init_db():

    with get_connection() as conn:

        # WAL improves reliability/concurrency between the
        # BirdNET monitor and the MQTT queue publisher.
        journal_mode = conn.execute(
            "PRAGMA journal_mode = WAL"
        ).fetchone()[0]

        if str(journal_mode).lower() != "wal":
            raise RuntimeError(
                f"Could not enable SQLite WAL mode: "
                f"{journal_mode}"
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS detections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                species_code TEXT NOT NULL,
                common_name TEXT NOT NULL,
                sci_name TEXT NOT NULL,
                confidence REAL NOT NULL,

                detected_at TEXT NOT NULL,
                node_id TEXT NOT NULL,

                lat REAL,
                lon REAL,

                sent INTEGER NOT NULL DEFAULT 0,
                sent_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,

                event_id TEXT,
                start_time TEXT,
                end_time TEXT,
                duration REAL
            )
            """
        )

        # --------------------------------------------
        # Upgrade an older database safely.
        # Existing detections are NOT deleted.
        # --------------------------------------------

        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(detections)"
            )
        }

        migrations = {
            "event_id":
                "ALTER TABLE detections "
                "ADD COLUMN event_id TEXT",

            "start_time":
                "ALTER TABLE detections "
                "ADD COLUMN start_time TEXT",

            "end_time":
                "ALTER TABLE detections "
                "ADD COLUMN end_time TEXT",

            "duration":
                "ALTER TABLE detections "
                "ADD COLUMN duration REAL",
        }

        for column, sql in migrations.items():

            if column not in columns:

                conn.execute(sql)

                print(
                    f"[DB MIGRATION] "
                    f"Added column: {column}"
                )

        # event_id provides duplicate protection.
        # NULL values from old test rows are allowed.

        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_detections_event_id
            ON detections(event_id)
            WHERE event_id IS NOT NULL
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_detections_pending
            ON detections(sent, id)
            """
        )

        conn.commit()


# ============================================================
# EVENT ID
# ============================================================

def make_event_id(
    node_id,
    species_code,
    start_time,
    end_time,
):

    source = "|".join(
        [
            str(node_id),
            str(species_code),
            str(start_time),
            str(end_time),
        ]
    )

    digest = hashlib.sha256(
        source.encode("utf-8")
    ).hexdigest()[:16]

    return (
        f"{node_id}_{digest}"
    )


# ============================================================
# INSERT FINAL BIRD EVENT
# ============================================================

def insert_event(
    species_code,
    common_name,
    sci_name,
    confidence,
    detected_at,
    node_id,
    lat=None,
    lon=None,
    start_time=None,
    end_time=None,
    duration=None,
):

    init_db()

    if start_time is None:
        start_time = detected_at

    event_id = make_event_id(
        node_id,
        species_code,
        start_time,
        end_time,
    )

    with get_connection() as conn:

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO detections (
                species_code,
                common_name,
                sci_name,
                confidence,
                detected_at,
                node_id,
                lat,
                lon,
                sent,
                sent_at,
                attempts,
                event_id,
                start_time,
                end_time,
                duration
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                0, NULL, 0,
                ?, ?, ?, ?
            )
            """,
            (
                species_code,
                common_name,
                sci_name,
                float(confidence),
                detected_at,
                node_id,
                lat,
                lon,
                event_id,
                start_time,
                end_time,
                (
                    float(duration)
                    if duration is not None
                    else None
                ),
            ),
        )

        conn.commit()

        inserted = (
            cursor.rowcount == 1
        )

        if inserted:

            detection_id = (
                cursor.lastrowid
            )

        else:

            existing = conn.execute(
                """
                SELECT id
                FROM detections
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()

            detection_id = (
                existing["id"]
                if existing
                else None
            )

        return (
            detection_id,
            event_id,
            inserted,
        )


# ============================================================
# BASIC INSERT
# Kept for simple tests / Module 10 compatibility.
# ============================================================

def insert_detection(
    species_code,
    common_name,
    sci_name,
    confidence,
    detected_at,
    node_id,
    lat=None,
    lon=None,
):

    with get_connection() as conn:

        cursor = conn.execute(
            """
            INSERT INTO detections (
                species_code,
                common_name,
                sci_name,
                confidence,
                detected_at,
                node_id,
                lat,
                lon,
                sent,
                sent_at,
                attempts
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                0, NULL, 0
            )
            """,
            (
                species_code,
                common_name,
                sci_name,
                float(confidence),
                detected_at,
                node_id,
                lat,
                lon,
            ),
        )

        conn.commit()

        return cursor.lastrowid


# ============================================================
# PENDING QUEUE
# ============================================================

def get_pending(
    limit=100,
):

    with get_connection() as conn:

        rows = conn.execute(
            """
            SELECT *
            FROM detections
            WHERE sent = 0
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]


# ============================================================
# DELIVERY STATUS
# ============================================================

def mark_sent(
    detection_id,
):

    sent_at = datetime.now(
        timezone.utc
    ).isoformat()

    with get_connection() as conn:

        conn.execute(
            """
            UPDATE detections
            SET
                sent = 1,
                sent_at = ?
            WHERE id = ?
            """,
            (
                sent_at,
                detection_id,
            ),
        )

        conn.commit()


def mark_sent_by_event_id(
    event_id,
):
    """
    Mark one locally buffered detection as cloud-confirmed.

    This should only be called after receiving an ACK that
    confirms Firestore successfully stored the event.
    """

    sent_at = datetime.now(
        timezone.utc
    ).isoformat()

    with get_connection() as conn:

        cursor = conn.execute(
            """
            UPDATE detections
            SET
                sent = 1,
                sent_at = ?
            WHERE event_id = ?
              AND sent = 0
            """,
            (
                sent_at,
                event_id,
            ),
        )

        conn.commit()

        return cursor.rowcount


def increment_attempts(
    detection_id,
):

    with get_connection() as conn:

        conn.execute(
            """
            UPDATE detections
            SET
                attempts = attempts + 1
            WHERE id = ?
            """,
            (detection_id,),
        )

        conn.commit()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    init_db()

    print(
        f"SQLite database ready: "
        f"{DB_PATH}"
    )
