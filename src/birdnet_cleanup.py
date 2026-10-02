#!/usr/bin/env python3

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


DB_PATH = Path.home() / "bird_monitor_data" / "birdnet_buffer.db"

RETENTION_HOURS = 24


def main():

    print("==========================================")
    print("BirdNET SQLite Storage Cleanup")
    print("==========================================")
    print(f"Database  : {DB_PATH}")
    print(f"Retention : {RETENTION_HOURS} hours")

    if not DB_PATH.exists():
        raise SystemExit(
            f"[ERROR] Database not found: {DB_PATH}"
        )

    # sent_at in the current S2 implementation is UTC.
    now_utc = datetime.now(timezone.utc)

    cutoff_utc = (
        now_utc - timedelta(hours=RETENTION_HOURS)
    )

    cutoff_text = cutoff_utc.isoformat()

    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
    )

    try:

        conn.execute("PRAGMA busy_timeout=30000")

        # --------------------------------------------------
        # State BEFORE cleanup
        # --------------------------------------------------

        total_before = conn.execute(
            "SELECT COUNT(*) FROM detections"
        ).fetchone()[0]

        sent_before = conn.execute(
            """
            SELECT COUNT(*)
            FROM detections
            WHERE sent = 1
            """
        ).fetchone()[0]

        pending_before = conn.execute(
            """
            SELECT COUNT(*)
            FROM detections
            WHERE sent = 0
            """
        ).fetchone()[0]

        eligible = conn.execute(
            """
            SELECT COUNT(*)
            FROM detections
            WHERE sent = 1
              AND sent_at IS NOT NULL
              AND sent_at < ?
            """,
            (cutoff_text,),
        ).fetchone()[0]

        print()
        print("===== BEFORE =====")
        print(f"Total             : {total_before}")
        print(f"Sent              : {sent_before}")
        print(f"Pending           : {pending_before}")
        print(f"Eligible >24h     : {eligible}")
        print(f"UTC cutoff        : {cutoff_text}")

        # --------------------------------------------------
        # SAFETY RULE:
        #
        # ONLY sent=1 rows may be deleted.
        # sent=0 rows are never touched by this cleanup.
        # --------------------------------------------------

        cursor = conn.execute(
            """
            DELETE FROM detections
            WHERE sent = 1
              AND sent_at IS NOT NULL
              AND sent_at < ?
            """,
            (cutoff_text,),
        )

        deleted = cursor.rowcount

        conn.commit()

        # --------------------------------------------------
        # WAL maintenance
        # --------------------------------------------------

        try:
            checkpoint = conn.execute(
                "PRAGMA wal_checkpoint(PASSIVE)"
            ).fetchone()

            print()
            print(
                f"[WAL] Passive checkpoint: {checkpoint}"
            )

        except sqlite3.OperationalError as exc:
            print()
            print(
                f"[WAL] Checkpoint skipped: {exc}"
            )

        # --------------------------------------------------
        # State AFTER cleanup
        # --------------------------------------------------

        total_after = conn.execute(
            "SELECT COUNT(*) FROM detections"
        ).fetchone()[0]

        sent_after = conn.execute(
            """
            SELECT COUNT(*)
            FROM detections
            WHERE sent = 1
            """
        ).fetchone()[0]

        pending_after = conn.execute(
            """
            SELECT COUNT(*)
            FROM detections
            WHERE sent = 0
            """
        ).fetchone()[0]

        integrity = conn.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        print()
        print("===== AFTER =====")
        print(f"Deleted           : {deleted}")
        print(f"Total             : {total_after}")
        print(f"Sent              : {sent_after}")
        print(f"Pending           : {pending_after}")
        print(f"Integrity         : {integrity}")

        # Critical safety verification.
        if pending_after != pending_before:
            raise RuntimeError(
                "SAFETY CHECK FAILED: "
                "pending sent=0 count changed."
            )

        if integrity != "ok":
            raise RuntimeError(
                "SQLite integrity check failed."
            )

        print()
        print(
            "[PASS] Cleanup completed safely."
        )
        print(
            "[PASS] sent=0 records were preserved."
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
