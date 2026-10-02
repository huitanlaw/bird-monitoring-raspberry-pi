#!/usr/bin/env python3

import hashlib
import json
import threading
import time
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

from birdnet_buffer import (
    init_db,
    get_pending,
    mark_sent,
    mark_sent_by_event_id,
    increment_attempts,
)


# ============================================================
# CONFIG
# ============================================================

BROKER = "localhost"
PORT = 1883

# Cloud identity.
NODE_ID = "birdnode3"

DETECTION_TOPIC = (
    f"birdnet/{NODE_ID}/detections"
)

ACK_TOPIC = (
    f"birdnet/{NODE_ID}/acks"
)

STATUS_TOPIC = (
    f"birdnet/{NODE_ID}/status"
)

HEARTBEAT_INTERVAL = 120.0

POLL_INTERVAL = 1.0
BATCH_SIZE = 50

# If Firestore ACK is not received within this period,
# the same deterministic event is published again.
ACK_TIMEOUT = 30.0


# ============================================================
# IN-FLIGHT STATE
# ============================================================

# local_event_id -> monotonic retry deadline
inflight = {}
inflight_lock = threading.Lock()


# ============================================================
# HELPERS
# ============================================================

def to_float(value):

    try:
        return float(value)

    except (ValueError, TypeError):
        return None


def make_cloud_event_id(row):

    # --------------------------------------------------------
    # Human-readable deterministic Firestore event ID
    #
    # Format:
    # birdnode3_<species>_<YYYYMMDD>_<HHMMSS>_<HHMMSS>
    #
    # Example:
    # birdnode3_asikoe2_20260914_131910_131916
    #
    # IMPORTANT:
    # This changes ONLY the cloud / Firestore event ID.
    # SQLite keeps its original hash-based local_event_id.
    # --------------------------------------------------------

    species_code = str(
        row.get("species_code", "unknown")
    ).strip()

    start_text = (
        row.get("start_time")
        or row.get("detected_at")
        or ""
    )

    end_text = (
        row.get("end_time")
        or start_text
    )

    try:
        start_dt = datetime.fromisoformat(
            str(start_text).replace("Z", "+00:00")
        )

        end_dt = datetime.fromisoformat(
            str(end_text).replace("Z", "+00:00")
        )

        date_part = start_dt.strftime(
            "%Y%m%d"
        )

        start_part = start_dt.strftime(
            "%H%M%S"
        )

        end_part = end_dt.strftime(
            "%H%M%S"
        )

        return (
            f"{NODE_ID}_"
            f"{species_code}_"
            f"{date_part}_"
            f"{start_part}_"
            f"{end_part}"
        )

    except Exception:

        # Safety fallback for legacy / malformed rows.
        # Keep a deterministic hash rather than generating
        # a random ID.
        source = "|".join(
            [
                NODE_ID,
                species_code,
                str(start_text),
                str(end_text),
            ]
        )

        digest = hashlib.sha256(
            source.encode("utf-8")
        ).hexdigest()[:16]

        return (
            f"{NODE_ID}_{digest}"
        )


def build_payload(row):

    start_time = (
        row.get("start_time")
        or row.get("detected_at")
        or ""
    )

    end_time = (
        row.get("end_time")
        or start_time
    )

    duration = to_float(
        row.get("duration")
    )

    payload = {
        "event_id":
            make_cloud_event_id(row),

        # Local identity is carried separately so an ACK
        # can identify the exact SQLite record.
        "local_event_id":
            row.get("event_id"),

        "local_detection_id":
            row.get("id"),

        "species_code":
            row.get(
                "species_code",
                "",
            ),

        "common_name":
            row.get(
                "common_name",
                "",
            ),

        "sci_name":
            row.get(
                "sci_name",
                "",
            ),

        "confidence":
            to_float(
                row.get(
                    "confidence"
                )
            ),

        # timestamp = actual event start
        "timestamp":
            start_time,

        "start_time":
            start_time,

        "end_time":
            end_time,

        "duration":
            duration,

        # Cloud identity.
        "node_id":
            NODE_ID,

        "lat":
            to_float(
                row.get(
                    "lat"
                )
            ),

        "lon":
            to_float(
                row.get(
                    "lon"
                )
            ),
    }

    return payload


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    rc,
):

    if rc == 0:

        print(
            "[MQTT] Connected.",
            flush=True,
        )

        result, mid = client.subscribe(
            ACK_TOPIC,
            qos=1,
        )

        print(
            f"[MQTT] ACK subscription: "
            f"{ACK_TOPIC} "
            f"(result={result})",
            flush=True,
        )

    else:

        print(
            f"[MQTT] Connection failed. "
            f"Return code: {rc}",
            flush=True,
        )


def on_disconnect(
    client,
    userdata,
    rc,
):

    print(
        f"[MQTT] Disconnected. "
        f"Return code: {rc}",
        flush=True,
    )


def on_message(
    client,
    userdata,
    msg,
):

    if msg.topic != ACK_TOPIC:
        return

    try:

        data = json.loads(
            msg.payload.decode("utf-8")
        )

    except Exception as e:

        print(
            f"[ACK] Invalid ACK payload: {e}",
            flush=True,
        )

        return

    if data.get("status") != "saved":

        print(
            f"[ACK] Ignoring non-success ACK: "
            f"{data}",
            flush=True,
        )

        return

    local_event_id = (
        data.get("local_event_id")
    )

    local_detection_id = (
        data.get("local_detection_id")
    )

    cloud_event_id = (
        data.get("event_id")
    )

    updated = 0

    if local_event_id:

        updated = (
            mark_sent_by_event_id(
                local_event_id
            )
        )

        with inflight_lock:
            inflight.pop(
                local_event_id,
                None,
            )

    elif local_detection_id is not None:

        # Compatibility path for legacy rows that may not
        # have a local event_id.
        mark_sent(
            int(local_detection_id)
        )

        updated = 1

    else:

        print(
            "[ACK] ACK does not contain a local "
            "record identifier.",
            flush=True,
        )

        return

    print(
        f"[ACK] Cloud confirmed "
        f"{cloud_event_id} "
        f"(local_event_id={local_event_id}, "
        f"updated={updated})",
        flush=True,
    )


# ============================================================
# MQTT CLIENT
# ============================================================

client = mqtt.Client()

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.reconnect_delay_set(
    min_delay=1,
    max_delay=30,
)


def connect_mqtt():

    while True:

        try:

            print(
                f"[MQTT] Connecting to "
                f"{BROKER}:{PORT}...",
                flush=True,
            )

            client.connect(
                BROKER,
                PORT,
                keepalive=60,
            )

            client.loop_start()

            for _ in range(20):

                if client.is_connected():
                    return

                time.sleep(0.25)

        except Exception as e:

            print(
                f"[MQTT] Connection failed: "
                f"{e}",
                flush=True,
            )

        time.sleep(3)


# ============================================================
# ACK / RETRY STATE
# ============================================================

def waiting_for_ack(
    local_event_id,
):

    if not local_event_id:
        return False

    now = time.monotonic()

    with inflight_lock:

        retry_at = inflight.get(
            local_event_id
        )

        if retry_at is None:
            return False

        if now >= retry_at:

            inflight.pop(
                local_event_id,
                None,
            )

            print(
                f"[ACK] Timeout for "
                f"{local_event_id}; "
                f"eligible for retry.",
                flush=True,
            )

            return False

        return True


def set_waiting_for_ack(
    local_event_id,
):

    if not local_event_id:
        return

    with inflight_lock:

        inflight[
            local_event_id
        ] = (
            time.monotonic()
            + ACK_TIMEOUT
        )


# ============================================================
# PUBLISH
# ============================================================

def publish_detection(row):

    detection_id = row["id"]
    local_event_id = row.get(
        "event_id"
    )

    payload = build_payload(row)

    if not client.is_connected():

        print(
            "[MQTT] Broker disconnected.",
            flush=True,
        )

        return False

    # This is a real transmission attempt.
    increment_attempts(
        detection_id
    )

    try:

        message = json.dumps(
            payload,
            ensure_ascii=False,
        )

        # Mark in-flight before publishing. If an ACK arrives
        # very quickly, the callback can remove it safely.
        set_waiting_for_ack(
            local_event_id
        )

        info = client.publish(
            DETECTION_TOPIC,
            message,
            qos=1,
        )

        info.wait_for_publish(
            timeout=10
        )

        if (
            info.rc
            == mqtt.MQTT_ERR_SUCCESS
            and info.is_published()
        ):

            print()
            print(
                f"[MQTT SUBMITTED] "
                f"{payload['common_name']}",
                flush=True,
            )

            print(
                f"  SQLite ID     : "
                f"{detection_id}",
                flush=True,
            )

            print(
                f"  Local Event ID: "
                f"{local_event_id}",
                flush=True,
            )

            print(
                f"  Cloud Event ID: "
                f"{payload['event_id']}",
                flush=True,
            )

            print(
                "  Status        : "
                "waiting for Firestore ACK",
                flush=True,
            )

            # IMPORTANT:
            # Do NOT mark SQLite sent here.
            # sent=1 happens only after Firestore ACK.
            return True

        with inflight_lock:
            inflight.pop(
                local_event_id,
                None,
            )

        print(
            f"[MQTT] Publish failed "
            f"for SQLite ID "
            f"{detection_id}",
            flush=True,
        )

        return False

    except Exception as e:

        with inflight_lock:
            inflight.pop(
                local_event_id,
                None,
            )

        print(
            f"[MQTT] Publish error "
            f"for SQLite ID "
            f"{detection_id}: "
            f"{e}",
            flush=True,
        )

        return False


# ============================================================
# SQLITE PENDING QUEUE
# ============================================================

def drain_pending():

    rows = get_pending(
        limit=BATCH_SIZE
    )

    if not rows:
        return 0

    processed = 0

    for row in rows:

        common_name = (
            row.get(
                "common_name",
                ""
            )
            .strip()
            .lower()
        )

        # nocall is intentionally never sent to cloud.
        if common_name in {
            "nocall",
            "no call",
        }:

            print(
                f"[QUEUE] Ignoring nocall "
                f"SQLite ID {row['id']}",
                flush=True,
            )

            mark_sent(
                row["id"]
            )

            continue

        local_event_id = (
            row.get("event_id")
        )

        if waiting_for_ack(
            local_event_id
        ):
            continue

        success = publish_detection(
            row
        )

        if success:

            processed += 1

        elif not client.is_connected():

            # If broker is down, there is no benefit in
            # trying every remaining row.
            break

    return processed


# ============================================================
# NODE STATUS / HEARTBEAT
# ============================================================

def get_boot_id():

    try:

        return (
            Path(
                "/proc/sys/kernel/random/boot_id"
            )
            .read_text()
            .strip()
        )

    except Exception as e:

        print(
            f"[HEARTBEAT] Could not read boot_id: {e}",
            flush=True,
        )

        return None


def publish_heartbeat():

    if not client.is_connected():
        return False

    payload = {
        "node_id": NODE_ID,
        "boot_id": get_boot_id(),
        "timestamp": datetime.now().astimezone().isoformat(),
    }

    try:

        info = client.publish(
            STATUS_TOPIC,
            json.dumps(payload),
            qos=1,
            retain=True,
        )

        info.wait_for_publish(
            timeout=10
        )

        if (
            info.rc == mqtt.MQTT_ERR_SUCCESS
            and info.is_published()
        ):

            print(
                f"[HEARTBEAT] Published "
                f"{NODE_ID} "
                f"boot_id={payload['boot_id']}",
                flush=True,
            )

            return True

    except Exception as e:

        print(
            f"[HEARTBEAT] Publish error: {e}",
            flush=True,
        )

    return False


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=========================================="
    )
    print(
        "BirdNET SQLite → MQTT → Cloud ACK Publisher"
    )
    print(
        "=========================================="
    )
    print(
        f"Detection topic : {DETECTION_TOPIC}"
    )
    print(
        f"ACK topic       : {ACK_TOPIC}"
    )
    print(
        f"Status topic    : {STATUS_TOPIC}"
    )
    print(
        f"Heartbeat       : {HEARTBEAT_INTERVAL}s"
    )
    print(
        f"Node            : {NODE_ID}"
    )
    print(
        f"ACK timeout     : {ACK_TIMEOUT}s"
    )
    print(
        "sent=1 only after Firestore ACK"
    )
    print(
        "=========================================="
    )

    init_db()

    connect_mqtt()

    last_heartbeat = 0.0

    while True:

        try:

            now = time.monotonic()

            if (
                now - last_heartbeat
                >= HEARTBEAT_INTERVAL
            ):

                if publish_heartbeat():
                    last_heartbeat = now

            drain_pending()

        except Exception as e:

            print(
                f"[QUEUE] Error: {e}",
                flush=True,
            )

        time.sleep(
            POLL_INTERVAL
        )


if __name__ == "__main__":
    main()
