#!/usr/bin/env python3

import hashlib
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

import firebase_admin
from firebase_admin import (
    credentials,
    firestore,
)


MQTT_BROKER = "localhost"
MQTT_PORT = 1883

DETECTION_TOPIC = (
    "birdnet/+/detections"
)

STATUS_TOPIC = (
    "birdnet/+/status"
)

QOS = 1

FIREBASE_KEY = (
    Path.home()
    / "firebase-key.json"
)

COLLECTION = "detections"
STATUS_COLLECTION = "node_status"

# Node is considered interrupted after 5 minutes without a Firestore heartbeat.
OFFLINE_THRESHOLD_SECONDS = 300.0


# ============================================================
# FIREBASE
# ============================================================

if not FIREBASE_KEY.exists():

    raise FileNotFoundError(
        f"Firebase key not found: "
        f"{FIREBASE_KEY}"
    )


cred = credentials.Certificate(
    str(FIREBASE_KEY)
)

firebase_admin.initialize_app(
    cred
)

db = firestore.client()

print(
    "[FIREBASE] Connected to Firestore.",
    flush=True,
)


# ============================================================
# HELPERS
# ============================================================

def convert_timestamp(value):

    if isinstance(
        value,
        datetime,
    ):
        return value

    if not value:

        return datetime.now(
            timezone.utc
        )

    try:

        text = str(
            value
        ).strip()

        if text.endswith("Z"):

            text = (
                text[:-1]
                + "+00:00"
            )

        dt = datetime.fromisoformat(
            text
        )

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt

    except Exception:

        return datetime.now(
            timezone.utc
        )


def make_event_id(data):

    existing = data.get(
        "event_id"
    )

    if existing:

        return str(
            existing
        )

    raw = json.dumps(
        data,
        sort_keys=True,
        default=str,
    )

    digest = hashlib.sha1(
        raw.encode("utf-8")
    ).hexdigest()[:20]

    node = data.get(
        "node_id",
        "unknown",
    )

    return (
        f"{node}_{digest}"
    )


def publish_cloud_ack(
    client,
    data,
    event_id,
):

    node_id = data.get(
        "node_id"
    )

    if not node_id:

        print(
            "[ACK] Cannot send ACK: "
            "node_id missing.",
            flush=True,
        )

        return False

    local_event_id = (
        data.get(
            "local_event_id"
        )
    )

    local_detection_id = (
        data.get(
            "local_detection_id"
        )
    )

    ack_topic = (
        f"birdnet/"
        f"{node_id}/acks"
    )

    ack = {
        "status":
            "saved",

        "event_id":
            event_id,

        "local_event_id":
            local_event_id,

        "local_detection_id":
            local_detection_id,

        "node_id":
            node_id,

        "saved_at":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    info = client.publish(
        ack_topic,
        json.dumps(
            ack,
            ensure_ascii=False,
        ),
        qos=1,
    )

    if (
        info.rc
        == mqtt.MQTT_ERR_SUCCESS
    ):

        print(
            f"[ACK] Queued ACK "
            f"for {event_id} "
            f"→ {ack_topic}",
            flush=True,
        )

        return True

    print(
        f"[ACK] Failed to queue ACK "
        f"for {event_id}. "
        f"MQTT rc={info.rc}",
        flush=True,
    )

    return False


# ============================================================
# NODE STATUS / HEARTBEAT
# ============================================================

def handle_node_status(data):

    node_id = str(
        data.get("node_id", "")
    ).strip()

    boot_id = str(
        data.get("boot_id", "")
    ).strip()

    if not node_id:
        print(
            "[NODE STATUS] Missing node_id.",
            flush=True,
        )
        return

    if not boot_id:
        print(
            f"[NODE STATUS] Missing boot_id "
            f"for {node_id}.",
            flush=True,
        )
        return

    doc_ref = (
        db
        .collection(STATUS_COLLECTION)
        .document(node_id)
    )

    # Read the previous Firestore state before updating it.
    snapshot = doc_ref.get()

    previous_data = (
        snapshot.to_dict()
        if snapshot.exists
        else {}
    )

    previous_boot_id = (
        previous_data.get("boot_id")
    )

    previous_heartbeat = (
        previous_data.get("last_heartbeat")
    )

    # Ignore invalid historical values such as None/"None".
    if previous_boot_id is not None:
        previous_boot_id = str(
            previous_boot_id
        ).strip()

    if previous_boot_id in {
        "",
        "None",
        "null",
    }:
        previous_boot_id = None

    heartbeat_gap_seconds = None

    if previous_heartbeat is not None:
        try:
            now_utc = datetime.now(timezone.utc)

            if (
                previous_heartbeat.tzinfo
                is None
            ):
                previous_heartbeat = (
                    previous_heartbeat.replace(
                        tzinfo=timezone.utc
                    )
                )

            heartbeat_gap_seconds = (
                now_utc
                - previous_heartbeat
            ).total_seconds()

        except Exception as exc:
            print(
                "[NODE STATUS] Could not calculate "
                f"heartbeat gap for {node_id}: {exc}",
                flush=True,
            )

    update_data = {
        "node_id": node_id,
        "boot_id": boot_id,

        # Firestore server time remains authoritative
        # for the stored online/offline timestamp.
        "last_heartbeat":
            firestore.SERVER_TIMESTAMP,
    }

    # A changed Linux boot_id means the Pi has booted again
    # since the previously recorded heartbeat.
    if (
        previous_boot_id
        and previous_boot_id != boot_id
    ):

        update_data[
            "previous_boot_id"
        ] = previous_boot_id

        update_data[
            "last_boot_change"
        ] = firestore.SERVER_TIMESTAMP

        update_data[
            "last_recovery_reason"
        ] = (
            "New Linux boot detected "
            "(reboot or power cycle)"
        )

        update_data[
            "last_recovery_at"
        ] = firestore.SERVER_TIMESTAMP

        print(
            f"[NODE STATUS] Boot change detected "
            f"for {node_id}.",
            flush=True,
        )

    # Same Linux boot, but Firestore did not receive a
    # heartbeat for more than the offline threshold.
    elif (
        previous_boot_id == boot_id
        and heartbeat_gap_seconds is not None
        and heartbeat_gap_seconds
            > OFFLINE_THRESHOLD_SECONDS
    ):

        update_data[
            "last_recovery_reason"
        ] = (
            "Heartbeat interruption "
            "without Linux reboot"
        )

        update_data[
            "last_recovery_at"
        ] = firestore.SERVER_TIMESTAMP

        print(
            "[NODE STATUS] Same-boot heartbeat "
            f"recovery detected for {node_id} "
            f"(gap={heartbeat_gap_seconds:.1f}s).",
            flush=True,
        )

    doc_ref.set(
        update_data,
        merge=True,
    )

    print(
        f"[NODE STATUS] Updated {node_id} "
        f"boot_id={boot_id}",
        flush=True,
    )


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
            "[MQTT] Connected to local Mosquitto.",
            flush=True,
        )

        client.subscribe(
            DETECTION_TOPIC,
            qos=QOS,
        )

        client.subscribe(
            STATUS_TOPIC,
            qos=QOS,
        )

        print(
            f"[MQTT] Subscribed to "
            f"{DETECTION_TOPIC}",
            flush=True,
        )

        print(
            f"[MQTT] Subscribed to "
            f"{STATUS_TOPIC}",
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

    try:

        payload_text = (
            msg.payload.decode(
                "utf-8"
            )
        )

        data = json.loads(
            payload_text
        )

        if msg.topic.endswith(
            "/status"
        ):

            handle_node_status(
                data
            )

            return

        print(
            f"[MQTT] Received: "
            f"{data.get('common_name', 'Unknown')}",
            flush=True,
        )

        for field in (
            "timestamp",
            "start_time",
            "end_time",
        ):

            data[field] = (
                convert_timestamp(
                    data.get(field)
                )
            )

        event_id = make_event_id(
            data
        )

        data["event_id"] = (
            event_id
        )

        # ----------------------------------------------------
        # FIRESTORE DOCUMENT
        #
        # Keep transport/internal S2 fields such as
        # local_event_id and local_detection_id in `data`
        # for ACK handling, but do not store them in Firestore.
        #
        # Firestore uses the shared 12-field event structure.
        # ----------------------------------------------------
        firestore_data = {
            "common_name":
                data.get("common_name"),

            "confidence":
                data.get("confidence"),

            "duration":
                data.get("duration"),

            "end_time":
                data.get("end_time"),

            "event_id":
                event_id,

            "lat":
                data.get("lat"),

            "lon":
                data.get("lon"),

            "node_id":
                data.get("node_id"),

            "sci_name":
                data.get("sci_name"),

            "species_code":
                data.get("species_code"),

            "start_time":
                data.get("start_time"),

            "timestamp":
                data.get("timestamp"),
        }

        for attempt in range(
            1,
            6,
        ):

            try:

                (
                    db
                    .collection(
                        COLLECTION
                    )
                    .document(
                        event_id
                    )
                    .set(
                        firestore_data
                    )
                )

                print(
                    f"[FIRESTORE] Saved "
                    f"{event_id} "
                    f"from "
                    f"{data.get('node_id')}",
                    flush=True,
                )

                # Only acknowledge after Firestore confirms
                # the document write succeeded.
                publish_cloud_ack(
                    client,
                    data,
                    event_id,
                )

                return

            except Exception as e:

                print(
                    f"[FIRESTORE] "
                    f"Attempt "
                    f"{attempt}/5 failed: "
                    f"{e}",
                    flush=True,
                )

                if attempt < 5:

                    wait_time = (
                        2
                        ** (
                            attempt
                            - 1
                        )
                    )

                    print(
                        f"[FIRESTORE] "
                        f"Retrying in "
                        f"{wait_time}s...",
                        flush=True,
                    )

                    time.sleep(
                        wait_time
                    )

        print(
            "[FIRESTORE] Detection could not "
            "be uploaded after retries. "
            "No ACK sent.",
            flush=True,
        )

    except json.JSONDecodeError:

        print(
            "[ERROR] Invalid JSON received.",
            flush=True,
        )

    except Exception as e:

        print(
            f"[ERROR] MQTT message "
            f"processing failed: {e}",
            flush=True,
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=========================================="
    )
    print(
        "BirdNET MQTT → Firestore + ACK Bridge"
    )
    print(
        "=========================================="
    )
    print(
        f"Broker     : "
        f"{MQTT_BROKER}:{MQTT_PORT}"
    )
    print(
        f"Detection topic : "
        f"{DETECTION_TOPIC}"
    )
    print(
        f"Status topic    : "
        f"{STATUS_TOPIC}"
    )
    print(
        f"Detection collection : "
        f"{COLLECTION}"
    )
    print(
        f"Status collection    : "
        f"{STATUS_COLLECTION}"
    )
    print(
        "ACK policy : Firestore success only"
    )
    print(
        "=========================================="
    )

    client = mqtt.Client(
        client_id=(
            "birdnode1-firestore-bridge"
        )
    )

    client.on_connect = (
        on_connect
    )

    client.on_disconnect = (
        on_disconnect
    )

    client.on_message = (
        on_message
    )

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=30,
    )

    while True:

        try:

            client.connect(
                MQTT_BROKER,
                MQTT_PORT,
                keepalive=60,
            )

            client.loop_forever()

        except KeyboardInterrupt:

            print(
                "\nStopping bridge...",
                flush=True,
            )

            break

        except Exception as e:

            print(
                f"[MQTT] Bridge error: "
                f"{e}",
                flush=True,
            )

            print(
                "[MQTT] Retrying in "
                "5 seconds...",
                flush=True,
            )

            time.sleep(5)


if __name__ == "__main__":
    main()
