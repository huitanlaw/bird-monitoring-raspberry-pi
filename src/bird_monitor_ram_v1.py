#!/usr/bin/env python3

import queue
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import sounddevice as sd

import birdnet_analyzer.config as cfg
from birdnet_analyzer import model
from birdnet_analyzer.analyze.utils import load_codes

from birdnet_buffer import init_db, insert_event


# ============================================================
# CONFIGURATION
# ============================================================

SAMPLE_RATE = 48000
CHANNELS = 1

WINDOW_SECONDS = 3.0
WINDOW_SAMPLES = int(SAMPLE_RATE * WINDOW_SECONDS)

MIN_CONFIDENCE = 0.50
QUEUE_SIZE = 10

SPECIES_LIST = Path.home() / "malaysia_species.txt"
LOCATION_SCRIPT = Path.home() / "get_node_location.py"

LOCAL_NODE_ID = "birdnode1"

SUPPORTED_MIC_NAMES = [
    "UACDemoV1.0",
    "UACDemo",
    "USB PnP Sound Device",
]


# ============================================================
# BIRDNET CONFIGURATION
# ============================================================

cfg.MODEL_PATH = cfg.BIRDNET_MODEL_PATH
cfg.LABELS_FILE = cfg.BIRDNET_LABELS_FILE
cfg.SAMPLE_RATE = cfg.BIRDNET_SAMPLE_RATE
cfg.SIG_LENGTH = cfg.BIRDNET_SIG_LENGTH
cfg.USE_PERCH = False
cfg.CUSTOM_CLASSIFIER = None

with open(
    cfg.LABELS_FILE,
    "r",
    encoding="utf-8",
) as f:
    ALL_LABELS = [
        line.strip()
        for line in f
        if line.strip()
    ]

with open(
    SPECIES_LIST,
    "r",
    encoding="utf-8",
) as f:
    MALAYSIA_SPECIES = {
        line.strip()
        for line in f
        if line.strip()
    }

MALAYSIA_INDICES = [
    i
    for i, label in enumerate(ALL_LABELS)
    if label in MALAYSIA_SPECIES
]

EBIRD_CODES = load_codes()


# ============================================================
# SQLITE
# ============================================================

init_db()


# ============================================================
# QUEUES / STATE
# ============================================================

audio_queue = queue.Queue(maxsize=QUEUE_SIZE)
stop_event = threading.Event()

audio_buffer = np.empty(
    0,
    dtype=np.float32,
)

buffer_lock = threading.Lock()


# Pending events:
#
# species_code -> {
#     label,
#     common_name,
#     sci_name,
#     start_time,
#     end_time,
#     confidence,
# }
#
pending_events = {}


# ============================================================
# MICROPHONE
# ============================================================

def find_microphone():

    print("===== MICROPHONE SEARCH =====")

    for index, device in enumerate(
        sd.query_devices()
    ):

        if device["max_input_channels"] <= 0:
            continue

        name = device["name"]

        for supported in SUPPORTED_MIC_NAMES:

            if supported.lower() in name.lower():

                print(f"[MIC] Detected: {name}")
                print(f"[MIC] Device index: {index}")

                return index, name

    raise RuntimeError(
        "No supported USB microphone detected."
    )


# ============================================================
# LOCATION
# ============================================================

def get_location():

    print("===== LOCATION =====")

    # Manual fallback coordinates:
    # Taman Negara Pahang
    FALLBACK_LAT = 4.7000
    FALLBACK_LON = 102.4667

    if not LOCATION_SCRIPT.exists():
        print(
            "[LOCATION] Geolocation helper not found."
        )
        print(
            "[LOCATION] Using manual fallback coordinates."
        )
        print(f"[LOCATION] Latitude  : {FALLBACK_LAT}")
        print(f"[LOCATION] Longitude : {FALLBACK_LON}")
        return FALLBACK_LAT, FALLBACK_LON

    for attempt in range(1, 4):

        print(
            f"[LOCATION] Wi-Fi geolocation "
            f"attempt {attempt}/3..."
        )

        try:

            result = subprocess.run(
                [
                    str(LOCATION_SCRIPT),
                    "--shell",
                ],
                capture_output=True,
                text=True,
                timeout=40,
                check=True,
            )

            values = {}

            for line in result.stdout.splitlines():

                if "=" not in line:
                    continue

                key, value = line.split("=", 1)

                values[
                    key.strip()
                ] = value.strip()

            lat = values.get("LAT")
            lon = values.get("LON")
            accuracy = values.get("ACCURACY")

            if lat and lon:

                lat = float(lat)
                lon = float(lon)

                print(
                    "[LOCATION] Wi-Fi geolocation acquired."
                )
                print(f"[LOCATION] Latitude  : {lat}")
                print(f"[LOCATION] Longitude : {lon}")

                if accuracy:
                    print(
                        f"[LOCATION] Accuracy   : "
                        f"{accuracy} m"
                    )

                print(
                    "[LOCATION] Source     : "
                    "Google Wi-Fi Geolocation"
                )

                return lat, lon

            raise RuntimeError(
                "No latitude/longitude returned."
            )

        except Exception as e:

            print(
                f"[LOCATION] Attempt "
                f"{attempt}/3 failed: {e}"
            )

            if attempt < 3:
                print(
                    "[LOCATION] Retrying in 3 seconds..."
                )
                time.sleep(3)

    print(
        "[LOCATION] All 3 Wi-Fi geolocation "
        "attempts failed."
    )
    print(
        "[LOCATION] Using manual fallback coordinates."
    )
    print(f"[LOCATION] Latitude  : {FALLBACK_LAT}")
    print(f"[LOCATION] Longitude : {FALLBACK_LON}")
    print(
        "[LOCATION] Source     : Manual fallback"
    )

    return FALLBACK_LAT, FALLBACK_LON


# ============================================================
# BIRDNET
# ============================================================

def run_birdnet(audio):

    prediction = model.predict(
        np.array(
            [audio],
            dtype="float32",
        )
    )

    scores = np.asarray(
        prediction
    )[0]

    if cfg.APPLY_SIGMOID:

        scores = model.flat_sigmoid(
            scores,
            sensitivity=-1,
            bias=cfg.SIGMOID_SENSITIVITY,
        )

    return scores


# ============================================================
# SQLITE EVENT SAVE
# ============================================================

def save_event(
    event,
    lat,
    lon,
):

    start_time = event["start_time"]
    end_time = event["end_time"]

    duration = int(round(
        (
            end_time - start_time
        ).total_seconds()
    ))

    detected_at = start_time.isoformat()

    detection_id, event_id, inserted = (
        insert_event(
            species_code=event["species_code"],
            common_name=event["common_name"],
            sci_name=event["sci_name"],
            confidence=round(
                event["confidence"],
                4,
            ),
            detected_at=detected_at,
            node_id=LOCAL_NODE_ID,
            lat=lat,
            lon=lon,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration=duration,
        )
    )

    print()
    print(
        "===== SQLITE EVENT ====="
    )
    print(
        f"Species    : "
        f"{event['common_name']}"
    )
    print(
        f"Code       : "
        f"{event['species_code']}"
    )
    print(
        f"Confidence : "
        f"{event['confidence']:.4f}"
    )
    print(
        f"Start      : "
        f"{start_time.isoformat()}"
    )
    print(
        f"End        : "
        f"{end_time.isoformat()}"
    )
    print(
        f"Duration   : "
        f"{duration} s"
    )
    print(
        f"SQLite ID  : {detection_id}"
    )
    print(
        f"Local event: {event_id}"
    )
    print(
        f"Inserted   : {inserted}"
    )


# ============================================================
# EVENT MERGING
# ============================================================

def process_window(
    detections,
    window_start,
    window_end,
    lat,
    lon,
):

    detected_codes = set()

    for score, label in detections:

        if "_" not in label:
            continue

        sci_name, common_name = (
            label.split("_", 1)
        )

        species_code = (
            EBIRD_CODES.get(label)
        )

        if not species_code:
            print(
                f"[WARNING] No eBird code: {label}"
            )
            continue

        detected_codes.add(
            species_code
        )

        if species_code in pending_events:

            event = pending_events[
                species_code
            ]

            event["end_time"] = (
                window_end
            )

            event["confidence"] = max(
                event["confidence"],
                score,
            )

        else:

            pending_events[
                species_code
            ] = {
                "species_code":
                    species_code,
                "label":
                    label,
                "common_name":
                    common_name,
                "sci_name":
                    sci_name,
                "start_time":
                    window_start,
                "end_time":
                    window_end,
                "confidence":
                    score,
            }

            print(
                f"[EVENT START] "
                f"{common_name} "
                f"({score:.4f})"
            )

    # Any pending species absent from this
    # 3-second window is now complete.
    finished = [
        code
        for code in pending_events
        if code not in detected_codes
    ]

    for code in finished:

        event = pending_events.pop(
            code
        )

        print(
            f"[EVENT END] "
            f"{event['common_name']}"
        )

        save_event(
            event,
            lat,
            lon,
        )


def flush_pending(
    lat,
    lon,
):

    for code in list(
        pending_events.keys()
    ):

        event = pending_events.pop(
            code
        )

        print(
            f"[EVENT FLUSH] "
            f"{event['common_name']}"
        )

        save_event(
            event,
            lat,
            lon,
        )


# ============================================================
# ANALYSIS WORKER
# ============================================================

def analysis_worker(
    lat,
    lon,
):

    print(
        "[WORKER] RAM BirdNET worker ready."
    )

    window_number = 0

    while (
        not stop_event.is_set()
        or not audio_queue.empty()
    ):

        try:

            (
                window_start,
                audio,
            ) = audio_queue.get(
                timeout=0.5
            )

        except queue.Empty:
            continue

        window_number += 1

        window_end = (
            window_start
            + timedelta(
                seconds=WINDOW_SECONDS
            )
        )

        rms = float(
            np.sqrt(
                np.mean(
                    np.square(audio)
                )
            )
        )

        peak = float(
            np.max(
                np.abs(audio)
            )
        )

        inference_start = (
            time.perf_counter()
        )

        try:

            scores = run_birdnet(
                audio
            )

        except Exception as e:

            print(
                f"[BIRDNET ERROR] {e}"
            )

            audio_queue.task_done()
            continue

        inference_time = (
            time.perf_counter()
            - inference_start
        )

        detections = []

        for idx in MALAYSIA_INDICES:

            score = float(
                scores[idx]
            )

            if (
                score
                >= MIN_CONFIDENCE
            ):

                detections.append(
                    (
                        score,
                        ALL_LABELS[idx],
                    )
                )

        detections.sort(
            key=lambda x: x[0],
            reverse=True,
        )

        print()
        print(
            "=========================================="
        )
        print(
            f"WINDOW {window_number}"
        )
        print(
            "=========================================="
        )
        print(
            f"Start     : "
            f"{window_start.isoformat()}"
        )
        print(
            f"End       : "
            f"{window_end.isoformat()}"
        )
        print(
            f"Peak      : {peak:.5f}"
        )
        print(
            f"RMS       : {rms:.5f}"
        )
        print(
            f"Inference : "
            f"{inference_time:.3f}s"
        )
        print(
            f"Queue     : "
            f"{audio_queue.qsize()}"
        )

        if detections:

            print(
                "[DETECTIONS]"
            )

            for score, label in detections:

                print(
                    f"  {score:.4f} "
                    f"{label}"
                )

        else:

            print(
                "[DETECTIONS] None"
            )

        process_window(
            detections,
            window_start,
            window_end,
            lat,
            lon,
        )

        audio_queue.task_done()


# ============================================================
# AUDIO CALLBACK
# ============================================================

def audio_callback(
    indata,
    frames,
    time_info,
    status,
):

    global audio_buffer

    if status:

        print(
            f"[AUDIO STATUS] {status}",
            flush=True,
        )

    if stop_event.is_set():
        return

    mono = (
        indata[:, 0]
        .copy()
        .astype(
            np.float32,
            copy=False,
        )
    )

    with buffer_lock:

        audio_buffer = (
            np.concatenate(
                (
                    audio_buffer,
                    mono,
                )
            )
        )

        while (
            len(audio_buffer)
            >= WINDOW_SAMPLES
        ):

            chunk = (
                audio_buffer[
                    :WINDOW_SAMPLES
                ].copy()
            )

            audio_buffer = (
                audio_buffer[
                    WINDOW_SAMPLES:
                ]
            )

            window_end = (
                datetime.now()
                .astimezone()
            )

            window_start = (
                window_end
                - timedelta(
                    seconds=
                    WINDOW_SECONDS
                )
            )

            try:

                audio_queue.put_nowait(
                    (
                        window_start,
                        chunk,
                    )
                )

            except queue.Full:

                print(
                    "[WARNING] "
                    "Analysis queue full. "
                    "3-second window dropped."
                )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=========================================="
    )
    print(
        "BirdNET RAM Monitor v1"
    )
    print(
        "Continuous 3s RAM + SQLite"
    )
    print(
        "=========================================="
    )
    print(
        f"Window      : "
        f"{WINDOW_SECONDS}s"
    )
    print(
        f"Sample rate : "
        f"{SAMPLE_RATE} Hz"
    )
    print(
        f"Confidence  : "
        f"{MIN_CONFIDENCE}"
    )
    print(
        f"Malaysia    : "
        f"{len(MALAYSIA_INDICES)} species"
    )
    print(
        "Audio files : NONE"
    )
    print(
        "=========================================="
    )

    device_index, device_name = (
        find_microphone()
    )

    lat, lon = get_location()

    print()
    print(
        "===== NODE READY ====="
    )
    print(
        f"Microphone : {device_name}"
    )
    print(
        "Location   : acquired"
        if lat is not None
        else "Location   : NA"
    )
    print(
        "Press Ctrl+C to stop."
    )

    worker = threading.Thread(
        target=analysis_worker,
        args=(lat, lon),
        daemon=True,
    )

    worker.start()

    try:

        with sd.InputStream(
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            dtype="float32",
            device=device_index,
            blocksize=0,
            latency="high",
            callback=audio_callback,
        ):

            while True:
                time.sleep(1)

    except KeyboardInterrupt:

        print()
        print(
            "[STOP] Ctrl+C received."
        )

    finally:

        stop_event.set()

        print(
            "[STOP] Waiting for "
            "queued RAM analysis..."
        )

        worker.join(
            timeout=20
        )

        flush_pending(
            lat,
            lon,
        )

        print(
            "[STOP] RAM monitor stopped."
        )


if __name__ == "__main__":
    main()
