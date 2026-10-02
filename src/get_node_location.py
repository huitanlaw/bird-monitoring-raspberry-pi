#!/usr/bin/env python3

import json
import sys
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

KEY_FILE = Path.home() / ".config" / "birdnet" / "google_geo_key"
WIFI_INTERFACE = "wlan0"


def load_api_key():
    if not KEY_FILE.exists():
        raise RuntimeError(f"API key file not found: {KEY_FILE}")

    key = KEY_FILE.read_text().strip()

    if not key:
        raise RuntimeError("API key file is empty.")

    return key


def scan_wifi():
    result = subprocess.run(
        ["sudo", "iw", "dev", WIFI_INTERFACE, "scan"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Wi-Fi scan failed:\n" + result.stderr.strip()
        )

    aps = []
    current = {}

    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()

        if line.startswith("BSS "):

            if current.get("mac") and current.get("signal") is not None:
                aps.append(current)

            current = {
                "mac": line.split()[1].split("(")[0].lower(),
                "signal": None
            }

        elif line.startswith("signal:") and current.get("mac"):

            try:
                current["signal"] = float(line.split()[1])
            except ValueError:
                pass

    if current.get("mac") and current.get("signal") is not None:
        aps.append(current)

    return aps


def is_usable_mac(mac):
    try:
        first_octet = int(mac.split(":")[0], 16)
    except Exception:
        return False

    # Reject multicast MAC
    if first_octet & 0x01:
        return False

    # Reject locally administered MAC
    if first_octet & 0x02:
        return False

    if mac == "ff:ff:ff:ff:ff:ff":
        return False

    return True


def get_google_location(api_key, aps):
    usable = [
        ap for ap in aps
        if is_usable_mac(ap["mac"])
    ]

    usable.sort(
        key=lambda x: x["signal"],
        reverse=True
    )

    selected = usable[:12]

    if len(selected) < 2:
        raise RuntimeError(
            f"Only {len(selected)} usable AP(s) found. "
            "At least 2 are required."
        )

    payload = {
        "considerIp": False,
        "wifiAccessPoints": [
            {
                "macAddress": ap["mac"],
                "signalStrength": ap["signal"]
            }
            for ap in selected
        ]
    }

    url = (
        "https://www.googleapis.com/geolocation/v1/geolocate"
        "?key=" + api_key
    )

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            data = json.loads(
                response.read().decode("utf-8")
            )

    except urllib.error.HTTPError as e:
        body = e.read().decode(
            "utf-8",
            errors="replace"
        )

        raise RuntimeError(
            f"Google API HTTP error {e.code}:\n{body}"
        )

    return data, len(usable), len(selected)


def main():
    shell_mode = "--shell" in sys.argv

    if not shell_mode:
        print("===== AUTOMATIC NODE LOCATION =====")

    api_key = load_api_key()

    if not shell_mode:
        print("Scanning Wi-Fi...")

    aps = scan_wifi()

    if not shell_mode:
        print("Detected APs :", len(aps))

    data, usable_count, selected_count = get_google_location(
        api_key,
        aps
    )

    if not shell_mode:
        print("Usable APs   :", usable_count)
        print("Selected APs :", selected_count)

    location = data.get("location", {})

    lat = location.get("lat")
    lon = location.get("lng")
    accuracy = data.get("accuracy")

    if lat is None or lon is None:
        raise RuntimeError(
            "Google response did not contain latitude/longitude."
        )

    if shell_mode:
        print(f"LAT={lat}")
        print(f"LON={lon}")
        print(f"ACCURACY={accuracy}")
    else:
        print("")
        print("Node ID    : birdnode1")
        print("Latitude   :", lat)
        print("Longitude  :", lon)
        print("Accuracy   :", accuracy, "m")
        print("Source     : Google Wi-Fi Geolocation")


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        print("")
        print("ERROR:", e)
        raise SystemExit(1)
