"""Send weather readings to Siddhi, so Siddhi can spot broken equipment.

Fake mode (default): sends made-up readings, some of them broken on purpose.
    python send_weather.py

Real mode: fetches the newest temperature from DMI for each station and sends it.
    python send_weather.py --real
"""

import argparse
import json
import time
from urllib.request import Request, urlopen

SIDDHI_URL = "http://localhost:7073/weather"

# The stations we watch: Kastrup Lufthavn, Drogden Fyr, Skagen Fyr
STATIONS = ["06180", "06183", "06041"]

# Made-up readings, sent from top to bottom.
# value None means "the station sent nothing" (a missing value).
FAKE_READINGS = [
    # Normal weather, nothing should happen
    {"stationId": "06180", "value": 2.1},
    {"stationId": "06183", "value": 2.4},
    {"stationId": "06041", "value": 1.6},

    # 06183 sends nothing  ->  Siddhi should raise "DetectMissing"
    {"stationId": "06183", "value": None},

    # 06180 sends an impossible temperature  ->  "FlagSuspect"
    {"stationId": "06180", "value": 9999.0},

    # 06041 gets stuck on the same number 5 times  ->  "FlagSuspect"
    {"stationId": "06041", "value": 1.7},
    {"stationId": "06041", "value": 1.7},
    {"stationId": "06041", "value": 1.7},
    {"stationId": "06041", "value": 1.7},
    {"stationId": "06041", "value": 1.7},
]


def send_to_siddhi(reading):
    """Send one reading to Siddhi as JSON."""
    body = {"stationId": reading["stationId"], "parameterId": "temp_dry"}
    # Leave "value" out completely when it is missing; Siddhi then sees it as empty (null)
    if reading.get("value") is not None:
        body["value"] = reading["value"]

    request = Request(
        SIDDHI_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        print(f"Sent {body} -> HTTP {response.status}")


def fetch_from_dmi(station_id):
    """Get the newest temperature for one station from DMI's open data API."""
    url = (
        "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
        f"?stationId={station_id}&parameterId=temp_dry&limit=1"
    )
    try:
        with urlopen(Request(url), timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
        features = data.get("features", [])
        if not features:
            return {"stationId": station_id, "value": None}  # DMI has no reading: missing
        return {"stationId": station_id, "value": features[0]["properties"]["value"]}
    except Exception as error:
        print(f"Could not reach DMI for station {station_id}: {error}")
        return {"stationId": station_id, "value": None}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--real", action="store_true", help="use live DMI data instead of fake data")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds to wait between readings")
    args = parser.parse_args()

    if args.real:
        readings = [fetch_from_dmi(station) for station in STATIONS]
    else:
        readings = FAKE_READINGS

    for reading in readings:
        send_to_siddhi(reading)
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
