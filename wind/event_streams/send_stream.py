"""Send an ordered event stream to the Siddhi customer input endpoint."""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fetch_data import get_wind_data




def load_wind_data():
    wind_data = get_wind_data()
    if wind_data is None:
        raise ValueError("Failed to fetch wind data")
    return wind_data

def send_event(url, body, timeout):
    print(f"Sending event to {url} with body: {body}")
    body = json.dumps(body).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        response_body = response.read().decode("utf-8")
        return response.status, response_body


def main():
    try:
        wind_data = load_wind_data()
        wind_data = {'id': '0ad81c4a-a445-73d9-c049-21a5badf3asa', 'parameterId': 'wind_data', 'mean_wind_speed': 2.49, 'wind_gust': 2.3466}
        print(f"Fetched wind data: {wind_data}")
        url = "http://localhost:7071/dmi"
        timeout = 10
        status, response = send_event(url, wind_data, timeout)
        print(f"Successfully HTTP {status} {response}")

    except (HTTPError, URLError, TimeoutError) as error:
        print(f"Error sending event: {error}")
        raise SystemExit(1)
    



if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
