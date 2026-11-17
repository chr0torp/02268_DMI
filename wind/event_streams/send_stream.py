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

test_data_wind = [{'id': '0ad81c4a-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 21.49, 'wind_gust': 26.3466},
                {'id': 'shfreiw2-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 2.49, 'wind_gust': 2.3466},
                {'id': '12345678-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 25.49, 'wind_gust': 26.3466},
                {'id': '66665554-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 33.49, 'wind_gust': 26.3466},
                {'id': '9r2gd77r-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 21.49, 'wind_gust': 26.3466}]

test_data_cloudburst = [{'id': 'ab5d26bf-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'precip_past10min', 'value': 0.0, 'stationId': '06180'},
                        {'id': '11111111-2222-1111-2222-111111111111', 'parameterId': 'precip_past10min', 'value': 16.0, 'stationId': '06180'},
                        {'id': '22222222-3333-2222-3333-222222222222', 'parameterId': 'precip_past10min', 'value': 20.0, 'stationId': '06180'},
                        {'id': '33333333-4444-3333-4444-333333333333', 'parameterId': 'precip_past10min', 'value': 30.0, 'stationId': '06180'},
                        {'id': '44444444-5555-4444-5555-444444444444', 'parameterId': 'precip_past10min', 'value': 40.0, 'stationId': '06180'}]


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

def send_response(url, id):
    body = {
        "id": id,
        "status": "received",
    }
    body = json.dumps(body).encode("utf-8")
    request = Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    with urlopen(request, timeout=10) as response:
        response_body = response.read().decode("utf-8")
        return response.status, response_body



def main():
    # for wind_data in test_data_wind:
    #     try:
    #         print(f"Fetched wind data: {wind_data}")
    #         url = "http://localhost:7071/dmi"
    #         timeout = 10
    #         status, response = send_event(url, wind_data, timeout)
    #         print(f"Successfully HTTP {status} {response}")

    #     except (HTTPError, URLError, TimeoutError) as error:
    #         print(f"Error sending event: {error}")
    #         raise SystemExit(1)
    #     time.sleep(1)  


    for cloudburst_data in test_data_cloudburst:
        try:
            print(f"Fetched cloudburst data: {cloudburst_data}")
            url = "http://localhost:7071/dmi"
            timeout = 10
            status, response = send_event(url, cloudburst_data, timeout)
            print(f"Successfully HTTP {status} {response}")

        except (HTTPError, URLError, TimeoutError) as error:
            print(f"Error sending event: {error}")
            raise SystemExit(1)
        time.sleep(1)

       
    
    
    # try:
    #     # wind_data = load_wind_data()

    #     print(f"Fetched wind data: {wind_data}")
    #     url = "http://localhost:7071/dmi"
    #     timeout = 10
    #     status, response = send_event(url, wind_data, timeout)
    #     print(f"Successfully HTTP {status} {response}")

    # except (HTTPError, URLError, TimeoutError) as error:
    #     print(f"Error sending event: {error}")
    #     raise SystemExit(1)
    



if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
