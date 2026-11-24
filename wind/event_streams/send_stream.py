"""Send an ordered event stream to the Siddhi customer input endpoint."""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import webbrowser

from fetch_data import get_wind_data

test_data_wind = [{'id': '0ad81c4a-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 21.49, 'wind_gust': 26.3466, 'stationId': '06183'},
                {'id': 'shfreiw2-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 2.49, 'wind_gust': 2.3466, 'stationId': '06183'},
                {'id': '12345678-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 25.49, 'wind_gust': 26.3466, 'stationId': '06183'},
                {'id': '66665554-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 33.49, 'wind_gust': 26.3466, 'stationId': '06183'},
                {'id': '9r2gd77r-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 21.49, 'wind_gust': 26.3466, 'stationId': '06183'},]

test_data_cloudburst = [{'id': 'ab5d26bf-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'cloudburst_data', 'rain': 0.0, 'stationId': '06180'},
                        {'id': '11111111-2222-1111-2222-111111111111', 'parameterId': 'cloudburst_data', 'rain': 16.0, 'stationId': '06180'},
                        {'id': '22222222-3333-2222-3333-222222222222', 'parameterId': 'cloudburst_data', 'rain': 20.0, 'stationId': '06180'},
                        {'id': '33333333-4444-3333-4444-333333333333', 'parameterId': 'cloudburst_data', 'rain': 30.0, 'stationId': '06180'},
                        {'id': '44444444-5555-4444-5555-444444444444', 'parameterId': 'cloudburst_data', 'rain': 40.0, 'stationId': '06180'}]

test_data_snow = [{'id': 'snow1111-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'snow_data', 'snow': 18.0, 'stationId': '06180'},
                    {'id': 'snow2222-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'snow_data', 'snow': 10.0, 'stationId': '06180'},
                    {'id': 'snow3333-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'snow_data', 'snow': 28.0, 'stationId': '06180'},
                    {'id': 'snow4444-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'snow_data', 'snow': 20.0, 'stationId': '06180'},
                    {'id': 'snow5555-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'snow_data', 'snow': 25.0, 'stationId': '06180'}]

test_data_waterlevel = [{'id': 'water1111-1111-9d2d-34c4-111111111111', 'waterlevel': 27.0, 'stationId': '30357', 'area': 'Østersøen/indre farvande', 'parameterId': 'waterlevel_data'},
                        {'id': 'water2222-2222-9d2d-34c4-222222222222', 'waterlevel': 130.0, 'stationId': '30357', 'area': 'Østersøen/indre farvande', 'parameterId': 'waterlevel_data'},
                        {'id': 'water3333-3333-9d2d-34c4-333333333333', 'waterlevel': 150.0, 'stationId': '30357', 'area': 'Østersøen/indre farvande', 'parameterId': 'waterlevel_data'},
                        {'id': 'water4444-4444-9d2d-34c4-444444444444', 'waterlevel': 280.0, 'stationId': '30357', 'area': 'Østersøen/indre farvande', 'parameterId': 'waterlevel_data'},
                        {'id': 'water5555-5555-9d2d-34c4-555555555555', 'waterlevel': 32.0, 'stationId': '30357', 'area': 'Østersøen/indre farvande', 'parameterId': 'waterlevel_data'},
                        {'id': 'water6666-20101-747rhfhc88ee', 'waterlevel': 126.0, 'stationId': '20101', 'area': 'Skagerrak/Limfjorden', 'parameterId': 'waterlevel_data'},
                        {'id': 'water7777-20101-747rhfhc88ee', 'waterlevel': 136.0, 'stationId': '20101', 'area': 'Skagerrak/Limfjorden', 'parameterId': 'waterlevel_data'},
                        {'id': 'water8888-21009-747rhfhc88ee', 'waterlevel': 146.0, 'stationId': '20101', 'area': 'Skagerrak/Limfjorden', 'parameterId': 'waterlevel_data'},
                        {'id': 'water9999-21009-747rhfhc88ee', 'waterlevel': 164.0, 'stationId': '20101', 'area': 'Skagerrak/Limfjorden', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1010-25149-747rhfhc88ee', 'waterlevel': 174.0, 'stationId': '21009', 'area': 'Vestkysten', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1111-25149-747rhfhc88ee', 'waterlevel': 194.0, 'stationId': '21009', 'area': 'Vestkysten', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1212-25149-747rhfhc88ee', 'waterlevel': 288.0, 'stationId': '21009', 'area': 'Vestkysten', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1313-25149-747rhfhc88ee', 'waterlevel': 304.0, 'stationId': '21009', 'area': 'Vestkysten', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1414-25149-747rhfhc88ee', 'waterlevel': 250.0, 'stationId': '25149', 'area': 'Vadehavet', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1515-25149-747rhfhc88ee', 'waterlevel': 392.0, 'stationId': '25149', 'area': 'Vadehavet', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1616-25149-747rhfhc88ee', 'waterlevel': 500.0, 'stationId': '25149', 'area': 'Vadehavet', 'parameterId': 'waterlevel_data'},
                        {'id': 'water1717-25149-747rhfhc88ee', 'waterlevel': 200.0, 'stationId': '25149', 'area': 'Vadehavet', 'parameterId': 'waterlevel_data'}
                        ]

test_one = [{'id': 'ab5d26bf-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'cloudburst_data', 'rain': 0.0, 'stationId': '06180'},
            {'id': 'shfreiw2-a49e-73d9-c049-21a5badf3bcb', 'parameterId': 'wind_data', 'mean_wind_speed': 2.49, 'wind_gust': 2.3466, 'stationId': '06183'},
            {'id': 'snow2222-7cd6-d557-bc9e-33619baaa0e3', 'parameterId': 'snow_data', 'snow': 1.0, 'stationId': '06180'},
            {'id': 'water1111-1111-9d2d-34c4-111111111111', 'waterlevel': 27.0, 'stationId': '30357', 'area': 'Østersøen/indre farvande', 'parameterId': 'waterlevel_data'},
            {'id': 'water6666-20101-747rhfhc88ee', 'waterlevel': 126.0, 'stationId': '20101', 'area': 'Skagerrak/Limfjorden', 'parameterId': 'waterlevel_data'},
            {'id': 'water1010-25149-747rhfhc88ee', 'waterlevel': 174.0, 'stationId': '21009', 'area': 'Vestkysten', 'parameterId': 'waterlevel_data'},
            {'id': 'water1717-25149-747rhfhc88ee', 'waterlevel': 200.0, 'stationId': '25149', 'area': 'Vadehavet', 'parameterId': 'waterlevel_data'}]

test_data_all = test_data_wind + test_data_cloudburst + test_data_snow + test_data_waterlevel


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

def data_generator(data_list):
    for data in data_list:
        try:
            print(f"Fetched data: {data}")
            url = "http://localhost:7071/dmi"
            timeout = 10
            status, response = send_event(url, data, timeout)
            print(f"Successfully HTTP {status} {response}")

        except (HTTPError, URLError, TimeoutError) as error:
            print(f"Error sending event: {error}")
            raise SystemExit(1)
        time.sleep(1)

def data_showof(data_list):
    for data in data_list:
        try:
            print(f"Fetched data: {data}")
            url = "http://localhost:7071/dmi"
            timeout = 10
            status, response = send_event(url, data, timeout)
            print(f"Successfully HTTP {status} {response}")
            time.sleep(1)
            if data.get("parameterId") == "waterlevel_data":
                show_url = f"http://localhost:8081/operate/processes?process=Process_waterlevel&version=1&active=true&incidents=true&completed=true&canceled=true"
            elif data.get("parameterId") == "wind_data":
                show_url = f"http://localhost:8081/operate/processes?process=Process_wind&version=1&active=true&incidents=true&completed=true&canceled=true"
            elif data.get("parameterId") == "cloudburst_data":
                show_url = f"http://localhost:8081/operate/processes?process=Process_cloudburst&version=1&active=true&incidents=true&completed=true&canceled=true"
            elif data.get("parameterId") == "snow_data":
                show_url = f"http://localhost:8081/operate/processes?process=Process_Snow&version=1&active=true&incidents=true&completed=true&canceled=true"
            else:
                break
            webbrowser.open(show_url)


        except (HTTPError, URLError, TimeoutError) as error:
            print(f"Error sending event: {error}")
            raise SystemExit(1)




def main():
    # data_generator(test_data_wind)
    # data_generator(test_data_cloudburst)
    # data_generator(test_data_snow)
    # data_generator(test_data_waterlevel)

    data_showof(test_data_all)

    # data_showof(test_one)

    
    
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
