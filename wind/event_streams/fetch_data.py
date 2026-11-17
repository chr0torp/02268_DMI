""" fetch wether data from the API and send json """


from urllib.request import Request, urlopen
import json 


def recive_event(url):

    request = Request(
        url,
        headers={"Content-Type": "application/json"},
        method="GET",
    )
    with urlopen(request) as response:
        response_body = response.read().decode("utf-8")
        return response.status, response_body

def clean_data(data):
    cleaned_data = {}
    if "features" in data:
        cleaned_data = {
            "id": data['features'][0]['id'],
            "parameterId": data['features'][0]['properties']['parameterId'],
            "value": data['features'][0]['properties']['value'],
            "stationId": data['features'][0]['properties']['stationId']
        }
    return cleaned_data

def clean_cloudburst_data(data):
    cleaned_data = {}
    if "features" in data:
        cleaned_data = {
            "id": data['features'][0]['id'],
            "parameterId": data['features'][0]['properties']['parameterId'],
            "value": (data['features'][0]['properties']['value'] + data['features'][1]['properties']['value'] + data['features'][2]['properties']['value']),
            "stationId": data['features'][0]['properties']['stationId']
        }
    return cleaned_data


def get_wind_data():
    try:
        url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06183&parameterId=wind_speed&limit=1"
        status, response_body = recive_event(url)
        if status != 200:
            raise Exception(f"Failed to fetch wind data. Status code: {status}")
        events = json.loads(response_body)
        mean_wind_speed = clean_data(events)
    except Exception as e:
        print(f"Error fetching wind data: {e}")
        return None
    
    try:
        url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06183&parameterId=wind_gust_past10min&limit=1"
        status, response_body = recive_event(url)
        if status != 200:
            raise Exception(f"Failed to fetch wind gust data. Status code: {status}")
        events = json.loads(response_body)
        wind_gust = clean_data(events)
    except Exception as e:
        print(f"Error fetching wind gust data: {e}")
        return None


    merged_data = {
        "id": mean_wind_speed.get("id"),
        "parameterId": "wind_data",
        "mean_wind_speed": mean_wind_speed.get("value"),
        "wind_gust": wind_gust.get("value")
    }

    return merged_data

def get_cloudburst_data():
    try:
        url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06180&parameterId=precip_past10min&limit=3"
        status, response_body = recive_event(url)
        if status != 200:
            raise Exception(f"Failed to fetch cloudburst data. Status code: {status}")
        events = json.loads(response_body)
        cloudburst_data = clean_cloudburst_data(events)
    except Exception as e:
        print(f"Error fetching cloudburst data: {e}")
        return None

    return cloudburst_data



if __name__ == "__main__":
    # url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06183&parameterId=wind_speed&limit=1"
    # url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06183&parameterId=wind_gust_past10min&limit=1"
    # status, response_body = recive_event(url)
    # events = json.loads(response_body)
    # print(f"Status: {status}")
    # print(f"Response body: {response_body}")
    # cleaned_events = clean_data(events)
    # print(f"Cleaned events: {cleaned_events}")


    print("Fetching wind data...")
    wind_data = get_wind_data()
    if wind_data is not None:
        print(f"wind data: {wind_data}")

    print("Fetching cloudburst data...")
    cloudburst_data = get_cloudburst_data()
    if cloudburst_data is not None:
        print(f"cloudburst data: {cloudburst_data}")
















