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
            "value": data['features'][0]['properties']['value'],
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
        "wind_gust": wind_gust.get("value"),
        "stationId": mean_wind_speed.get("stationId")
    }

    return merged_data




def clean_cloudburst_data(data):
    cleaned_data = {}
    if "features" in data:
        cleaned_data = {
            "id": data['features'][0]['id'],
            "rain": (data['features'][0]['properties']['value'] + data['features'][1]['properties']['value'] + data['features'][2]['properties']['value']),
            "stationId": data['features'][0]['properties']['stationId']
        }
    return cleaned_data

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

    cloudburst_data["parameterId"] = "cloudburst_data"

    return cloudburst_data

def clean_Snow_data(data):
    cleaned_data = {}
    if "features" in data:
        cleaned_data = {
            "id": data['features'][0]['id'],
            "snow": (data['features'][0]['properties']['value'] + data['features'][1]['properties']['value'] + data['features'][2]['properties']['value'] +    
                      data['features'][3]['properties']['value'] + data['features'][4]['properties']['value'] + data['features'][5]['properties']['value']),
            "stationId": data['features'][0]['properties']['stationId']
        }
    return cleaned_data

def get_Snow_data():
    try:
        url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06180&parameterId=precip_past1h&limit=6"
        status, response_body = recive_event(url)
        if status != 200:
            raise Exception(f"Failed to fetch snow data. Status code: {status}")
        events = json.loads(response_body)
        snow_data = clean_Snow_data(events)
    except Exception as e:
        print(f"Error fetching snow data: {e}")
        return None

    snow_data["parameterId"] = "snow_data"

    return snow_data




def clean_WaterLevel_data(data):
    cleaned_data = {}
    if "features" in data:
        if data['features'][0]['properties']['stationId'] == '25149':
            area = "Vadehavet"
        elif data['features'][0]['properties']['stationId'] == '21009':
            area = "Vestkysten"
        elif data['features'][0]['properties']['stationId'] == '20101':
            area = "Skagerrak/Limfjorden"
        elif data['features'][0]['properties']['stationId'] == '30357':
            area = "Østersøen/indre farvande"
        else:
            area = "Unknown"
        cleaned_data = {
            "id": data['features'][0]['id'],
            "waterlevel": data['features'][0]['properties']['value'],
            "stationId": data['features'][0]['properties']['stationId'],
            "area": area
        }
    return cleaned_data



# 30357   Østersøen/indre farvande
# 20101	Skagerrak/Limfjorden
# 21009	Vestkysten
# 25149	vadehavet

def get_WaterLevel_data(station):
    try:
        url = "https://opendataapi.dmi.dk/v2/oceanObs/collections/observation/items?stationId=" + station + "&parameterId=sealev_dvr&limit=1"
        print(url)
        
        status, response_body = recive_event(url)
        if status != 200:
            raise Exception(f"Failed to fetch water level data. Status code: {status}")
        events = json.loads(response_body)
        waterlevel_data = clean_WaterLevel_data(events)
    except Exception as e:
        print(f"Error fetching water level data: {e}")
        return None

    waterlevel_data["parameterId"] = "waterlevel_data"

    return waterlevel_data


if __name__ == "__main__":


    # print("Fetching wind data...")
    # wind_data = get_wind_data()
    # if wind_data is not None:
    #     print(f"wind data: {wind_data}")

    # print("Fetching cloudburst data...")
    # cloudburst_data = get_cloudburst_data()
    # if cloudburst_data is not None:
    #     print(f"cloudburst data: {cloudburst_data}")

    # print("Fetching snow data...")
    # snow_data = get_Snow_data()
    # if snow_data is not None:
    #     print(f"snow data: {snow_data}")    

    print("Fetching water level data...")
    waterlevel_data = get_WaterLevel_data("30357")
    if waterlevel_data is not None:
        print(f"water level data: {waterlevel_data}")

















