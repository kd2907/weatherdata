import io
import json
import requests
import boto3
from datetime import datetime

def s3_object():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id="AKIA3IDUKSIMXORQDN5C",
        aws_secret_access_key="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"
        )
    return s3

def collect_data(city_coordinates: list) -> tuple:
    """
    collects data from openweather api
    """
    status_codes = {}
    weather_data = {}
    for city_name, coordinates in city_coordinates.items():
        api_data = requests.get(f"https://api.openweathermap.org/data/2.5/onecall?lat={coordinates[0]}&lon={coordinates[1]}&units=metric&exclude=minutely,alerts&appid=9c5b1ede0700b6f2a72e2f1dd7fd4348")
        if api_data.status_code == 200:
            status_codes[city_name] = 200
            weather_data[city_name] = api_data.json()
        else:
            status_codes[city_name] = api_data.status_code
            weather_data[city_name] = f"Could not get weather data from city: {city_name}"

    return status_codes, weather_data

def repair_data(data: list, columns: list) -> list:
    """
    check if the data dict has all the required fields
    adds fields with None value if not already present
    """
    data_new = []
    for item in data:
        item_new = {}
        for col_name in columns:
            val = item.get(col_name, None)
            item_new[col_name] = val
        data_new.append(item_new)
    return data_new

def fix_rain(data_obj: dict) -> dict:
    rain = data_obj.get("rain", None)
    if isinstance(rain, dict):
        assert len(rain)==1
        rain = list(rain.values())[0]
    elif isinstance(rain, float):
        pass
    elif rain is not None:
        raise RuntimeError(f"Unknown data type in the field rain.\
            Expected dict or Nan, but found {type(rain)}")
    data_obj["rain"] = rain
    return data_obj

def fix_weather(data_obj: dict) -> dict:
    weather_dict = data_obj.pop('weather')[0]
    for key, val in weather_dict.items():
        new_key = "weather_"+key
        data_obj[new_key] = val
    return data_obj

def get_current_weather(weather_data: dict) -> list:
    data_current = []
    for city, data_object in weather_data.items():
        # take only first forecast value as this is most accurate
        data_obj_current = data_object["current"]
        data_obj_current = fix_weather(data_obj_current)
        data_obj_current = fix_rain(data_obj_current)
        data_obj_current["city"] = city

        data_current.append(data_obj_current)
    return data_current

def get_forecast_daily(weather_data: dict) -> list:
    data_current = []
    for city, data_object in weather_data.items():
        # take only first forecast value as this is most accurate
        data_obj_current = data_object["daily"][0]
        data_obj_current = fix_weather(data_obj_current)
        data_obj_current = fix_rain(data_obj_current)        
        data_obj_current["city"] = city
      
        data_current.append(data_obj_current)
    return data_current

def get_forecast_hourly(weather_data: dict) -> list:
    data_hourly = []
    num_hours = 12  # take only 12 timestamps
    for city, data_object in weather_data.items():
        # take only first 12 forecast values as they are more accurate
        data_obj_hourly = data_object["hourly"][:num_hours]
        flat_data_hourly = []
        for item in data_obj_hourly:
            item = fix_weather(item)
            item = fix_rain(item)
            item["city"] = city
            flat_data_hourly.append(item)
        data_hourly.extend(flat_data_hourly)
    return data_hourly

def dump_raw_api_data(s3: object, bucket_name: str, data: list, foldername: str) -> None:
    timestamp = datetime.strftime(datetime.now(), "%y%m%d_%H%M%S_%f")
    json_str = json.dumps(data)
    s3.Object(bucket_name, f'{foldername}/{timestamp}.json').put(Body=json_str)
    return

def dump_jsons(s3: object, bucket_name: str, data: list, foldername: str) -> None:
    for entry in data:
        city = entry["city"]
        dt = entry["dt"]
        jsonname = f"{city}_{dt}"
        dt = datetime.fromtimestamp(dt)
        dt = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        entry["dt"] = dt
        json_str = json.dumps(entry)
        s3.Object(bucket_name, f'{foldername}/{jsonname}.json').put(Body=json_str)
    return 

def hello(event, context):
    city_coordinates = {"Moscow": [55.7558, 37.6173],
                        "Amsterdam": [52.3676, 4.9041],
                        "Berlin": [52.5200, 13.4050],
                        "London": [51.5074, 0.1278]
                            }
    columns = ['dt', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', \
                'uvi', 'clouds', 'wind_speed', 'wind_deg', 'wind_gust', 'rain', \
                'weather_id', 'weather_main', 'weather_description', 'weather_icon',\
                 'city']
    status_codes, weather_data = collect_data(city_coordinates)

    # check that nothing went wrong with fetching the data
    all_status = list(status_codes.values())
    if all(item==200 for item in status_codes.values()):
        pass
    else:
        return status_codes

    data_current = get_current_weather(weather_data)
    forecast_hourly = get_forecast_hourly(weather_data)
    # forecast_daily = get_forecast_daily(weather_data)

    data_current = repair_data(data_current, columns)
    forecast_hourly = repair_data(forecast_hourly, columns)
    # forecast_daily = repair_data(forecast_daily, columns)

    s3 = s3_object()
    dump_jsons(s3, "weatherdatalake", data_current, "weather_current")
    dump_jsons(s3, "weatherdatalake", forecast_hourly, "forecast_hourly")
    # dump_jsons(s3, "weatherdatalake", forecast_daily, "forecast_daily")
    return status_codes

if __name__ == '__main__':
    status_codes = hello({}, "")
    print(status_codes)