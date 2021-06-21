# Fetch data from openweather API and dump in Amazon S3 bucket
This repository contains python script that fetched data from openweather API *[https://openweathermap.org/api](https://openweathermap.org/api)*, parses the data and dumps current weather and hourly forecast jsons in ```weather_current/``` and ```forecast_hourly/``` folder, respectively in weatherdatalake bucket.

# Install dependencies
Run the following command on terminal:
```
pip3 install -r requirements.txt
```

# Use
Run ```python3 handler.py``` directly.

### Or
Use as Lambda function in Amazon Lambda and trigger ```hello``` function from a scheduler. 
