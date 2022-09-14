import requests
import pymysql
import sqlalchemy
import pandas as pd

def lambda_handler(event, context):
    
    # call get_cities-function to receive cities and put them into a list for later use
    cities_df = get_cities()
    cities = cities_df["city"].to_list()

    # call get_weather-function with the cities-list from above and clear weather-table for use in MySQL/AWS-DB
    weather_data = get_weather_data(cities)
    weather_data = weather_data[["pop", "dt_txt", "main.temp", "main.feels_like", "main.humidity", "clouds.all", "wind.speed", "wind.gust", "city"]]
    weather_data.rename(
        columns = {"pop": "precip_prob", 
        "dt_txt": "forecast_time", 
        "main.temp": "temperature", 
        "main.feels_like": "feels_like", 
        "main.humidity": "humidity", 
        "clouds.all": "cloudiness", 
        "wind.speed": "wind_speed", 
        "wind.gust": "wind_gust",}, 
        inplace = True)
    weather_data = weather_data.merge(cities_df[["city_id", "city"]], how = "left", on = "city")
    weather_data["forecast_time"] = pd.to_datetime(weather_data["forecast_time"])
    
    # connection to DB and send weather_data from above to SQL
    schema = "gans"
    host = 
    user = 
    password =
    port = 
    con = f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"
    weather_data.to_sql("weathers", if_exists = "append", con = con, index = False)

# function to get weather-data using API "OpenWeatherMap"
def get_weather_data(cities):
    weather_list = []
    url = "http://api.openweathermap.org/data/2.5/forecast?q=Boston&appid=&units=metric"
    test = requests.get(url)
    if test.status_code >= 200 and test.status_code <= 299:
        for city in cities:
            url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid=&units=metric"
            weather = requests.get(url)
            weather_df = pd.json_normalize(weather.json()["list"])
            weather_df["city"] = city
            weather_list.append(weather_df)
    else:
        return -1
    weather_combined = pd.concat(weather_list, ignore_index = True)
    return weather_combined

# function to connect with DB to retrieve cities (cities have been put into database first using Jupyter notebook)
def get_cities():
    schema = "gans"
    host = 
    user = 
    password =
    port = 
    con = f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"
    cities_df = pd.read_sql_table("cities", con=con)
    return cities_df
    


    
