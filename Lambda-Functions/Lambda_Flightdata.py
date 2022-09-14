import requests
import pymysql
import sqlalchemy
import pandas as pd
from datetime import datetime, date, timedelta
from pytz import timezone

def lambda_handler(event, context):

    # call get_cities-function to receive cities, use their latitude and longitude and put those values into a list for later use
    cities_df = get_cities()
    lat = cities_df["city_latitude"].to_list()
    lon = cities_df["city_longitude"].to_list()
    
    # call get_airpprt-function with the lat- and lon-list from above and clear airport-table; finally retrieving the ICAO for the aiports and store them in a list for later use
    airport_data = get_airport_data(lat, lon)
    airport_data = airport_data[~airport_data.name.str.contains("Air Base", case = False)]
    airport_data.drop_duplicates(subset = "icao", inplace = True)
    airport_data.drop(columns = ["shortName", "localCode"], inplace = True)
    airport_data.rename(columns = {"name": "airport_name",
                               "municipalityName": "municipality_name",
                               "countryCode": "country_code",
                               "location.lat": "airport_latitude",
                               "location.lon": "airport_longitude"},
                    inplace = True)
    airport_data.at[5, "municipality_name"] = "Düsseldorf"
    airport_data.at[6, "municipality_name"] = "Frankfurt am Main"
    airport_data = airport_data.merge(cities_df[["city_id", "city"]], how = "left", left_on = "municipality_name", right_on = "city")
    airport_data.drop(columns = ["city"], inplace = True)
    airport_data.reset_index(drop = True, inplace = True)
    icao = airport_data["icao"].to_list()
    
    # call flight_data-function with ICAO-list from above and clean data
    flight_data = get_flight_data(icao)
    flight_data.drop(columns = ["codeshareStatus", "isCargo", "movement.scheduledTimeUtc", "movement.quality", 
                             "aircraft.reg", "aircraft.modeS", "callSign", "movement.actualTimeLocal",
                             "movement.actualTimeUtc", "movement.gate", "movement.baggageBelt"], inplace = True)
    flight_data.rename(columns = {"number": "flight_number",
                              "movement.airport.icao": "departure_icao",
                              "movement.airport.iata": "departure_iata",
                              "movement.airport.name": "departure_airport",
                              "movement.scheduledTimeLocal": "scheduled_time",
                              "movement.terminal": "terminal",
                              "aircraft.model": "aircraft_model",
                              "airline.name": "airline"},
                   inplace = True)
    flight_data["scheduled_time"] = pd.to_datetime(flight_data["scheduled_time"])
    
    # connection to DB and send flight_data from above to SQL
    schema = "gans"
    host = 
    user = 
    password = 
    port = 
    con = f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"
    flight_data.to_sql("flights", if_exists = "append", con = con, index = False)

# function to get airport-data using API "AeroDataBox"
def get_airport_data(latitude, longitude):
    airport_list = []
    # check the length of the latitude and longitude lists to make sure they are equal
    assert len(latitude) == len(longitude)
    # set the API call to get airport data within 50km of the lat and lon being input and show 10 results
    url = "https://aerodatabox.p.rapidapi.com/airports/search/location/51.511142/-0.103869/km/50/10"
    querystring = {"withFlightInfoOnly":"true"}
    headers = {
        "X-RapidAPI-Key": ,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }
    test = requests.request("GET", url, headers = headers, params = querystring)
    if test.status_code >= 200 and test.status_code <= 299:
        for i in range(len(latitude)):
            url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{latitude[i]}/{longitude[i]}/km/50/10"
            querystring = {"withFlightInfoOnly":"true"}
            headers = {
                "X-RapidAPI-Key": , 
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers = headers, params = querystring)
            airport_df = pd.json_normalize(response.json()["items"])
            airport_list.append(airport_df)
    else:
        return -1
    airports_df = pd.concat(airport_list, ignore_index = True)
    return airports_df

# function to get flight-data using API "AeroDataBox"
def get_flight_data(icao):
    # use the datetime function in python to get today's and tomorrow's date to be used as inputs of the API call
    today = datetime.now().astimezone(timezone("Europe/Berlin")).date()
    tomorrow = (today + timedelta(days = 1))
    arrival_list = []
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/EHAM/{tomorrow}T10:00/{tomorrow}T22:00"
    querystring = {"withLeg":"false","direction":"Arrival","withCancelled":"false","withCodeshared":"true",
                   "withCargo":"false","withPrivate":"false","withLocation":"false"}
    headers = {
        "X-RapidAPI-Key": ,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }
    test = requests.request("GET", url, headers = headers, params = querystring)
    if test.status_code >= 200 and test.status_code <= 299:
        for code in icao:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{code}/{tomorrow}T10:00/{tomorrow}T22:00"
            querystring = {"withLeg":"false","direction":"Arrival","withCancelled":"false",
                           "withCodeshared":"true","withCargo":"false","withPrivate":"false",
                           "withLocation":"false"}
            headers = {
                "X-RapidAPI-Key": ,
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }
            response = requests.request("GET", url, headers = headers, params = querystring)
            arrival_df = pd.json_normalize(response.json()["arrivals"])
            arrival_df["arrival_icao"] = code
            arrival_list.append(arrival_df)
    else:
        return -1
    arrivals_df = pd.concat(arrival_list, ignore_index = True)  
    return arrivals_df   

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
