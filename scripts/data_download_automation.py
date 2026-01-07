import requests
import pandas as pd
import os
from datetime import date, timedelta

# Create folder if it doesn't exist
os.makedirs("data/raw", exist_ok=True)

# City coordinates
cities = {
    "Houston": (29.7604, -95.3698),
    "London": (51.5072, -0.1276),
    "Pune": (18.5204, 73.8567),
    "Chennai": (13.0827, 80.2707)
}


# Weather parameters
params = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "relative_humidity_2m_max"
]

# Fetch and save data
def fetch_data(lat, lon,start_date,end_date):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily={','.join(params)}&timezone=auto"
    )
    #join method concatenates the elements of the params list using the "," seperator
    
    response = requests.get(url) # sends a http get request to the url 
    data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(data['daily'])

    '''
    daily could be like 
    daily:{
        time:['2025-01-01', '2025-01-02'],
        temp:[25, 35],
        humidity:[25,20]
    }
    '''    
    return df

for city,(lat,lon) in cities.items():
    file_path=f"data/raw/{city}_weather_data.csv"
    
    end_date=date.today() #whether snapshot or incremental we need data till today

    #if the data for the city already exists then the start date will be the last date + 1(incremental download)
    if os.path.exists(file_path):
        existing_df=pd.read_csv(file_path)
        existing_df["time"]=pd.to_datetime(existing_df["time"])
        last_fetched=existing_df["time"].max().date()
        start_date=last_fetched+timedelta(days=1)
    else:
        existing_df=pd.DataFrame()
        start_date=end_date-timedelta(days=3*365) #fetch 3 yrs data from today

    #fetching new data only
    if start_date<=end_date:
        print(f"Fetching data for {city} from {start_date} to {end_date}")
        new_df=fetch_data(lat,lon,start_date,end_date)
        combined_df=pd.concat([existing_df,new_df],ignore_index=True)
        combined_df.drop_duplicates(subset=["time"],inplace=True) #duplicate check is performed on column time
        combined_df.to_csv(file_path,index=False)
        print(f"Updated the data for {city}")
    else:
        print(f"Everything is up to date for {city}")    

