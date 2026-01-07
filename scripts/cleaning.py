import pandas as pd
import os

input_folder = "data/raw"
output_folder = "data/processed"
os.makedirs(output_folder, exist_ok=True)

cities = ["Houston", "London", "Pune", "Chennai"]

dataframes = []

for city in cities:
    file_path = f"{input_folder}/{city}_weather_data.csv"
    df = pd.read_csv(file_path)
    df["city"] = city  # Add city column
    df["date"] = pd.to_datetime(df["time"]) #creates a new column date by converting the values in the time column (incase it was in the string format) to datetime format
    # example : if time - > '25-01-01' then date -> Timestamp ('25-01-01 00:00:00' ) but in csv we can only see the date alone not time as the time is midnight for all rows
    
    df.drop(columns=['time'],inplace=True) # we drop the time column hence
    #inplace = True ensures that a new dataframe is not created after dropping the time column and the original one
    # is only modified. some methods like drop(), sort_values() etc create a new dataframe in pandas. 
    
    # Check for missing values
    df = df.dropna() # this drops entire row if it contains missing values or NaN 
     # or df.fillna(method='ffill') this fills the missing value with previous row value 
    
    dataframes.append(df)

# Combine all cities into one DataFrame. we are doing this because analysing will be difficult if we have 4 different dataframes instead of one as we need to write seperate code for each city.also training one model for all the cities becomes messy. 
combined_df = pd.concat(dataframes, ignore_index=True)
# ignore index ensures that it creates new indices sequentially for the new dataframe instead of 0,1...and then again 0,1...

# Rename columns for clarity
combined_df.rename(columns={
    "temperature_2m_max": "temp_max",
    "temperature_2m_min": "temp_min",
    "precipitation_sum": "precipitation",
    "wind_speed_10m_max": "wind_speed",
    "relative_humidity_2m_max": "humidity"
}, inplace=True) # inplace to enusre that a new df is not created 

# Save cleaned dataset
output_path = f"{output_folder}/cleaned_weather_data.csv"
combined_df.to_csv(output_path, index=False)

print(f"Cleaned and combined dataset saved at {output_path}")
