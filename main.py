# !/usr/bin/python
"""
***********************************************************************************
Name                 : qgis-plugin-downloads
Description          : Download the information about qgis plugins.
Date                 : 5/July/2023
copyright            : (C) 2023 by Marios S. Kyriakou, University of Cyprus,
                       KIOS Research and Innovation Center of Excellence (KIOS CoE)
email                : mariosmsk@gmail.com
***********************************************************************************
"""
import datetime
import requests
import pandas as pd
import time
import json
import os

num_pages = 150 # number of pages

# Read all plugin information
all_tables = []
for i in range(1, num_pages):
    url = f"https://plugins.qgis.org/plugins/?page={i}&sort=name&"
    response = requests.get(url)
    if response.status_code != 200:
        break

    tables = pd.read_html(response.content)
    tables = [table.drop(table.columns[0], axis=1) for table in tables]
    tables = [table.drop(table.columns[1], axis=1) for table in tables]
    table = tables[0][pd.to_numeric(tables[0]['Downloads'], errors="coerce").notna()]
    all_tables.append(table)
    time.sleep(0.5)

# Get current date
current_datetime = datetime.datetime.now().date()
datetime_string = current_datetime.strftime("%Y-%m-%d")
combined_table = pd.concat(all_tables, ignore_index=True)

# Create if not exists the folder `data`
os.makedirs("data", exist_ok=True)

# Create lists with plugin information
plugin_data = []
plugin_data_ts = []
for index, row in combined_table.iterrows():
    data = row.to_dict()
    plugin_name = data[list(data.keys())[0]]
    plugin_name = plugin_name.replace('/', '_')
    plugin_name = plugin_name.replace(' ', '_')
    plugin_download = row['Downloads']
    author = data['Author']
    latest_update = data['Latest Plugin Version']
    stars = data['Stars (votes)']
    stars = stars.replace('(', '')
    stars = stars.replace(')', '')
    pl = {
        "plugin_name": plugin_name,
        "author": author,
        "latest_update": latest_update,
        "downloads": plugin_download,
        "stars": stars,
        "date": datetime_string
    }
    plugin_data.append(pl)
    pl_ts = {
        "plugin_name": plugin_name,
        "downloads": plugin_download,
        "date": datetime_string
    }
    plugin_data_ts.append(pl_ts)

# Create the `plugins.json` file / the latest information
filename_plugins = f"data/plugins.json"
with open(filename_plugins, "w") as json_file:
    json.dump(plugin_data, json_file)

# Create the `plugins.csv` file / the latest information
df = pd.DataFrame(plugin_data)
df.to_csv("data/plugins.csv", index=False)

# Make the date columns
df_ts = pd.DataFrame(plugin_data_ts)
df_pivot = df.pivot(index="plugin_name", columns="date", values="downloads")

filename_ts_plugins = f"data/plugins_time_series.csv"
if not os.path.exists(filename_ts_plugins):
    df_pivot.to_csv(filename_ts_plugins)

existing_data = pd.read_csv(filename_ts_plugins)

# Add the missing plugins in the file
last_plugins = set(df['plugin_name'])
existing_plugins = set(existing_data['plugin_name'])
missing_plugins = []
if last_plugins == existing_plugins:
    pass
else:
    missing_plugins = last_plugins - existing_plugins

for plugin in missing_plugins:
    index = df['plugin_name'].tolist().index(plugin)
    new_row = {"plugin_name": plugin, f"{datetime_string}": df['downloads'][index]}
    existing_data = pd.concat([existing_data, pd.DataFrame([new_row])], ignore_index=True)

# Update the file `plugins_time_series.csv`
merged_df = pd.merge(existing_data, df_pivot, on='plugin_name', how='inner')
merged_df.to_csv(filename_ts_plugins, index=False)

