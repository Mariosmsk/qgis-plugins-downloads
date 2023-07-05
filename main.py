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

num_pages = 150  # number of pages

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
plugin_data = {}
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
    plugin_data[f"{plugin_name}"] = {
        "author": author,
        "latest_update": latest_update,
        "downloads": plugin_download,
        "stars": stars,
        "date": datetime_string
    }

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

# Make the date columns
df_ts = pd.DataFrame(plugin_data_ts)

filename_ts_plugins = f"data/plugins_time_series.csv"
duplicates = df_ts.duplicated(subset='plugin_name', keep=False)
df_ts.loc[duplicates, 'plugin_name'] = df_ts.loc[duplicates, 'plugin_name'] + '_' + \
                                       df_ts.groupby('plugin_name').cumcount().astype(str)
df_pivot = df_ts.pivot(index='plugin_name', columns='date', values='downloads')
if not os.path.exists(filename_ts_plugins):
    df_pivot.to_csv(filename_ts_plugins)

existing_data = pd.read_csv(filename_ts_plugins)

# Add the missing plugins in the file
last_plugins = set(plugin_data.keys())
existing_plugins = set(existing_data['plugin_name'])
missing_plugins = []
if last_plugins == existing_plugins:
    pass
else:
    missing_plugins = last_plugins - existing_plugins
    for plugin in missing_plugins:
        new_row = {"plugin_name": plugin, f"{datetime_string}": plugin_data[plugin]['downloads']}
        existing_data = pd.concat([existing_data, pd.DataFrame([new_row])], ignore_index=True)

# Update the file `plugins_time_series.csv`
existing_data = existing_data.rename(columns={'downloads': f'{datetime_string}'})
df_pivot.drop(columns=[datetime_string], inplace=True)
merged_df = pd.merge(df_pivot, existing_data[['plugin_name', datetime_string]], on='plugin_name', how='inner', suffixes=('', '_y'))
merged_df.to_csv(filename_ts_plugins, index=False)