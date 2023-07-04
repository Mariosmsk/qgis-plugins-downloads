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

import requests
import pandas as pd
import time
import json
import os

num_pages = 150  # number of pages
all_tables = []
for i in range(1, num_pages):
    url = f"https://plugins.qgis.org/plugins/?page={i}&sort=name&"

    response = requests.get(url)
    if response.status_code != 200:
        break

    tables = pd.read_html(response.content)
    tables = [table.drop(table.columns[0], axis=1) for table in tables]
    table = tables[0][pd.to_numeric(tables[0]['Downloads'], errors="coerce").notna()]
    all_tables.append(table)
    time.sleep(0.5)

combined_table = pd.concat(all_tables, ignore_index=True)

os.makedirs("jsons", exist_ok=True)

for index, row in combined_table.iterrows():
    data = row.to_dict()
    plugin_name = data[list(data.keys())[0]]
    plugin_name = plugin_name.replace('/', '_')
    filename = f"jsons/{plugin_name}.json"
    with open(filename, "w") as json_file:
        json.dump(data, json_file)

combined_table.to_csv("qgis-plugins-downloads.csv", index=False)
