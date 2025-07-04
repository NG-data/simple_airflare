import requests as rs
import pandas as pd
import airflow as aw

headers = {'x-access-token': '167317b476e1808633c659a8bbb35b13'}

url = "https://api.travelpayouts.com/v2/prices/month-matrix"

querystring = {"currency": "rub",
               "show_to_affiliates": "true",
               "origin": "IKT",
               "destination": "HKT",
               'month': '2025-09-01'}

response = rs.get(url, headers=headers, params=querystring)
response_json = response.json()
