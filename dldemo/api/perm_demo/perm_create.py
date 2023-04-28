from requests import get, post, delete, put
from pprint import pprint
import random
import pandas as pd
import numpy as np


## create Users
url = "http://localhost:5001/core4/api/v1"
signin = get(url + "/login?username=admin&password=hans")
token = signin.json()["data"]["token"]
h = {"Authorization": "Bearer " + token}
perm = "api://dldemo.api.perm_demo.api_demo.*"
methods = ["c", "r", "u", "d"]


demo_perms = [str(perm + "/" + i) for i in methods]

for i in demo_perms:
    name = "test" + i[-1:]
    rv = post(url + "/roles", headers=h,
              json={
                  "name": name,
                  "realname": name,
                  "role": ["standard_user"],
                  "email": name + "@example.com",
                  "passwd": "test",
                  "perm": [i]
              })
    rv.raise_for_status()


# special job user
name = "test_job"
rv = post(url + "/roles", headers=h,
          json={
              "name": name,
              "realname": name,
              "role": ["standard_user"],
              "email": name + "@example.com",
              "passwd": "test",
              "perm": ["job://dldemo.api.perm_demo.data_demo.GenDemoData/x", "api://core4.*"]
          })
rv.raise_for_status()

# special protected access
name = "testp1"
rv = post(url + "/roles", headers=h,
          json={
              "name": name,
              "realname": name,
              "role": ["standard_user"],
              "email": name + "@example.com",
              "passwd": "test",
              "perm": ["api://dldemo.api.perm_demo.api_demo.ApiDemo.*", "app://dldemo/protected1"]
          })
rv.raise_for_status()


name = "testp2"
rv = post(url + "/roles", headers=h,
          json={
              "name": name,
              "realname": name,
              "role": ["standard_user"],
              "email": name + "@example.com",
              "passwd": "test",
              "perm": ["api://dldemo.api.perm_demo.api_demo.ApiDemo.*", "app://bla"]
          })
rv.raise_for_status()

name = "testpa"
rv = post(url + "/roles", headers=h,
          json={
              "name": name,
              "realname": name,
              "role": ["standard_user"],
              "email": name + "@example.com",
              "passwd": "test",
              "perm": ["api://dldemo.api.perm_demo.api_demo.ApiDemo.*", "app://dldemo/protected2", "app://dldemo/protected1"]
          })
rv.raise_for_status()

