# -*- coding: utf-8 -*-
"""
Retrieve airfare pricing data, for a list of routekeys, from an internal API. Load to a database.
"""

from __future__ import print_function, division, absolute_import, unicode_literals
import requests
import json
import datetime
import pyodbc as db
import datetime
import time


# Database Connection String Parameters connecting to a local database
#db_config = {
#    'Driver': '{MySQL ODBC 5.1 Driver}',
#    'Server': '',
#    'Database': 'advice_data',
#    'user': '',
#    'password': ''
#}

db_config = {
    'Driver': '{SQL Server}',
    'Server': '',
    'Database': 'advice_data',
    'user': '',
    'password': ''
}

app_config = {
    'minDepartDate': '20140222',
    'maxDepartDate': '20140601'
}

   
    


routekeys = ['DFW.LAX.US',
             'CHI.NYC.US',
             'BOS.WAS.US',
             'HOU.YCC.US',
             'ORL.LAS.US',
             'ATL.SEA.US']



def dt(intDate):
""" Accepts a date in integer format (20150101) and converts to a date """

    strDate = str(intDate)
    year = int(strDate[0:4])
    month = int(strDate[4:6])
    day = int(strDate[6:8])
    s = datetime.datetime(year, month, day)
    return s
    


def loop_getLowDepartPrices(routekey):
""" Accepts a list of routekeys. Calls the pricing API for each routekey, retrieves JSON results, and writes to a database """

    db_connection = db.connect(**db_config)
    print("Opening database connection...")
    db_cursor = db_connection.cursor()
    
    for routekey in routekeys:
        
        params = routekey.split('.')
        
        param_dict = {}
        param_dict["origin"] = params[0]
        param_dict["destination"] = params[1]
        param_dict["pos"] = params[2]
        param_dict["minDepartDate"] = app_config["minDepartDate"]
        param_dict["maxDepartDate"] = app_config["maxDepartDate"]

        advice_json = getLowDepartPrices(param_dict)            # Who ya gonna call?
        
        err_list = ""
        for error in advice_json.get("errors"):
            err_list = err_list + error
            
        print("Loading results to database...")        
        for record in advice_json.get("prices"):
            db_cursor.execute("insert into dbo.getLowDepartPrices2 values (?,?,?,?,?,?,?,?,?)", datetime.datetime.now(), param_dict["origin"], param_dict["destination"], param_dict["pos"], dt(param_dict["minDepartDate"]), dt(param_dict["maxDepartDate"]), err_list, dt(record.get("date")), record.get("price"))
            db_connection.commit()
        #print(advice_json)
  
    print("Closing DB connection...")
    db_connection.close()
    
        

def getLowDepartPrices(kwargs):
""" Request data from the pricing API for a single routekey and return JSON """

    url = "http://.../getLowDepartPrices"
    print("Requesting data via API...")
    r = requests.get(url, params=kwargs)
    jsondata = json.loads(r.content)
    return jsondata


loop_getLowDepartPrices(routekeys)

    
#print(advice_getLowDepartPrices(origin='DFW', destination='LAX', minDepartDate='20140210', maxDepartDate="20140211", pos='US'))
