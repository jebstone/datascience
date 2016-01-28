# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import, unicode_literals
import iopro.pyodbc as iopro  # iopro is an optimized database driver from Continuum.io
import numpy as np
import pandas as pd
#import pandas.io.data
#from pandas.tools.plotting import autocorrelation_plot
import statsmodels.api as sm
#from datetime import datetime
 
 
# Database Connection String Parameters connecting to a local database
db_config = {
    'Driver': '{SQL Server}',
    'Server': 'localhost',
    'Trusted_Connection': 'yes'
}
 
# Set up Time-Series configuration 
ts_config = {}
 
# Get historical bounds and other transaction info
def db_get_ts_config():
    """
    This is an initialization routine for the app. 
    App needs to know the range of purchase dates in order to distinguish between pre-history and
    days with zero sales. Sets a time-series config dict (ts_config) to contain these min and max dates.
    """
    db_connection = iopro.connect(**db_config)
    db_cursor = db_connection.cursor()
     
    db_cursor.execute("select * from dbo.vTransactionStats")    # Application needs to know, minimally, first and last overall transaction dates
    result = db_cursor.fetchone()
    ts_config["minPurchaseDate"] = result.minPurchaseDate
    ts_config["maxPurchaseDate"] = result.maxPurchaseDate   # Assumes the most recent PurchaseDate applies to all products, so zeros can be filled in appropriately for trending
    db_connection.close()
    del(db_cursor)
    del(db_connection)
     
 
# Get a list of distinct ProductIDs along with first/last sales dates
def db_get_productlist():
    """
    Connects to an existing database view containing the distinct ProductIDs for a given client, and returns those IDs as a list.
    This is highly suboptimal but works as a proof of concept.
    """
    db_connection = iopro.connect(**db_config) 
    db_cursor = db_connection.cursor()
    productIDs = []
     
    db_cursor.execute("exec TimeSeriesQueueGet")    # Expects a table or view containing distinct ProductIDs in a 'ProductID' int field
    for row in db_cursor.fetchall():
        productIDs.append(row[0])
         
    db_connection.commit()
    db_connection.close()
        
    return productIDs                         # Return result as a list of integers          
 
 
def db_get_trx_series(productID):
    """
    Accepts a single ProductID. Queries the profile database to get the DAILY sales counts for that single ProductID.
    This is then converted into a clean time series, bounded by the min and max sales dates, with all missing dates
    filled in with zero sales. 
     
    Returns a Pandas time-series object for further processing.
    """
    db_connection = iopro.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_cursor.execute("select * from dbo.fxTransactionSeries(?)", productID)
    result = db_cursor.fetchsarray()
    db_connection.close()
     
    ts_idx = pd.date_range(ts_config["minPurchaseDate"], ts_config["maxPurchaseDate"])
    df = pd.DataFrame(result)
    df.set_index("PurchaseDate", drop=True, append=False, inplace=True, verify_integrity=False)      # Set Pandas index to the date column
    ts = pd.Series(df["Purchases"])
    ts.index = pd.DatetimeIndex(ts.index)
    ts = ts.reindex(ts_idx, fill_value=0)
     
    return ts                               # Returns a Series indexed by Date, no missing dates and all zeros filled
 
 
def get_single_value(frame, position):
    tmpframe = frame.tail(position)
    tmplist = tmpframe.tolist()
    tmpval = tmplist[0]
    if np.isnan(tmpval):
        return 0
    else:
        return tmpval
     
       
def timeseries(productID):   
    """
    Accepts a single ProductID as a paremeter. Retrieves a time-series vector for that product,
    and creates several moving averages (e.g., ewma7) from that data to identify upward/downward trends.
    Plucks the last values from those moving averages and writes them to a ts_values dict.
    Attempts to separate seasonality from trend into two values (ts_cycle, ts_trend) and write to ts_values dict also.
     
    Loads all resulting weights to a DB for that ProductID.
     
    """
    ts = db_get_trx_series(productID) # Get a Time-Series vector for a specific product #1587
    ts_values = {}
     
    # Compute exponentially weighted moving averages (EWMAs) for specific time periods
    ewma7 = pd.Series(pd.ewma(ts, span=7, freq="D"))
    ewma14 = pd.Series(pd.ewma(ts, span=14, freq="D"))
    ewma30 = pd.Series(pd.ewma(ts, span=30, freq="D"))
     
    # Compute moving average convergence-divergence to identify strength and direction of trend
    # ASSUMES no partial days are provided; transaction counts are for a full day
    macd = pd.Series(ewma14 - ewma30)
     
    # Get the tail value or last value we observed from each of the EWMA calculations
    ts_values["macd"] = get_single_value(macd, 1)
    ts_values["ewma7"] = get_single_value(ewma7, 1)
    ts_values["ewma14"] = get_single_value(ewma14, 1)
    ts_values["ewma30"] = get_single_value(ewma30, 1)
      
    try:
        # Apply Hodrick-Prescott filter to separate out seasonality (ts_cycle) from overall linear trend (ts_trend)
        ts_cycle, ts_trend = sm.tsa.filters.hpfilter(ts.resample("M", how="sum"), 129600)
         
    except ValueError:
        #print("Skipping ValueError (sparse matrix) for ProductID=" + str(productID))   
        ts_values["ts_cycle"] = 0
        ts_values["ts_cycle_z"] = 0
        print(productID, "***********************************ERROR -- Time Series")
         
    else:
        ts_cycle_z = (ts_cycle - ts_cycle.mean()) / ts_cycle.std()
        #ts_trend_z = (ts_trend - ts_trend.mean()) / ts_trend.std()
        ts_values["ts_cycle"] = get_single_value(ts_cycle, 13)        
        ts_values["ts_cycle_z"] = get_single_value(ts_cycle_z, 13)
        #print("OK", productID, ts_values["ts_cycle"])
         
        print(productID, "-- Time Series Completed")
        db_update_weights(productID, ts_values)
 
 
 
def db_update_weights(productID, weights_dict):
    """
    Loads a set of weights to a timeseries weights table in the DB.
    Could benefit from some connection pooling all around.
     
    ** NOTE: Needs to actually DROP all these weights first, which isn't written yet...
    """
    db_connection = iopro.connect(**db_config) 
    db_cursor = db_connection.cursor()
     
    for k, v in weights_dict.items():
        db_cursor.execute("insert into dbo.TimeSeriesWeights_TMP values (?,?,?)", productID, k, v)
         
    db_connection.commit()
    db_connection.close()
    print(productID, "-- Loading Weights...")
     
     
 
def main(db):
    """
    Main program-flow logic. Sets a db_config parameter to the desired database,
    Gets required purchase-date parameters to apply to all ProductIDs,
    Gets the list of all known ProductIDs,
    Runs time-series extraction for daily sales totals for each ProductID (serially),
    and Writes the resulting weights to a database.
    """
    db_config["Database"] = db
    # Load queue file
    db_get_ts_config()
     
    # Load Product Table on initialization
    productIDs = db_get_productlist()
     
    for productID in productIDs:
        timeseries(productID)
        print()
 
         
     
    #print(ts_config["productIDList"][0:3])    
 
 
 
main("Company_Name")   
