# snotel.py
"""
LOAD PYTHON LIBRARIES
"""
import netCDF4 as nc
import pyproj
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import ulmo
from shapely.geometry import Point
import contextily as ctx
import os
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')

def snotel_fetch(sitecode, variablecode, start_date='1950-01-01', end_date=today):
    values_df = None
    wsdlurl = 'https://hydroportal.cuahsi.org/Snotel/cuahsi_1_1.asmx?WSDL'
    try:
        # start by requesting data from the server
        print('Requesting Data from Server...')
        site_values = ulmo.cuahsi.wof.get_values(wsdlurl,sitecode, variablecode, start=start_date, end=end_date)
        # convert to pandas dataframe
        values_df = pd.DataFrame.from_dict(site_values['values'])
        # parse the datetime values to Pandas Timestamp object
        print('Cleaning Data...')
        values_df['datetime'] = pd.to_datetime(values_df['datetime'], utc=False)
        # set df index to datetime
        values_df = values_df.set_index('datetime')
        # convert values to float and replace -9999 with NaN
        values_df['value'] = pd.to_numeric(values_df['value']).replace(-9999,np.nan)
        # rename values column after variable code
        values_df.rename(columns = {'value':variablecode}, inplace = True)
        # remove lower quality records
        values_df = values_df[values_df['quality_control_level_code']=='1']
        print('Done!')
    except:
        print('Unable to fetch %s' % variablecode)
    return values_df


def getStation_output(outpath ,fname, sitecode, start, end,lat,lon, savefile = 0,variables=['SNOTEL:SNWD_D','SNOTEL:WTEQ_D']):
    lat = np.array([lat])
    lon = np.array([lon])
    # site_name = 'MuckaMuck'

    # collect snowdepth #
    try:
        snod_snotel = snotel_fetch(sitecode, variables[0],start_date=start,end_date=end)
        snod_datetime = snod_snotel.date_time_utc.values
        snod_value = snod_snotel[variables[0]].values * 0.0254 # convert to meters


        # collect swe #
        swe_snotel = snotel_fetch(sitecode, variables[1],start_date=start,end_date=end)
        swe_datetime = swe_snotel.date_time_utc.values
        swe_value = swe_snotel[variables[1]].values * 0.0254 # convert to meters

        if np.array_equal(snod_datetime,swe_datetime) == False:
            print('SNOD AND SWE TIME SERIES ARE DIFFERENT')
            return fname
        else:
            print('SAME TIME ARRAY')

        # xarray #
        ds = xr.Dataset(
                data_vars = dict(
                    SWED = (["Time"],swe_value),
                    SNOD = (["Time"],snod_value)),
                coords={"Time": ("Time",snod_datetime),
                        "XLAT": (("south_north"),lat),
                        "XLONG": (("east_west"),lon),})

        ds.SWED['units'] = 'meters'
        ds.SWED['standard_name'] = 'snow water equivalent'
        ds.SNOD['units'] = 'meters'
        ds.SNOD['standard_name'] = 'snow depth'

        ds['Time'] = pd.DatetimeIndex(ds['Time'].values)

        name_ = fname.replace(' ','_')

        if savefile == 1:
            ds.to_netcdf(outpath + name_ + '.nc',
                        encoding = {"Time":
                                        {'dtype' : 'float64',
                                         'units' : 'hours since 1901-01-01 00:00:00',
                                         'calendar' : 'standard'}})
        return
    except:
        return fname

    return tuple(output)
