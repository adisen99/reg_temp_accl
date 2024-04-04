from statsmodels.tsa.seasonal import STL
import numpy as np
import xarray as xr


def calc_gmst(temp):
    # temp = temp.rename(dict(latitude = 'lat', longitude = 'lon'))
    try:
        weights = np.cos(np.deg2rad(temp.lat))
        weights.name = 'weights'
        temp_ts = temp.weighted(weights).mean(('lat', 'lon'))
        annual_temp = temp_ts.groupby('time.year').mean('time')
    except:
        weights = np.cos(np.deg2rad(temp.latitude))
        weights.name = 'weights'
        temp_ts = temp.weighted(weights).mean(('latitude', 'longitude'))
        annual_temp = temp_ts.groupby('time.year').mean('time')
    return annual_temp

def loess1d(x):
    res = STL(x, period=30).fit()
    return res.trend


def loess3d(x, dim):
    return xr.apply_ufunc(loess1d, x, input_core_dims=[[dim]], output_core_dims=[[dim]], vectorize=True, dask="parallelized")