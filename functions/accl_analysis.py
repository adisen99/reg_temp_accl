import numpy as np
import xarray as xr
from scipy import stats
import pymannkendall as mk


def calc_accl(temp_data, duration=30, gap=4):
    i = 0
    trend_arr = []
    pval_arr = []
    slope_arr = []
    distance_arr = []
    start_years = []
    end_years = []
    while i < (len(temp_data) - duration):
        res = mk.hamed_rao_modification_test(temp_data[i:(i+duration)])
        trend = res.trend
        pval = res.p
        slope = res.slope*10
        # distance = 2022 - (((temp_data.year[i+duration]) + temp_data.year[i])/2)
        distance = 2022 - temp_data.year[i+duration]
        start_years.append(temp_data.year[i])
        end_years.append(temp_data.year[i+duration])
        trend_arr.append(trend)
        pval_arr.append(pval)
        slope_arr.append(slope)
        distance_arr.append(distance)
        i += gap
    return {
        'slope': np.array(slope_arr), 
        'distance': np.array(distance_arr), 
        'pval': np.array(pval_arr), 
        'trend':np.array(trend_arr), 
        'sy': np.array(start_years), 
        'ey': np.array(end_years)
    }


def calc_accl1d(temp_data, duration=100, gap=4):
    if np.nansum(temp_data) != 0:
        i = 0
        # pval_arr = []
        slope_arr = []
        distance_arr = []
        while i < (len(temp_data) - duration):
            res = mk.hamed_rao_modification_test(temp_data[i:(i+duration)])
            # pval = res.p
            slope = res.slope*10
            distance = 2022 - (i+1850+duration)
            slope_arr.append(slope)
            distance_arr.append(distance)
            i += gap
        res = stats.linregress(distance_arr, slope_arr)
        return -res.slope
    else:
        return np.nan


def calc_pval1d(temp_data, duration=100, gap=4):
    if np.nansum(temp_data) != 0:
        i = 0
        # pval_arr = []
        slope_arr = []
        distance_arr = []
        while i < (len(temp_data) - duration):
            res = mk.hamed_rao_modification_test(temp_data[i:(i+duration)])
            # pval = res.p
            slope = res.slope*10
            distance = 2022 - (i+1850+duration)
            slope_arr.append(slope)
            distance_arr.append(distance)
            i += gap
        res = stats.linregress(distance_arr, slope_arr)
        return res.pvalue
    else:
        return np.nan


def calc_accl3d(da, dim, duration=100, gap=4):
    return xr.apply_ufunc(calc_accl1d, da, input_core_dims=[[dim]], kwargs=dict(duration=duration, gap=gap), vectorize=True, dask="parallelized")


def calc_pval3d(da, dim, duration=100, gap=4):
    return xr.apply_ufunc(calc_pval1d, da, input_core_dims=[[dim]], kwargs=dict(duration=duration, gap=gap), vectorize=True, dask="parallelized")