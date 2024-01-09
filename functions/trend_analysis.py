import numpy as np
import pymannkendall as mk
import xarray as xr
# from scipy import stats

def calc_trend(temp_data, start_year, end_year):
    duration = end_year - start_year
    res = mk.hamed_rao_modification_test(temp_data.sel(year = slice(str(start_year), str(end_year))))
    return res.slope*10, res.p, duration


def get_all_trends(temp_data, initial_dur=10):
    start_years = np.arange(1850, 2026, 4)
    end_years = np.arange(1850, 2026, 4)
    slope = np.empty((len(start_years), len(end_years)))
    pval = np.empty((len(start_years), len(end_years)))
    duration = np.empty((len(start_years), len(end_years)))
    for i in range(len(start_years)):
        for j in range(len(end_years)):
            if end_years[j] < start_years[i] + initial_dur:
                slope[i, j], pval[i, j], duration[i, j] = np.NaN, np.NaN, np.NaN
            else:
                slope[i, j], pval[i, j], duration[i, j] = calc_trend(temp_data, start_years[i], end_years[j])
        # print(f"Completed {start_years[i]}", end='\r')
    return start_years, end_years, slope, pval, duration
        


def calc_trend1d(x):
    if np.nansum(x) != 0:
        res = mk.hamed_rao_modification_test(x)
        return res.slope*10
    else:
        return np.nan

def calc_trend_pval1d(x):
    if np.nansum(x) != 0:
        res = mk.hamed_rao_modification_test(x)
        return res.p
    else:
        return np.nan

def calc_trend3d(da, dim):
    return xr.apply_ufunc(calc_trend1d, da, input_core_dims=[[dim]], vectorize=True, dask='parallelized')

def calc_trend_pval3d(da, dim):
    return xr.apply_ufunc(calc_trend_pval1d, da, input_core_dims=[[dim]], vectorize=True, dask='parallelized')