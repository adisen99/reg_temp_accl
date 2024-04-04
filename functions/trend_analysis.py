import numpy as np
import pymannkendall as mk
import xarray as xr
from scipy import stats


def calc_trend(temp_data: xr.DataArray, start_year: int, end_year: int):
    if not isinstance(temp_data, xr.DataArray):
        raise TypeError("'xr.DataArray' input is required, not the %s" % (type(temp_data)))
    duration = end_year - start_year
    res = mk.hamed_rao_modification_test(temp_data.sel(year = slice(str(start_year), str(end_year))))
    return res.slope*10, res.p, duration


def get_all_trends(temp_data: xr.DataArray, initial_dur: int = 10):
    if not isinstance(temp_data, xr.DataArray):
        raise TypeError("'xr.DataArray' input is required, not the %s" % (type(temp_data)))
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
        

def calc_trend1d(x: np.ndarray):
    if np.nansum(x) != 0:
        res = mk.hamed_rao_modification_test(x)
        return res.slope*10
    else:
        return np.nan


def calc_trend_pval1d(x: np.ndarray):
    if np.nansum(x) != 0:
        res = mk.hamed_rao_modification_test(x)
        return res.p
    else:
        return np.nan


def calc_trend3d(da: xr.DataArray, dim: str):
    if not isinstance(da, xr.DataArray):
        raise TypeError("'xr.DataArray' input is required, not the %s" % (type(da)))
    return xr.apply_ufunc(calc_trend1d, da, input_core_dims=[[dim]], vectorize=True, dask='parallelized')


def calc_trend_pval3d(da: xr.DataArray, dim: str):
    if not isinstance(da, xr.DataArray):
        raise TypeError("'xr.DataArray' input is required, not the %s" % (type(da)))
    return xr.apply_ufunc(calc_trend_pval1d, da, input_core_dims=[[dim]], vectorize=True, dask='parallelized')


def  calc_sensitivity(best_data: xr.DataArray, temp_data: xr.DataArray, dim: str, duration=30, gap=4):
    if not len(best_data[dim]) == len(temp_data[dim]):
        raise TypeError(f"Both input DataArrays must have same length along dimension {dim}")
    i = 0
    trend_pattern_corrs = []
    trend_pattern_pvals = []
    trend_pattern_bias = []
    trend_pattern_fbias = []
    trend_pattern_rmse = []
    while i < (len(best_data[dim]) - duration):
        trend_best = calc_trend3d(best_data.isel(year = slice(i, i+duration)), dim=dim).to_numpy().flatten()
        trend_temp = calc_trend3d(temp_data.isel(year = slice(i, i+duration)), dim=dim).to_numpy().flatten()
        pattern_corr = stats.spearmanr(trend_best, trend_temp).statistic
        pattern_pval = stats.spearmanr(trend_best, trend_temp).pvalue
        trend_pattern_corrs.append(pattern_corr)
        trend_pattern_pvals.append(pattern_pval)
        #
        bias = np.mean(np.abs(trend_temp - trend_best)) # calculating the mean absolute bias
        frac_bias = 2 * np.mean((trend_temp - trend_best)/(trend_temp + trend_best))
        rmse = np.sqrt(np.mean((trend_temp - trend_best)**2))
        trend_pattern_bias.append(bias)
        trend_pattern_fbias.append(frac_bias)
        trend_pattern_rmse.append(rmse)
        print(best_data[dim][i].to_numpy())
        i += gap
    return np.array(trend_pattern_corrs), np.array(trend_pattern_pvals), \
        np.array(trend_pattern_bias), np.array(trend_pattern_fbias), np.array(trend_pattern_rmse)