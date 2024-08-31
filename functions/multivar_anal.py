"""
Multivariate analyis using HDI, AOD, pop, and accl data
"""


import numpy as np
import pandas as pd


def get_demarcated_data(accl_dat, pop_dat, pop_dens_dat, aodc_dat, hdi_dat, hdi_cutoff, pop_dens_cutoff):
    pop_high_hdi = pop_dat.where((hdi_dat >= hdi_cutoff) & ((pop_dens_dat) > pop_dens_cutoff)).to_numpy().flatten()
    accl_high_hdi = accl_dat.where((hdi_dat >= hdi_cutoff) & ((pop_dens_dat) > pop_dens_cutoff)).to_numpy().flatten()
    aodc_high_hdi = aodc_dat.where((hdi_dat >= hdi_cutoff) & ((pop_dens_dat) > pop_dens_cutoff)).to_numpy().flatten()

    pop_low_hdi = pop_dat.where((hdi_dat < hdi_cutoff) & ((pop_dens_dat) > pop_dens_cutoff)).to_numpy().flatten()
    accl_low_hdi = accl_dat.where((hdi_dat < hdi_cutoff) & ((pop_dens_dat) > pop_dens_cutoff)).to_numpy().flatten()
    aodc_low_hdi = aodc_dat.where((hdi_dat < hdi_cutoff) & ((pop_dens_dat) > pop_dens_cutoff)).to_numpy().flatten()

    df_low = pd.DataFrame(dict(pop = np.log10(pop_low_hdi), accl=accl_low_hdi, aodc=aodc_low_hdi))
    df_high = pd.DataFrame(dict(pop = np.log10(pop_high_hdi), accl=accl_high_hdi, aodc=aodc_high_hdi))

    return df_low, df_high