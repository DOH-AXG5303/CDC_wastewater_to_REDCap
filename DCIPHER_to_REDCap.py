from credentials import token_pid353
from credentials import redcap_api_url
import pandas as pd
import redcap


path = 
Y:\Confidential\DCHS\PHOCIS\Surveillance\COVID-19 Wastewater Surveillance\DCIPHER_download\csv




columns_map = {'pcr_target_below_lod':'sars_cov2_below_lod',
                 'pcr_target_avg_conc':"sars_cov2_avg_conc"}


def rename_columns(df):
    """
    rename from DCIPHER convention to REDCap format
    """
    df = df.copy()
    df = df.rename(columns = columns_map)
    
    return df

def long_to_wide(df):
    """
    samples are tested multiple times with different gene targets ("pcr_gene_target").
    reduce to only n1 and n2 gene targets. Convert to wide format from 'sars_cov2_below_lod' and 'sars_cov2_avg_conc'. 
    to n1_sars_cov2_below_lod, n1_sars_cov2_below_lod and n1_sars_cov2_avg_conc, n1_sars_cov2_avg_conc. 
    """
    
    df = df.copy()
    
    df = df[df["pcr_gene_target"].isin(["n1","n2"])].copy()

    df_pivot_A = df.pivot(columns = "pcr_gene_target",
                          index = "sample_id",
                          values = ['sars_cov2_below_lod','sars_cov2_avg_conc'])
    #renaming to flatted multi-index columns
    new_cols = ['{1}_{0}'.format(*tup) for tup in df_pivot_A.columns]
    df_pivot_A.columns = new_cols


    df_pivot_B = df.pivot(columns = "pcr_gene_target",
                          index = "sample_id",
                          values = df.columns[~df.columns.isin(['sars_cov2_below_lod','sars_cov2_avg_conc'])])
    #drop all n2 results to flatten multi-index (all other results are independant of pcr_target and associated to sample.
    df_pivot_B = df_pivot_B.drop(columns = "n2", level = 1) #drop all "N2" values in subindex
    df_pivot_B.columns = df_pivot_B.columns.droplevel(1)
    
    df_wide = df_pivot_A.join(df_pivot_B)
    
    return df_wide