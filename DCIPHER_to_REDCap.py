from credentials import token_pid353
from credentials import token_pid171
from credentials import redcap_api_url
import pandas as pd
import redcap


# path =
# Y:\Confidential\DCHS\PHOCIS\Surveillance\COVID-19 Wastewater Surveillance\DCIPHER_download\csv


#rename columns
columns_map = {
                'pcr_target_below_lod':'sars_cov2_below_lod',
                'pcr_target_avg_conc':"sars_cov2_avg_conc",
                'pcr_target_units': 'sars_cov2_units',
                'pcr_target_std_error': 'sars_cov2_std_error',
                'pcr_target_cl_95_lo': 'sars_cov2_cl_95_lo',
                'pcr_target_cl_95_up': 'sars_cov2_cl_95_up',
                #'pcr_gene_target_ref': 'pcr_target_ref', #not relavent
                }

keep_columns = [
                'sars_cov2_below_lod',
                "sars_cov2_avg_conc",
                'sars_cov2_units',
                'sars_cov2_std_error',
                'sars_cov2_cl_95_lo',
                'sars_cov2_cl_95_up',
                #'pcr_target_ref',

                'sample_id', #need for index
                'pcr_gene_target', #needed for long to wide

                #all in-common columns
                'collection_storage_temp',
                'collection_storage_time',
                'collection_water_temp',
                'concentration_method',
                'conductivity',
                'equiv_sewage_amt',
                'extraction_method',
                'flow_rate',
                'hum_frac_chem_conc',
                'hum_frac_mic_conc',
                'hum_frac_mic_unit',
                'inhibition_adjust',
                'inhibition_detect',
                'inhibition_method',
                'lod_sewage',
                'ntc_amplify',
                'other_norm_conc',
                'other_norm_unit',
                'ph',
                'pre_conc_storage_temp',
                'pre_conc_storage_time',
                'pre_ext_storage_temp',
                'pre_ext_storage_time',
                'pretreatment',
                'pretreatment_specify',
                'quality_flag',
                'rec_eff_percent',
                'sample_collect_date',
                'sample_collect_time',
                'test_result_date',
                'time_zone',
                'tot_conc_vol',
                'tss'
                ]


def rename_columns(df):
    """
    rename from DCIPHER convention to REDCap format
    """
    df = df.copy()
    df = df.rename(columns = columns_map)

    df = df[keep_columns].copy()

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
    df_pivot_B = df_pivot_B.drop("pcr_gene_target", axis =1) #data is captured in new column names with prefix n1_ or n2_

    df_wide = df_pivot_A.join(df_pivot_B)

    return df_wide

def map_values(df):
    """
    Change DCIPHER values to REDCap coded values
    """

    df = df.copy()


    pretreatment = {
                    'yes': 1,
                    'no': 0
                    }

    units = {
            'copies/l wastewater': 1,
            'log10 copies/L wastewater': 2,
            'copies/g wet sludge': 3,
            'log10 copies/g wet sludge': 4,
            'copies/g dry sludge': 5,
            'log10 copies/g dry sludge': 6
            }

    hum_frac_chem_unit = {
                    'copies/L wastewater': 1,
                    'log10 copies/L wastewater': 2,
                    'copies/g wet sludge': 3,
                    'log10 copies/g wet sludge': 4,
                    'copies/g dry sludge': 5,
                    'log10 copies/g dry sludge': 6,
                    'micrograms/L wastewater': 7,
                    'log10 micrograms/L wastewater': 8,
                    'micrograms/g wet sludge': 9,
                    'log10 micrograms/g wet sludge': 10,
                    'micrograms/g dry sludge': 11,
                    'log10 micrograms/g dry sludge': 12
                    }

    conc_method = {
                    'membrane filtration with addition of mgcl2': 'mf-mgcl2',
                    'membrane filtration with sample acidification': 'mf-acid',
                    'membrane filtration with acidification and mgcl2': 'mf-acid-mgcl2',
                    'membrane filtration with no amendment': 'mf',
                    'membrane filtration with addition of mgcl2, membrane recombined with separated solids': 'mf-mgcl2-addsolids',
                    'membrane filtration with sample acidification, membrane recombined with separated solids': 'mf-acid-addsolids',
                    'membrane filtration with acidification and mgcl2, membrane recombined with separated solids': 'mf-acid-mgcl2-addsolids',
                    'membrane filtration with no amendment, membrane recombined with separated solids': 'mf-addsolids',
                    'peg precipitation': 'peg',
                    'ultracentrifugation': 'ultracentrifugation',
                    'skimmed milk flocculation': 'skimmilk',
                    'beef extract flocculation': 'beefextract',
                    'promega wastewater large volume tna capture kit': 'promega-tna',
                    'centricon ultrafiltration': 'uf-centricon',
                    'amicon ultrafiltration': 'uf-amicon',
                    'hollow fiber dead end ultrafiltration': 'uf-hf-deadend',
                    'innovaprep ultrafiltration': 'uf-innovaprep',
                    'no liquid concentration, liquid recombined with separated solids': 'noconc-addsolids',
                    'ceres nanotrap': 'ceresnano',
                    'none': '13'
                    }

    extract_meth = {
                    'qiagen allprep powerviral dna/rna kit': 'qiagen-viral',
                    'qiagen allprep powerfecal dna/rna kit': 'qiagen-fecal',
                    'qiagen allprep dna/rna kit': 'qiagen',
                    'qiagen rneasy powermicrobiome kit': 'qiagen-rneasy-power',
                    'qiagen powerwater kit': 'qiagen-powerwater',
                    'qiagen rneasy kit': 'qiagen-rneasy',
                    'qiagen qiaamp buffers with epoch columns': 'qiagen-qiaamp-epoch',
                    'promega ht tna kit': 'promega-ht-tna',
                    'promega automated tna kit': 'promega-ht-auto',
                    'promega manual tna kit': 'promega-manual-tna',
                    'promega wastewater large volume tna capture kit': 'promega-ww-largevol-tna',
                    'nuclisens automated magnetic bead extraction kit': 'nuclisens-auto-magbead',
                    'phenol chloroform': 'phenol',
                    'chemagic viral dna/rna 300 kit': 'chemagic300',
                    'trizol, zymo mag beads w/ zymo clean and concentrator': 'trizol-zymomagbeads-zymo',
                    '4s method': '4smethod',
                    'zymo quick-rna fungal/bacterial miniprep #r2014': 'zymoquick-r2014',
                    'thermo magmax microbiome ultra nucleic acid isolation kit': 'magmax',
                    'none': 'none'
                    }

    #lis_of_dicts = [pretreatment, units, hum_frac_mic_unit, hum_frac_chem_unit, conc_method, extract_meth]
    fields_to_map = {'pretreatment': pretreatment,
                     "sars_cov2_units":units,
                     'hum_frac_mic_unit':units,
                     'other_norm_unit': hum_frac_chem_unit,
                     "concentration_method": conc_method,
                     "extraction_method": extract_meth}

    for key, value in fields_to_map.items():
        df[key] = df[key].map(value)

    return df


def change_dtypes(df):
    """
    """
    df = df.copy()

    dtypes = {
        'pretreatment':"Int64",
         "sars_cov2_units":"Int64",
         'hum_frac_mic_unit':"Int64",
         'other_norm_unit':"Int64"
            }

    df = df.astype(dtypes,)


    #Handling timestamp fields
    timestamps = ["sample_collect_date",
             "test_result_date",
              "sample_collect_time"]

    df[timestamps] = df[timestamps].apply(pd.to_datetime, errors = "coerce")
    df["sample_collect_time"] = df["sample_collect_time"].dt.strftime('%H:%M')


    return df
