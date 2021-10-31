import pandas as pd
import numpy as np
from mstrio.connection import Connection
from mstrio.application_objects.datasets import SuperCube
from functools import reduce


def vdo_invoice_calc(df):
    """Calculates only bill values."""
    df['REVENUE'] = df['REVENUE'].astype(float)
    df['PRICE_NO_DISCOUNT'] = df['PRICE_NO_DISCOUNT'].astype(float)
    df['PRICE_TAG'] = df['PRICE_TAG'].astype(int)
    df['ENTER_DATE'] = pd.DatetimeIndex(df['ENTER_DATE'])
    df['ENTER_DATE'] = df['ENTER_DATE'].dt.date
    df['LEAVE_DATE'] = pd.DatetimeIndex(df['LEAVE_DATE'])
    df['LEAVE_DATE'] = df['LEAVE_DATE'].dt.date
    df['BIRTH_DATE'] = pd.DatetimeIndex(df['BIRTH_DATE'])
    df['MAIN_CATEGORY_CODE'] = df['MAIN_CATEGORY_CODE'].astype(str)
    df.loc[df['MAIN_CATEGORY_CODE'].isin(['115', '129', '147', '161']), 'BUNDLE'] = 'YES'
    df.loc[(df['MAIN_CATEGORY_CODE'] == '44') & (df['PRICE_TAG'] >= 25), 'BUNDLE'] = 'YES'
    df['SESSION_NO'] = df['CUSTOMERNO'].astype(str) + df['DATE_TAG'].astype(str)
    df['AGE'] = pd.DatetimeIndex(df['ENTER_DATE']).year - pd.DatetimeIndex(df['BIRTH_DATE']).year
    df['STORE_NAME'].replace(to_replace=['Aadvark'], value='ADVRK', inplace=True)

    df_bndl_revenue = df[df['BUNDLE'] == 'YES'].groupby('SESSION_NO').agg({'REVENUE': 'sum'})
    df_bndl_revenue = pd.DataFrame(df_bndl_revenue).reset_index()
    df_bndl_revenue.columns = ['SESSION_NO', 'BUNDLE_REVENUE']

    df_bndl_individual_rvn = df[df['PAYMENT_TAG'] == 'P'].groupby('SESSION_NO').agg({'REVENUE': 'sum'})
    df_bndl_individual_rvn = pd.DataFrame(df_bndl_individual_rvn).reset_index()
    df_bndl_individual_rvn.columns = ['SESSION_NO', 'BUNDLE_IND_SUM']

    merged = df_bndl_revenue.merge(df_bndl_individual_rvn, on='SESSION_NO', how='outer',
                                   left_index=False, right_index=False)
    merged['RATIO'] = merged['BUNDLE_REVENUE'] / merged['BUNDLE_IND_SUM']

    df_last = df.merge(merged[['SESSION_NO', 'RATIO']], on='SESSION_NO', how='outer', left_index=False, right_index=False)
    df_last.loc[df_last['PAYMENT_TAG'] == 'P', 'UNBUNDLED_DISCOUNTED'] = df_last['REVENUE'] * df_last['RATIO']
    df_last.loc[df_last['PAYMENT_TAG'] != 'P', 'UNBUNDLED_DISCOUNTED'] = df_last['REVENUE']
    df_last.loc[df_last['MAIN_CATEGORY_CODE'].isin(['102', '108', '109', '110', '111', '112', '114', 
                                                    '115', '116', '117', '120', '121', '123', '124', '125', '129',
                                                    '132', '133', '134', '136', '141', '149',
                                                    '150', '157', '166', '175', '179', '182']), 'MATCHING_CAT'] = 'SUPPLEMENT'
    df_last.loc[df_last['MAIN_CATEGORY_CODE'].isin(['113', '122', '128', '145', '163']), 'MATCHING_CAT'] = 'CONSUMABLES'
    df_last.loc[df_last['MAIN_CATEGORY_CODE'].isin(['103', '118', '135']), 'MATCHING_CAT'] = 'HOMEBUILD'
    df_last.loc[df_last['MAIN_CATEGORY_CODE'].isin(['106', '107', '131', '154', '181']), 'MATCHING_CAT'] = 'PROFESSIONAL'
    df_last.loc[(df_last['MAIN_CATEGORY_CODE'] == '04') & (df_last['PRICE_TAG'].between(10, 60)), 'MATCHING_CAT'] = 'PROFESSIONAL'
    df_last.loc[(df_last['MAIN_CATEGORY_CODE'] == '04') & (df_last['PRICE_TAG'] == 100), 'MATCHING_CAT'] = 'HOMEBUILD'
    df_last.loc[(df_last['MAIN_CATEGORY_CODE'] == '04') & (df_last['PRICE_TAG'] == 70), 'MATCHING_CAT'] = 'HOMEBUILD'
    df_last.loc[df_last['MATCHING_CAT'].isna(), 'MATCHING_CAT'] = 'Other'  # to detect uncategorized goods

    seller_fee_includes = df_last[(df_last['MAIN_CATEGORY_CODE'] == '04') & (df_last['PRICE_TAG'] == 1)]['CUSTOMERNO'].unique()
    depot_fee_includes = df_last[df_last['MAIN_CATEGORY_CODE'] == '45']['CUSTOMERNO'].unique()

    pivot = pd.pivot_table(df_last[(df_last['BUNDLE'] != 'YES') & (~df_last['PAYMENT_TAG'].isin(['M', 'S', 'X']))],
                           index='CUSTOMERNO', columns='MATCHING_CAT', values='UNBUNDLED_DISCOUNTED', aggfunc=np.sum)
    pivot = pd.DataFrame(pivot).reset_index()

    pivot_info = df_last.groupby('CUSTOMERNO').agg({'SESSION_NO': 'first',
                                                 'STATE': 'first',
                                                 'GENDER': 'first',
                                                 'AGE': 'first',
                                                 'STORE_CODE': 'first',
                                                 'STORE_NAME': 'first',
                                                 'OPERATION': 'first',
                                                 'NAME_SURNAME': 'first',
                                                 'SELLER_NAME': 'first',
                                                 'ENTER_DATE': 'first',
                                                 'LEAVE_DATE': 'first'})
    pivot_info = pd.DataFrame(pivot_info).reset_index()
    pivot_info.columns = ['CUSTOMERNO', 'SESSION_NO', 'STATE', 'GENDER', 'AGE', 'STORE CODE', 'STORE NAME', 'OPERATION',
                           'NAME SURNAME', 'SELLER NAME', 'ENTER DATE', 'LEAVE DATE']
    pivot_info.loc[pivot_info['CUSTOMERNO'].isin(seller_fee_includes), 'SELLER FEE INCLUDES'] = 'YES'
    pivot_info.loc[pivot_info['SELLER FEE INCLUDES'].isna(), 'SELLER FEE INCLUDES'] = 'NO'
    pivot_info.loc[pivot_info['CUSTOMERNO'].isin(depot_fee_includes), 'DEPOT FEE INCLUDES'] = 'YES'
    pivot_info.loc[pivot_info['DEPOT FEE INCLUDES'].isna(), 'DEPOT FEE INCLUDES'] = 'NO'

    dashboard = reduce(lambda x, y: pd.merge(x, y, on='CUSTOMERNO', how='outer', left_index=False, right_index=False),
                       [pivot_info, pivot])

    writer = pd.ExcelWriter('VDO_output.xlsx', engine='xlsxwriter')
    dashboard.to_excel(writer, sheet_name='sheet1')
    writer.save()

    return dashboard


def mstr_conn(dataframe, how):
    """Connects to mstr web API using mstrio_py
    Writes first argument with the type in second argument"""
    base_url = "http://192.168.1.1:1010/MicroStrategyLibrary/api"
    username = "john.doe"
    password = "awesome_character_combination"
    conn = Connection(base_url, username, password, application_id="xxxxxxxxxxxxxxxxxxxxxx", login_mode=1)

    if how == 'create':
        ds = SuperCube(connection=conn, name='VDO_Dashboard_Data', description='created by MSTR API')
        ds.add_table(name='table1', data_frame=dataframe, update_policy="replace")
        ds.create()
        ds.certify()
        print("Successfully created.")
        conn.close()
    elif how == 'update':
        ds = SuperCube(connection=conn, id='xxxxxxxxxxxxxxxxxxxxxx')
        ds.add_table(name='table1', data_frame=dataframe, update_policy="replace")
        ds.update()
        print("Successfully updated.")
        conn.close()
    else:
        print("Wrong MSTR operation type! Connection will be closed.")
        conn.close()



