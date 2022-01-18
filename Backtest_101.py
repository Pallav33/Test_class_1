import psycopg2
import tempfile
import pandas as pd
import os
import numpy as np


DBHOST = os.environ['DBHOST']
DBUSER = os.environ['DBUSER']
DBPASS = os.environ['DBPASS']

stock_name = 'BANKNIFTY'
Open_time = '09:15:59'
Entry_time = '09:20:59'
Close_time = '09:30:59'
Exit_time = '15:15:59'

def connect(dbname):
    conn = psycopg2.connect(host=DBHOST,
                database=dbname,
                user=DBUSER,
                password=DBPASS,
                port=5432)
    return conn

def query_db(conn, query):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        db_results = pd.read_csv(tmpfile)
        return db_results

TABLES = [
'troptions_jan_1_10',
'troptions_jan_11_20',
'troptions_jan_21_end',
'troptions_feb_1_10',
'troptions_feb_11_20',
'troptions_feb_21_end',
'troptions_mar_1_10',
'troptions_mar_11_20',
'troptions_mar_21_end',
'troptions_apr_1_10',
'troptions_apr_11_20',
'troptions_apr_21_end',
'troptions_may_1_10',
'troptions_may_11_20',
'troptions_may_21_end',
'troptions_jun_1_10',
'troptions_jun_11_20',
'troptions_jun_21_end',
'troptions_jul_1_10',
'troptions_jul_11_20',
'troptions_jul_21_end',
'troptions_aug_1_10',
'troptions_aug_11_20',
'troptions_aug_21_end',
'troptions_sep_1_10',
'troptions_sep_11_20',
'troptions_sep_21_end',
'troptions_oct_1_10',
'troptions_oct_11_20',
'troptions_oct_21_end',
'troptions_nov_1_10',
'troptions_nov_11_20',
'troptions_nov_21_end',
'troptions_dec_1_10',
'troptions_dec_11_20',
'troptions_dec_21_end'
]

fno_df = pd.DataFrame()
for table in TABLES[0:1]:
    
    fno_query = f"""
    SELECT tr_date, stock_name, tr_time, tr_open, tr_close
    FROM {table}
    WHERE stock_name =  '{stock_name}'
    AND tr_segment = 3
    AND month_expiry = '1'
    ORDER BY tr_date, tr_time 
    """

    fno_df = fno_df.append(query_db(connect('fnodata2019'), fno_query))

filter_1 = (fno_df['tr_time'] == '09:15:59') # Entry Signal ---> Open price @ 15:30:59
fno_df_1 = fno_df.loc[filter_1]
print(fno_df_1)

filter_2 = (fno_df['tr_time']== '15:30:59') # Exit Signal ---> Close price @ 09:15:59
fno_df_2 = fno_df.loc[filter_2]
print(fno_df_2)

filter_3 = (fno_df['tr_time'] == '09:31:59')
fno_df_3 = fno_df.loc[filter_3]

filter_4 = (fno_df['tr_time'] == '15:15:59')
fno_df_4 = fno_df.loc[filter_4]

final_df_1 = pd.merge(fno_df_1, fno_df_2, on = ['tr_date', 'stock_name'])

print(final_df_1)

final_df_1.rename(columns = {'tr_date': 'Trade_date',\
                                'stock_name': 'Stock_name',\
                                    'tr_time_x':'Entry_signal',\
                                        'tr_time_y':'Exit_signal',\
                                            'tr_open_x': 'Open_price',\
                                                'tr_close_y': 'Close_price'}, inplace = True)

final_df_1.drop(['tr_close_x', \
                    'tr_open_y'], axis = 1, inplace = True )

final_df_1['Previous_Day_Close'] = final_df_1['Close_price'].shift(periods = 1)

Conditions = [final_df_1['Open_price'] > final_df_1['Previous_Day_Close'],
              final_df_1['Open_price'] < final_df_1['Previous_Day_Close'],
              final_df_1['Open_price'] == final_df_1['Previous_Day_Close']]

Choices = ['Sell', 'Buy', 'No Trade']

final_df_1['Signal'] = np.select(Conditions, Choices)


final_df_2 = pd.merge(fno_df_3, fno_df_4, on = ['tr_date', 'stock_name'])

final_df_2.rename(columns = {'tr_date': 'Trade_date',\
                                'stock_name': 'Stock_name',\
                                    'tr_time_x':'Entry_time',\
                                        'tr_time_y':'Exit_time',\
                                            'tr_open_x': 'Entry_price',\
                                                'tr_close_y': 'Exit_price'}, inplace = True)

final_df_2.drop(['tr_close_x', \
                    'tr_open_y'], axis = 1, inplace = True )

print(final_df_2)

final_fno_df = pd.merge(final_df_1, final_df_2, on = ['Trade_date','Stock_name'])

print(final_fno_df)

final_fno_df['PNL'] = final_fno_df['Entry_price'] - final_fno_df['Exit_price']

final_fno_df.to_excel('Part_5_df_report.xlsx')
