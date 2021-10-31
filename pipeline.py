import pandas as pd
import sql_al as sa
import queries
import fn_mstr_invoice as fn

con = sa.conn(1)
df = sa.run_query(con, queries.query, 'test_dhl')
df = pd.read_csv(df, dtype='object')
df['OPERATION'] = 'DHL'
print("Query saved in CSV file!!!")
summary = fn.vdo_invoice_calc(df)
fn.mstr_conn(summary, 'update')
