import cx_Oracle
import pandas as pd
from datetime import datetime
import db_config  # configuration for database
import queries  # queries can be stored in here

"""
If you may want to save your Oracle PL/SQL query result as multiple sheets to an Excel file 
(for whatever reason) this code here can help you to divide the result sheet by sheet.
"""

writer = pd.ExcelWriter('query_output.xlsx', engine='xlsxwriter')


dsn_tns = cx_Oracle.makedsn(db_config.host, db_config.port, service_name=db_config.service)
print(dsn_tns)
con = cx_Oracle.connect(user=db_config.user, password=db_config.pw, dsn=dsn_tns, events=True)
print("Database version:", con.version)


def run_query(self):
    cursor = con.cursor()
    cursor.execute(self)
    result = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    return result, columns


begin = datetime.now()
print(f"Beginning of query: {begin}")

res, col = run_query(queries.query)

end = datetime.now()
print(f"Ending of query: {end}", )
print(f"Query time: {end-begin}")

df = pd.DataFrame(res, columns=col)

maxrows = 1048576  # max number of rows allowed for .xlsx files
rows = len(df)
count = 0

if rows < maxrows:
    df.to_excel(writer, sheet_name="page")
else:
    while rows >= maxrows:
        tosave = df.head(maxrows-1)
        df = df.iloc[maxrows-1:]
        rows = len(df)
        count += 1
        ad = str(count) + ". page"
        tosave.to_excel(writer, sheet_name=ad)
        if rows < maxrows:
            count += 1
            ad = str(count) + ". page"
            df.to_excel(writer, sheet_name=ad)

writer.save()
con.close()
completed_at = datetime.now()
print(f"Process completed at: {completed_at}", )
exit()
