from datetime import datetime
import db_config
import csv
from sqlalchemy.engine import create_engine
from sqlalchemy import types


def conn(i):
    """connecting with i"""
    dialect = 'oracle'
    sql_driver = 'cx_oracle'

    user = db_config.user[i]
    pw = db_config.pw[i]
    host = db_config.host
    port = db_config.port
    serv = db_config.service
    ENGINE_PATH_WIN_AUTH = dialect + '+' + sql_driver + '://' + user + ':' + \
                           pw +'@' + host + ':' + str(port) + '/?service_name=' + serv
    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    engine = engine.execution_options(autocommit=True)
    con = engine.connect()
    return con


def run_query(con, m, k):
    """Executes m, k is the file name which query result will be saved to, with connection con"""
    begin = datetime.now()
    print(f"Beginning of query: {begin}")
    result = con.execute(m)
    print('Cursor execution completed.')
    columns = result.keys()
    columns = list(map(str.upper, columns))
    file_name = k + '.csv'
    rowcount = 0

    with open(file_name, 'w', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        while True:
            chunk = result.fetchmany(100000)
            if not chunk:
                break
            writer.writerows(chunk)
            rowcount += len(chunk)
            print("Rows processed: " + str(rowcount))

    end = datetime.now()
    print(f"Ending of query: {end}", )
    print(f"Duration of query: {end - begin}")
    print("---------------------------")
    con.close()
    return file_name


def write_table(df, con, table):
    df.to_sql(name=table, con=con, schema='REPORT_DB', if_exists='replace', dtype={'INVOICENO': types.VARCHAR(20)})
    print("Table written and commited.")


