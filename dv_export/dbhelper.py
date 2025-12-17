# coding=utf-8

import os
import pyodbc
import pandas as pd
import socket
from sqlalchemy import create_engine,types
import io
from urllib.parse import quote_plus
"""sqlserverport
A module to query the SQL Browser service for the port number of a SQL Server instance.
"""
def sqlserver_port_lookup(server, instance):
    """Query the SQL Browser service and extract the port number
    :type server: str
    :type instance: str
    """
    udp_port = 1434
    # message type per SQL Server Resolution Protocol
    udp_message_type = b'\x04'  # CLNT_UCAST_INST (client, unicast, instance)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(20)

    udp_message = udp_message_type + instance.encode()
    sock.sendto(udp_message, (server, udp_port))
    response = sock.recv(1024)  # max 1024 bytes for CLNT_UCAST_INST

    response_list = response[3:].decode().split(';')
    response_dict = {response_list[i]: response_list[i+1] for i in range(0, len(response_list), 2)}

    return int(response_dict['tcp'])


conn_server = None
sqlserver_hostaddr = "192.168.1.149"
sqlserver_instance = "MSSQL"
sql_uid = "sa"
sql_pwd = "PasswordGS15"


#server_port = sqlserver_port_lookup(sqlserver_hostaddr, sqlserver_instance)
conn_server = '{0}'.format(sqlserver_hostaddr)


# Trusted_Connection conn str: 'DRIVER={SQL Server};Server=.\MSSQL;Database=InvestSuite;Trusted_Connection=True;'
conn = {
    "InvestSuite":"DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=InvestSuite;UID=%s;PWD=%s;"%(conn_server, sql_uid, sql_pwd),
    "FixedIncomeSuite":"DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=FixedIncomeSuite;UID=%s;PWD=%s;"%(conn_server, sql_uid, sql_pwd),
    'TaskProcess':"DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=TaskProcess;UID=%s;PWD=%s;"%(conn_server, sql_uid, sql_pwd),
    'PaymentManagement':"DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=PaymentManagement;UID=%s;PWD=%s;"%(conn_server, sql_uid, sql_pwd),
    'PortfolioManagement':"DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=PortfolioManagement;UID=%s;PWD=%s;"%(conn_server, sql_uid, sql_pwd)
}



def open_dbconn(conn_name):
    return pyodbc.connect(conn[conn_name])
def bulkInsert(df,table_name):
    dtyp = {c:types.VARCHAR(20) for c in df.columns[df.dtypes == 'object'].tolist()}
    con = conn['PaymentManagement']
    quoted = quote_plus(con)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con,fast_executemany=True)
    try:
        df.to_sql(table_name,schema="PaymentManagement", con=engine, if_exists='append', index=False, index_label=None, dtype=dtyp,chunksize=20000)
    except Exception as ex:
        raise ex 
    finally:
        engine.dispose()
def bulkInsertWithschema(df,table_name,schema_Name,DBName):
    dtyp = {c:types.VARCHAR(20) for c in df.columns[df.dtypes == 'object'].tolist()}
    con = conn[DBName]
    quoted = quote_plus(con)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con,fast_executemany=True)
    try:
        df.to_sql(table_name,schema=schema_Name, con=engine, if_exists='append', index=False, index_label=None, dtype=dtyp,chunksize=20000)
    except Exception as ex:
        raise ex 
    finally:
        engine.dispose()    
def exec_fetch_many(sql, **kwargs):
    dbcn = kwargs.get("dbcn")
    conn_name = kwargs.get("conn_name")

    if dbcn is None:
        dbcn = pyodbc.connect(conn[conn_name])

    try:
        cursor = dbcn.cursor()
        cursor.execute(sql)
        cols = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        cursor.commit()

        return cols, rows
    except Exception as ex:
        raise ex
    finally:
        if conn_name is not None:
            
            dbcn.close()
def exec_fetch_dataset(sql, **kwargs):
    dbcn = kwargs.get("dbcn")
    conn_name = kwargs.get("conn_name")
    dataset = []
    colsset = []
    if dbcn is None:
        dbcn = pyodbc.connect(conn[conn_name])

    try:
        cursor = dbcn.cursor()
        cursor.execute(sql)
        cols = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        colsset.append(cols)
        dataset.append(rows)
        while cursor.nextset():
            cols = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            
            if len(rows) > 0 :
                 dataset.append(rows)
                 colsset.append(cols)
        cursor.commit()

        return colsset, dataset
    except Exception as ex:
        raise ex
    finally:
        if conn_name is not None:
            dbcn.close()
def exec_fetch_one(sql, **kwargs):
    dbcn = kwargs.get("dbcn")
    conn_name = kwargs.get("conn_name")

    if dbcn is None:
        dbcn = pyodbc.connect(conn[conn_name])

    try:
        cursor = dbcn.cursor()
        cursor.execute(sql)
        # col = cursor.description[0][0]
        row = cursor.execute(sql).fetchone()
        if row is not None:
            return row[0]
        else:
            return None
    except Exception as ex:
        raise ex
    finally:
        if conn_name is not None:
            dbcn.close()

def dbrc_to_pddataframe(rows, cols):
    row_lists = []
    for row in rows:
        row_list = [c for c in row]
        row_lists.append(row_list)

    return pd.DataFrame(row_lists, columns=cols)

def closecn(conn_name):
    #conn = create_engine('mysql+pymysql:user:passwd@host:port/db?charset=etf-8')
    con = conn['PaymentManagement']
        
    quoted = quote_plus(con)
       
    new_con =  'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
        
    engine = create_engine(new_con,fast_executemany=True)
        #print(engine)
    con_a = engine.raw_connection()
    try:
        print('aaaaaa')
    except Exception as ee:
        #logger.error('fileToMysql fialed',ee)
        raise ee
    finally:
        engine.raw_connection().close()
def exec_commit_with_result(sql, **kwargs):
    dbcn = kwargs.get("dbcn")
    conn_name = kwargs.get("conn_name")
    parameters = kwargs.get("parameters")
    if dbcn is None:
        dbcn = pyodbc.connect(conn[conn_name])

    try:
        cursor = dbcn.cursor()
        # cursor.execute(sql)
        if parameters is not None:
            row = cursor.execute(sql,parameters).fetchone()
        else:
            row = cursor.execute(sql).fetchone()
        cursor.commit()
        return row[0]
    except Exception as ex:
        raise ex
    finally:
        if conn_name is not None:
            dbcn.rollback()
            dbcn.close()

def exec_commit(sql, **kwargs):
    dbcn = kwargs.get("dbcn")
    conn_name = kwargs.get("conn_name")
    if dbcn is None:
        dbcn = pyodbc.connect(conn[conn_name])
    try:
        cursor = dbcn.cursor()
        cursor.execute(sql)
        dbcn.commit()
    except Exception as ex:
        raise ex
    finally:
        if conn_name is not None:
            dbcn.rollback()
            dbcn.close()


if __name__ == "__main__":
    pass