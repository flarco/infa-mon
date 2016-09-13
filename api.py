
import sys, os, re, time, datetime

import sqlalchemy
from sqlalchemy import (
  create_engine
)

from helpers import (
  dict2,
  parse_yaml,
  dir_path
)

from cx_Oracle import (
  makedsn,
)

from sql import(
  sql_oracle
)

creds = parse_yaml(dir_path + '/creds.yml')
cred = dict2(creds['INFA_DEV2'])

if cred.type == 'oracle':
  dnsStr = makedsn(cred.host, cred.port, service_name=cred.instance)
  # dnsStr = cx_Oracle.makedsn(cred.host,cred.port,sid=cred.instance)
  # conn_str = 'oracle+cx_oracle://{user}:{password}@{host}:{port}/{instance}'
  conn_str = 'oracle+cx_oracle://{user}:{password}@' + dnsStr
elif cred.type == 'mssql':
  conn_str = 'mssql+pymssql://{user}:{password}@{host}:{port}/{instance}'

engine = sqlalchemy.create_engine(conn_str.format(**cred))
# conn = engine.connect()

fields, sql = sql_oracle.list_mapping


# result = engine.execute("select count(1) from INF_RP.OPB_MAPPING")
result = engine.execute(sql.format(folder_id=108))
data = [r for r in result]
print(str(len(data)))
