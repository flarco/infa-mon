
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

creds = parse_yaml(dir_path + '/creds.yml')
cred = dict2(creds['INFA_DEV'])

if cred.type == 'oracle':
  dnsStr = makedsn(cred.host, cred.port, service_name=cred.instance)
  # dnsStr = cx_Oracle.makedsn(cred.host,cred.port,sid=cred.instance)
  # conn_str = 'oracle+cx_oracle://{user}:{password}@{host}:{port}/{instance}'
  conn_str = 'oracle+cx_oracle://{user}:{password}@' + dnsStr
elif cred.type == 'mssql':
  conn_str = 'mssql+pymssql://{user}:{password}@{host}:{port}/{instance}'

engine = sqlalchemy.create_engine(conn_str.format(**cred))
# conn = engine.connect()

result = engine.execute("select count(1) from INF_RP.OPB_MAPPING")
for r in result:
  print(str(r))


class Infa_Rep:
  """
  A general class abstracting the objects in the Informatica
  repository database.
  """


  def get_list_mappings(self, folder):
    """
    Obtain the list of mappings, ids in a folder.
    """
    

  def get_list_workflows(self, folder):
    """
    Obtain the list of workflows, ids in a folder.
    """
  

