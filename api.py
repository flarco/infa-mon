
import sys, os, re, time, datetime

import sqlalchemy

from helpers import (
  d,
  d2,
  parse_yaml,
  dir_path
)

from cx_Oracle import (
  makedsn,
)

from infa_classes import (
  Infa_Rep
)


creds = parse_yaml(dir_path + '/creds.yml')
cred = d2(creds['INFA_DEV2'])

if cred.type == 'oracle':
  dnsStr = makedsn(cred.host, cred.port, service_name=cred.instance)
  # dnsStr = cx_Oracle.makedsn(cred.host,cred.port,sid=cred.instance)
  # conn_str = 'oracle+cx_oracle://{user}:{password}@{host}:{port}/{instance}'
  conn_str = 'oracle+cx_oracle://{user}:{password}@' + dnsStr
elif cred.type == 'mssql':
  conn_str = 'mssql+pymssql://{user}:{password}@{host}:{port}/{instance}'

engine = sqlalchemy.create_engine(conn_str.format(**cred))

Repo = Infa_Rep(engine)
Repo.get_list_folders()

folder = Repo.folders['ARIBA']
folder.get_list_sources()
folder.get_list_targets()
folder.get_list_mappings()
folder.get_list_sessions()
folder.get_list_workflows()
folder.generate_workflow_report_1()

pass