
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
cred = d2(creds['INFA_DEV'])
cred = d2(creds['INFA_QA'])

def create_engine(cred):
  cred = d2(cred)
  if cred.type == 'oracle':
    dnsStr = makedsn(cred.host, cred.port, service_name=cred.instance)
    # dnsStr = cx_Oracle.makedsn(cred.host,cred.port,sid=cred.instance)
    # conn_str = 'oracle+cx_oracle://{user}:{password}@{host}:{port}/{instance}'
    conn_str = 'oracle+cx_oracle://{user}:{password}@' + dnsStr
  elif cred.type == 'mssql':
    conn_str = 'mssql+pymssql://{user}:{password}@{host}:{port}/{instance}'

  return sqlalchemy.create_engine(conn_str.format(**cred), pool_size=10)

engines = d2(
  dev=create_engine(creds['INFA_DEV']),
  qa=create_engine(creds['INFA_QA']),
  prd=create_engine(creds['INFA_PRD']),
)

if __name__ == '__main__':
  
  
  Repo = Infa_Rep(engines.dev)
  Repo.get_list_folders()

  # folder = Repo.folders['ARIBA']
  folders = [
    # 'SOR_BLUEBOX',
    'BIDW_RMS',
    'BIDW_PROCUREMENT',
    'ARIBA',
  ]

  for i, folder_name in enumerate(folders):
    folder = Repo.folders[folder_name]
    folder.get_list_sources()
    folder.get_list_targets()
    folder.get_list_mappings()
    folder.get_list_sessions()
    folder.get_list_workflows()
    # if i == 0:
    #   folder.generate_workflow_report_1()
    # else:
    #   folder.generate_workflow_report_1(append=True)
