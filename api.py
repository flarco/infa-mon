
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
  Infa_Rep,
  log,
  compare_repo_folder,
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
  log('Start.')
  
  repos = dict(
    DEV=Infa_Rep('DEV', engines.dev),
    QA=Infa_Rep('QA', engines.qa),
    PRD=Infa_Rep('PRD', engines.prd),
  )

  folders = [
    # 'SOR_BLUEBOX',
    # 'FUSION',
    # 'CDE',
    # 'BIDW_RMS',
    # 'BIDW_PROCUREMENT',
    'CRM_ANALYTICS',
    # 'ARIBA',
  ]

  for i, folder_name in enumerate(folders):
    # folder = Repo.folders[folder_name]
    # folder.get_objects()
    # folder.get_list_fields()
  
    # folder_name = 'CRM_ANALYTICS'


    # Get folders
    tasks = [repo.get_list_folders() for repo in repos.values()]
    for t in tasks: t.join()
    
    # Get Objects
    tasks = [repo.get_folder_objects(folder_name, get_fields=True) for repo in repos.values()]
    for t in tasks: t.join()

  compare_repo_folder(repos, folders)

  log('End.')
