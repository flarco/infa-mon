import logging

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

from helpers import (
  d,
  d2,
  dir_path,
  split_list
)

from sql import(
  sql_oracle
)

from collections import (
  OrderedDict,
  namedtuple
)

get_rec = lambda r,f: d2({k:r[v.lower()] for k,v in f.items()})

def log(text):
  "Print / Log text or status"
  logger.info(text)

class Session:
  "A class abstracting a Session in the Informatica."

  def __init__(self, *args, **kwargs):
    self.sources = {}
    self.targets = {}

    for key,val in kwargs.items():
      strip_prefix = lambda k: k.replace(self.__class__.__name__.lower() + "_", '')
      setattr(self, strip_prefix(key), val)
  
  def add_connection(self, conn_type, conn_name):
    "Add a Reader / Writer Connection used in the Session"
    conn_name = conn_name.replace('Relational:', '') if conn_name else ''

    if 'reader' in conn_type.lower():
      self.sources[(conn_type.replace(' ', '_'), conn_name)] = None
    elif 'writer' in conn_type.lower():
      self.targets[(conn_type.replace(' ', '_'), conn_name)] = None

class Workflow:
  "A class abstracting a Workflow in the Informatica."
  def __init__(self, *args, **kwargs):
    self.session_ids = []
    for key,val in kwargs.items():
      strip_prefix = lambda k: k.replace(self.__class__.__name__.lower() + "_", '')
      setattr(self, strip_prefix(key), val)

class Folder:
  "A class abstracting a Folder in the Informatica."

  def __init__(self, *args, **kwargs):
    self.name = ''
    for key,val in kwargs.items():
      strip_prefix = lambda k: k.replace(self.__class__.__name__.lower() + "_", '')
      setattr(self, strip_prefix(key), val)
    
    self.sources = d2()
    self.sources_id = d2()
    self.targets = d2()
    self.targets_id = d2()
    self.mappings = d2()
    self.mappings_id = d2()
    self.sessions = d2()
    self.sessions_id = d2()
    self.workflows = d2()
    self.workflows_id = d2()

    self.childs = [
      'sources',
      'targets',
      'mappings',
      'sessions',
      'workflows',
    ]

  def get_list_sources(self):
    """
    Obtain the list of sources, ids in a folder.
    """
    log("Getting sources for {0}.".format(self.name))
    fields, sql = sql_oracle.list_source
    result = db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    self.sources = d2({rec.source_name: rec for rec in data})
    self.sources_id = d2({rec.source_id: rec.source_name for rec in data})

  def get_list_targets(self):
    """
    Obtain the list of targets, ids in a folder.
    """
    log("Getting targets for {0}.".format(self.name))
    fields, sql = sql_oracle.list_target
    result = db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    self.targets = d2({rec.target_name: rec for rec in data})
    self.targets_id = d2({rec.target_id: rec.target_name for rec in data})

  def get_list_mappings(self):
    """
    Obtain the list of mappings, ids in a folder.
    """
    log("Getting mappings for {0}.".format(self.name))
    fields, sql = sql_oracle.list_mapping
    result = db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    self.mappings = d2({rec.mapping_name: rec for rec in data})
    self.mappings_id = d2({rec.mapping_id: rec.mapping_name for rec in data})

  def get_list_workflows(self):
    """
    Obtain the list of workflows, ids in a folder.
    """
    log("Getting workflows for {0}.".format(self.name))
    fields, sql = sql_oracle.list_workflow
    result = db.execute(sql, d(folder_id=self.id))
    
    for row in result:
      rec = get_rec(row, fields)
      self.workflows_id[rec.workflow_id] = self.workflows[rec.workflow_name] = Workflow(**rec)
      
    
    fields, sql = sql_oracle.list_workflow_sessions
    result = db.execute(sql, d(folder_id=self.id))

    for row in result:
      session = get_rec(row, fields)
      if session.workflow_id in self.workflows_id:
        self.workflows_id[session.workflow_id].session_ids.append(session.session_id)
    

  def get_list_sessions(self):
    """
    Obtain the list of sessions, ids in a folder.
    """
    log("Getting sessions for {0}.".format(self.name))
    fields, sql = sql_oracle.list_session
    result = db.execute(sql, d(folder_id=self.id))
    
    for row in result:
      rec = get_rec(row, fields)
      self.sessions_id[rec.session_id] = self.sessions[rec.session_name] = Session(**rec)
    
    # Obtain connections
    for ids in split_list(self.sessions_id.keys(), 499):
      fields, sql = sql_oracle.list_session_conns
      # result = db.execute(sql, session_id=str(tuple(self.sessions_id.keys())))
      result = db.execute(sql.format(session_id=str(tuple(ids))))
      
      for row in result:
        rec = get_rec(row, fields)
        self.sessions_id[rec.session_id].add_connection(rec.connection_type, rec.connection_name)
  
  def analyze_workflow(self, workflow_name):
    "Analyze of workflow. Gives last 20 executions and statistics for each of them"


  def generate_workflow_report_1(self, output_path='/__/temp/wf_report.csv', append=False):
    "Generate a workflow report."
    headers = "FOLDER WORKFLOW_NAME SESSION_NAME MAPPING_NAME SOURCE_CONNECTION SOURCE TARGET_CONNECTION TARGET ".split()
    WFRecord = namedtuple('WFRecord', headers)

    if append:
      out_file = open(output_path, 'a')
    else:
      out_file = open(output_path, 'w')
      out_file.write(','.join(headers) + '\n')
    
    for wf_name, workflow in self.workflows.items():
      for session_id in workflow.session_ids:
        session = self.sessions_id[session_id]
        source_connections = '|'.join([
          conn_type+'.'+conn_name for conn_type, conn_name in session.sources
        ])
        target_connections = '|'.join([
          conn_type+'.'+conn_name for conn_type, conn_name in session.targets
        ])
          
        record = d(
          FOLDER=self.name,
          WORKFLOW_NAME=workflow.name,
          SESSION_NAME= session.name,
          MAPPING_NAME= self.mappings_id[session.mapping_id],
          SOURCE_CONNECTION=source_connections,
          SOURCE='',
          TARGET_CONNECTION=target_connections,
          TARGET='',
        )

        out_file.write(','.join(WFRecord(**record)) + '\n')
        # print(','.join(WFRecord(**record)))
    
    out_file.close()
  
  def get_EU_FolderTree(self, id_):
    "Generate a dict object to add to a easyuijs Tree"

    def create_child(name):
      "Create child element"
      id_ += 1
      return dict(
        id=self.id_,
        text=name,
        # iconCls=None
      )
    
    id_ += 1
    child = dict(
      id=id_,
      text=folder.name,
      # iconCls=None,
      checked=False,
      state='closed',
      children = [
        dict(
          text='Sources', state='closed',
          children = [create_child(n) for n in sorted(folder.sources)],
        ),
        dict(
          text='Targets',state='closed',
          children = [create_child(n) for n in sorted(folder.targets)],
        ),
        dict(
          text='Mappings',state='closed',
          children = [create_child(n) for n in sorted(folder.mappings)],
        ),
        dict(
          text='Sessions',state='closed',
          children = [create_child(n) for n in sorted(folder.sessions)],
        ),
        dict(
          text='Workflows',state='closed',
          children = [create_child(n) for n in sorted(folder.workflows)],
        ),
      ]
    )
    return child, id_

class Infa_Rep:
  "A general class abstracting the objects in the Informatica repository database."

  def __init__(self, engine=None):
    global db
    db = engine.connect()

    self.folders = d2()
  
  def get_list_folders(self):
    """
    Obatin the list of folders in a repository.
    """
    log("Getting list of folders.")
    fields, sql = sql_oracle.list_folder
    result = db.execute(sql)

    for row in result:
      rec = get_rec(row, fields)
      self.folders[rec.folder_name] = Folder(**rec)
    
class eUI_FolderTreeInfaObjects:
  "A class to abstract objects for the Tree components of easyuiJS."

  def __init__(self):
    self.id_ = 0  # the id counter
    self.root = []

  def add_folder(self,folder):
    "Generate a dict object to add to a easyuijs Tree"

    def create_child(name):
      "Create child element"
      self.id_ += 1
      return dict(
        id=self.id_,
        text=name,
        attributes={
            "url":"/test",
            "price":100,
            "klass": "child"
        }
        # iconCls=None
      )
    
    self.id_ += 1
    child = dict(
      id=self.id_,
      text=folder.name,
      # iconCls=None,
      checked=False,
      state='closed',
      children = [
        dict(
          text='Sources', state='closed',
          children = [create_child(n) for n in sorted(folder.sources)],
        ),
        dict(
          text='Targets',state='closed',
          children = [create_child(n) for n in sorted(folder.targets)],
        ),
        dict(
          text='Mappings',state='closed',
          children = [create_child(n) for n in sorted(folder.mappings)],
        ),
        dict(
          text='Sessions',state='closed',
          children = [create_child(n) for n in sorted(folder.sessions)],
        ),
        dict(
          text='Workflows',state='closed',
          children = [create_child(n) for n in sorted(folder.workflows)],
        ),
      ]
    )
    self.root.append(child)