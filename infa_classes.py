from helpers import (
  d,
  d2,
  dir_path
)

from sql import(
  sql_oracle
)

from collections import (
  OrderedDict,
  namedtuple
)

get_rec = lambda r,f: d2({k:r[v.lower()] for k,v in f.items()})

class Session:
  """
  A class abstracting a Session in the Informatica.
  """
  def __init__(self, *args, **kwargs):
    for key,val in kwargs.items():
      strip_prefix = lambda k: k.replace(self.__class__.__name__.lower() + "_", '')
      setattr(self, strip_prefix(key), val)

class Workflow:
  """
  A class abstracting a Workflow in the Informatica.
  """
  def __init__(self, *args, **kwargs):
    self.session_ids = []
    for key,val in kwargs.items():
      strip_prefix = lambda k: k.replace(self.__class__.__name__.lower() + "_", '')
      setattr(self, strip_prefix(key), val)

class Folder:
  """
  A class abstracting a Folder in the Informatica.
  """
  def __init__(self, *args, **kwargs):
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
    fields, sql = sql_oracle.list_source
    result = db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    self.sources = d2({rec.source_name: rec for rec in data})
    self.sources_id = d2({rec.source_id: rec.source_name for rec in data})

  def get_list_targets(self):
    """
    Obtain the list of targets, ids in a folder.
    """
    fields, sql = sql_oracle.list_target
    result = db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    self.targets = d2({rec.target_name: rec for rec in data})
    self.targets_id = d2({rec.target_id: rec.target_name for rec in data})

  def get_list_mappings(self):
    """
    Obtain the list of mappings, ids in a folder.
    """
    fields, sql = sql_oracle.list_mapping
    result = db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    self.mappings = d2({rec.mapping_name: rec for rec in data})
    self.mappings_id = d2({rec.mapping_id: rec.mapping_name for rec in data})

  def get_list_workflows(self):
    """
    Obtain the list of workflows, ids in a folder.
    """
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
    fields, sql = sql_oracle.list_session
    result = db.execute(sql, d(folder_id=self.id))
    
    for row in result:
      rec = get_rec(row, fields)
      self.sessions_id[rec.session_id] = self.sessions[rec.session_name] = Session(**rec)
  
  def generate_workflow_report_1(self):
    """
    Generate a workflow report.
    """
    headers = "FOLDER WORKFLOW_NAME SESSION_NAME MAPPING_NAME SOURCE_CONNECTION SOURCE TARGET_CONNECTION TARGET ".split()
    WFRecord = namedtuple('WFRecord', headers)

    
    for wf_name, workflow in self.workflows.items():
      for session_id in workflow.session_ids:
        session = self.sessions_id[session_id]
        record = d(
          FOLDER=self.name,
          WORKFLOW_NAME=workflow.name,
          SESSION_NAME= session.name,
          MAPPING_NAME= self.mappings_id[session.mapping_id],
          SOURCE_CONNECTION='',
          SOURCE='',
          TARGET_CONNECTION='',
          TARGET='',
        )

        # out_file.write(','.join(WFRecord(**record)) + '\n')
        print(','.join(WFRecord(**record)))

class Infa_Rep:
  """
  A general class abstracting the objects in the Informatica
  repository database.
  """

  def __init__(self, engine=None):
    global db
    db = engine.connect()

    self.folders = d2()
  
  def get_list_folders(self):
    """
    Obatin the list of folders in a repository.
    """
    fields, sql = sql_oracle.list_folder
    result = db.execute(sql)

    for row in result:
      rec = get_rec(row, fields)
      self.folders[rec.folder_name] = Folder(**rec)
    
    
  
