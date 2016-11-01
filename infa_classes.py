import logging, datetime, os, time

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
  split_list,
  run_async,
  export_data_to_csv,
)

from sql import(
  sql_oracle
)

from collections import (
  OrderedDict,
  namedtuple
)

DIR = os.path.dirname(os.path.realpath(__file__))
get_rec = lambda r,f: d2({k:r[v.lower()] for k,v in f.items()}) if len(f) > 0 else d2(r)

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
    self.repo = ''
    for key,val in kwargs.items():
      strip_prefix = lambda k: k.replace(self.__class__.__name__.lower() + "_", '')
      setattr(self, strip_prefix(key), val)
    
    self.sources = d2()
    self.sources_fields = d2()
    self.sources_id = d2()
    self.targets = d2()
    self.targets_fields = d2()
    self.targets_id = {}
    self.mappings = d2()
    self.mappings_id = d2()
    self.mappings_transf_fields = d2()
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
  
  def get_objects(self):
    self.get_list_sources()
    self.get_list_targets()
    self.get_list_mappings()
    self.get_list_sessions()
    self.get_list_workflows()

  def get_list_sources(self):
    """
    Obtain the list of sources, ids in a folder.
    """
    log(self.repo + " > Getting sources for {0}.".format(self.name))
    fields, sql = sql_oracle.list_source
    result = self.db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    for rec in data:
      self.sources_id[rec.source_id] = self.sources[rec.source_name] = rec

  def get_list_fields(self):
    """
    Obtain the list of fields for each transformation/source/target in all mappings in folder.
    """
    log(self.repo + " > Getting transformation fields for {0}.".format(self.name))
    fields, sql = sql_oracle.list_tranformation_fields
    result = self.db.execute(sql, d(folder_id=self.id))
    data_trans = [get_rec(row, fields) for row in result]

    log(self.repo + " > Getting source/target fields for {0}.".format(self.name))
    fields, sql = sql_oracle.list_source_target_fields
    result = self.db.execute(sql, d(folder_id=self.id))
    data_src_tgt = [get_rec(row, fields) for row in result]

    if len(self.mappings_id) == 0: self.get_list_mappings()
    if len(self.sources_id) == 0: self.get_list_sources()
    if len(self.targets_id) == 0: self.get_list_targets()

    for rec in data_trans:
      rec['TYPE_'] = ' | '.join([
        rec['datatype'],
        'S:' + str(rec['scale']) if rec['scale'] else 'S:' + 'null',
        'P:' + str(rec['precision']) if rec['precision'] else 'P:' + 'null',
        'E:' + str(rec['expression']) if rec['expression'] else 'E:' + 'null',
      ])

      self.mappings_transf_fields[rec.widget_field_id] = rec

    for rec in data_src_tgt:
      rec['TYPE_'] = ' | '.join([
        rec['field_datatype'].replace('nvarchar2','varchar2').replace('nchar','char'),
        'S:' + str(rec['field_scale']) if rec['field_scale'] else 'S:' + 'null',
        'P:' + str(rec['field_precision']) if rec['field_precision'] else 'P:' + 'null',
        'K:' + str(rec['field_key_type']) if rec['field_key_type'] else 'K:' + 'null',
        'N:' + str(rec['field_nulltype']) if rec['field_nulltype'] else 'N:' + 'null',
      ])

      if rec.type == 'SOURCE':
        self.sources_fields[rec.field_id] = rec
      else:
        self.targets_fields[rec.field_id] = rec


  def get_list_targets(self):
    """
    Obtain the list of targets, ids in a folder.
    """
    log(self.repo + " > Getting targets for {0}.".format(self.name))
    fields, sql = sql_oracle.list_target
    result = self.db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]
    
    for rec in data:
      self.targets_id[rec.target_id] = self.targets[rec.target_name] = rec
    
    # log(str(list(self.targets_id.keys())))

  def get_list_mappings(self):
    """
    Obtain the list of mappings, ids in a folder.
    """
    log(self.repo + " > Getting mappings for {0}.".format(self.name))
    fields, sql = sql_oracle.list_mapping
    result = self.db.execute(sql, d(folder_id=self.id))
    data = [get_rec(row, fields) for row in result]

    for rec in data:
      self.mappings_id[rec.mapping_id] = self.mappings[rec.mapping_name] = rec


  def get_list_workflows(self):
    """
    Obtain the list of workflows, ids in a folder.
    """
    log(self.repo + " > Getting workflows for {0}.".format(self.name))
    fields, sql = sql_oracle.list_workflow
    result = self.db.execute(sql, d(folder_id=self.id))
    
    for row in result:
      rec = get_rec(row, fields)
      self.workflows_id[rec.workflow_id] = self.workflows[rec.workflow_name] = Workflow(**rec)
      
    
    fields, sql = sql_oracle.list_workflow_sessions
    result = self.db.execute(sql, d(folder_id=self.id))

    for row in result:
      session = get_rec(row, fields)
      if session.workflow_id in self.workflows_id:
        self.workflows_id[session.workflow_id].session_ids.append(session.session_id)
    

  def get_list_sessions(self):
    """
    Obtain the list of sessions, ids in a folder.
    """
    log(self.repo + " > Getting sessions for {0}.".format(self.name))
    fields, sql = sql_oracle.list_session
    result = self.db.execute(sql, d(folder_id=self.id))
    
    for row in result:
      rec = get_rec(row, fields)
      self.sessions_id[rec.session_id] = self.sessions[rec.session_name] = Session(**rec)
    
    # Obtain connections
    for ids in split_list(self.sessions_id.keys(), 499):
      fields, sql = sql_oracle.list_session_conns
      # result = self.db.execute(sql, session_id=str(tuple(self.sessions_id.keys())))
      result = self.db.execute(sql.format(session_id=str(tuple(ids))))
      
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
          MAPPING_NAME= self.mappings_id[session.mapping_id].name,
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

  stats_details_key_order = 'folder workflow mapping session start end duration error src_success_rows src_failed_rows targ_success_rows targ_failed_rows total_trans_errs workflow_run_id folder_id workflow_id mapping_id session_id'.split()
    
  def __init__(self, name, engine=None):
    self.db = engine.connect()
    self.name = name
    self.objects = eUI_FolderTreeInfaObjects()
    self.folders = {}
    self.connections = d2()
    self.connections_id = d2()
    self.last_wf_run_id = None
    self.run_stats_data = OrderedDict()
    self.keep_refreshing = False
    self.folder_details = dict()
  
  @run_async
  def get_folder_objects(self, folder_name, get_fields=False):
    log(self.name + " > Getting list of objects.")
    self.folders[folder_name].get_objects()
    if get_fields: self.folders[folder_name].get_list_fields()

  def get_connections(self):
    """
    Obatin the list of connections in a repository.
    """
    log(self.name + " > Getting list of connections.")

    data_dict = sql_oracle.list_connections
    fields = {f.lower():f for f in data_dict.fields}
    sql = text('''select * from {table}'''.format(table=data_dict['table']))
    result = self.db.execute(sql)

    Record = namedtuple('Connection', data_dict.fields)
    for row in result:
      rec = get_rec(row, fields)
      self.connections[rec.object_id] = self.connections[rec.object_name] = Record(**rec)

  @run_async
  def get_list_folders(self):
    """
    Obatin the list of folders in a repository.
    """
    log(self.name + " > Getting list of folders.")
    fields, sql = sql_oracle.list_folder
    result = self.db.execute(sql)

    for row in result:
      rec = get_rec(row, fields)
      self.folders[rec.folder_name] = Folder(repo=self.name, db=self.db, **rec)
  
  @run_async
  def get_latest_run_stats(self):
    self.last_run_stats_refresh = datetime.datetime.now()
    if not self.last_wf_run_id:
      log(self.name + " > Getting latest run stats [full].")
      fields, sql = sql_oracle.log_session_run_full
      result = self.db.execute(
        sql,
        d(limit=999)
      )
    else:
      log(self.name + " > Getting latest run stats [recent >= {s}].".format(
          s=self.last_wf_run_id
        )
      )
      fields, sql = sql_oracle.log_session_run_recent
      result = self.db.execute(
        sql,
        d(last_wf_run_id=self.last_wf_run_id)
      )
    
    running_wf_run_id = None
    folder_ids = set()

    for row in result:
      rec = get_rec(row, fields)
      rec['start'] = rec['start'].strftime('%Y-%m-%d %H:%M:%S')
      rec['success'] = 'No' if rec['error'] else 'Yes'
      row_stats_str = ' S:{}/{} | T:{}/{} | E:{}'.format(
        rec['src_success_rows'],
        rec['src_failed_rows'],
        rec['targ_success_rows'],
        rec['targ_failed_rows'],
        rec['total_trans_errs'],
      )
      rec['error'] = rec['error'] + ' --> ' + row_stats_str if rec['error'] else row_stats_str

      try:
        rec['end'] = rec['end'].strftime('%Y-%m-%d %H:%M:%S')
        self.last_wf_run_id = rec['workflow_run_id'] if not self.last_wf_run_id or rec['workflow_run_id'] > self.last_wf_run_id else self.last_wf_run_id
      except ValueError:  # still in progress
        rec['end'] = ''
        rec['success'] = 'Running'
        running_wf_run_id = rec['workflow_run_id'] if not running_wf_run_id or rec['workflow_run_id'] < running_wf_run_id else running_wf_run_id
      rec['duration'] = float(rec['duration'])
      rec['combo'] = str(rec.workflow_run_id) + '-' + str(rec.session_id)
      
      self.run_stats_data[rec['combo']] = rec
      if not rec['folder_id'] in self.folder_details:
        folder_ids.add(rec['folder_id'])
        self.folder_details[rec['folder_id']] = 'updating'
    
    if len(folder_ids) > 0:
      self.get_folder_details(list(folder_ids))
    
    if running_wf_run_id:
      self.last_wf_run_id = running_wf_run_id
  
  @run_async
  def get_folder_details(self, folder_ids):
    log(self.name + " > get_folder_details({}).".format(str(folder_ids)))
    fields, sql = sql_oracle.list_workflow_details
    sql = sql.format(folder_ids=','.join([str(f) for f in folder_ids]))
    result = self.db.execute(sql)
    fields = {k.lower():k for k in result.keys()}
    data = [get_rec(row, fields) for row in result]

    for folder_id in folder_ids:
      self.folder_details[folder_id] = []
    
    for rec in data:
      self.folder_details[rec['folder_id']].append(rec)
    
    for folder_id in folder_ids:
      log(self.name + " > done with get_folder_details({}) -> {} rows.".format(folder_id, len(self.folder_details[folder_id])))

  def get_stats_details(self, combo):
    sess_run_inst = self.run_stats_data[combo]
    folder_id = sess_run_inst['folder_id']
    folder_data = self.folder_details[folder_id]

    w_counter = 0
    while folder_data == 'updating' and w_counter < 20:
      time.sleep(1)
      w_counter += 1
      folder_data = self.folder_details[folder_id]
    
    if folder_data == 'updating': return sess_run_inst

    e_i = 0
    for rec in folder_data:
      if rec['session_id'] == sess_run_inst['session_id']:
        e_i += 1
        for k in ['source_table', 'target_table',
          'source_conn', 'target_conn']:
          sess_run_inst[(e_i,k)] = rec[k]
    
    self.run_stats_data[combo] = sess_run_inst

    return sess_run_inst

class eUI_FolderTreeInfaObjects:
  "A class to abstract objects for the Tree components of easyuiJS."

  def __init__(self):
    self.id_ = 0  # the id counter
    self.root = []
    self.folders = OrderedDict()

  def root_search(self, q_text):
    "Search the object text"
    result = []
    for f in sorted(self.folders):
      folder = folders[f]
      for categ in folder['children']:
        for obj in categ['children']:
          if q_text.lower() in str(obj['text']).lower():
            result.append(folder)
    
    return result

  def add_folder(self,folder, q_text = None):
    "Generate a dict object to add to a easyuijs Tree"

    def create_child(name):
      "Create child element"
      self.id_ += 1
      child2 = dict(
        id=self.id_,
        text=name,
        attributes={
            "url":"/test",
            "price":100,
            "klass": "child"
        }
        # iconCls=None
      )

      if q_text:
        if q_text.lower() in name.lower():
          return child2
        else:
          return None
      else:
        return child2
    

    def gen_category(cat_name, list_):
      child_list = []
      for n in sorted(list_):
        obj = create_child(n)
        if obj:
          child_list.append(obj)
      
      category = dict(
        text=cat_name, state='open' if len(child_list) > 0 and q_text else 'closed',
        children = child_list,
      )
      return category
    

    self.id_ += 1
    child = dict(
      id=self.id_,
      text=folder.name,
      # iconCls=None,
      checked=False,
      state='closed',
      children = [
        gen_category('Sources',folder.sources),
        gen_category('Targets',folder.targets),
        gen_category('Mappings',folder.mappings),
        gen_category('Sessions',folder.sessions),
        gen_category('Workflows',folder.workflows),
      ]
    )

    # self.root.append(child)
    self.folders[folder.name] = child
    self.root = [self.folders[f] for f in sorted(self.folders)]
  
def compare_repo_folder(repos, folder_names):
  log(" > Creating Compare Report.")
  headers = "FOLDER OBJECT_TYPE OBJECT_NAME DEV QA PRD DELTA DEV_TYPE QA_TYPE PRD_TYPE".split()
  report_data_lines = OrderedDict()

  for env, repo in repos.items():
    for folder_name in folder_names:
      folder = repo.folders[folder_name]
      
      for step_val in [
        ('WORKFLOW', folder.workflows),
        ('SESSION', folder.sessions),
        ('MAPPING', folder.mappings),
        ('SOURCE', folder.sources),
        ('TARGET', folder.targets),
      ]:
        obj_type, obj_list = step_val
        for obj_name in sorted(obj_list):
          combo = (folder_name, obj_type, obj_name)
          report_data_lines[combo] = report_data_lines.get(
            combo,
            dict(
              DEV='No',
              QA='No',
              PRD='No',
              DELTA='No',
              DEV_TYPE='',
              QA_TYPE='',
              PRD_TYPE='',
            )
          )
          id_field_name = obj_type.lower() + '_id'
          try:report_data_lines[combo][env] = str(obj_list[obj_name][id_field_name])
          except: report_data_lines[combo][env] = str(obj_list[obj_name].id)
      
      for step_val in [
        ('SOURCE_FIELD', folder.sources_fields),
        ('TARGET_FIELD', folder.targets_fields),
        ('TRANSF_FIELD', folder.mappings_transf_fields),
      ]:
        obj_type, obj_list = step_val
        for obj_id in sorted(obj_list):
          obj_name = obj_list[obj_id]['combo2']
          combo = (folder_name, obj_type, obj_name)
          report_data_lines[combo] = report_data_lines.get(
            combo,
            dict(
              DEV='No',
              QA='No',
              PRD='No',
              DELTA='No',
              DEV_TYPE='missing',
              QA_TYPE='missing',
              PRD_TYPE='missing',
            )
          )
          report_data_lines[combo][env] = str(obj_id)
          report_data_lines[combo][env+'_TYPE'] = obj_list[obj_id]['TYPE_']
  
  
  list_mapping = {
    'SOURCE_FIELD': folder.sources_fields,
    'TARGET_FIELD': folder.targets_fields,
    'TRANSF_FIELD': folder.mappings_transf_fields
  }

  data_rec = []
  for combo,val in report_data_lines.items():
    FOLDER, OBJECT_TYPE, OBJECT_NAME = combo
    
    if OBJECT_TYPE in ('SOURCE_FIELD','TARGET_FIELD','TRANSF_FIELD'):
      if val['DEV_TYPE'] != val['QA_TYPE'] or \
      val['QA_TYPE'] != val['PRD_TYPE']  or \
      val['DEV_TYPE'] != val['PRD_TYPE']:
        val['DELTA'] = 'Yes'
    
    if (val['DEV'] != 'No' and val['QA'] == 'No') or \
    (val['DEV'] != 'No' and val['PRD'] == 'No') or \
    (val['QA'] != 'No' and val['DEV'] == 'No') or \
    (val['QA'] != 'No' and val['PRD'] == 'No') or \
    (val['PRD'] != 'No' and val['DEV'] == 'No') or \
    (val['PRD'] != 'No' and val['QA'] == 'No'):
      val['DELTA'] = 'Yes'

    rec = dict(
      FOLDER=FOLDER,
      OBJECT_TYPE=OBJECT_TYPE,
      OBJECT_NAME=OBJECT_NAME,
      DEV='Yes' if val['DEV'] != 'No' else 'No',
      QA='Yes' if val['QA'] != 'No' else 'No',
      PRD='Yes' if val['PRD'] != 'No' else 'No',
      DELTA=str(val['DELTA']),
      DEV_ID=val['DEV'],
      QA_ID=val['QA'],
      PRD_ID=val['PRD'],
      DEV_TYPE=val['DEV_TYPE'],
      QA_TYPE=val['QA_TYPE'],
      PRD_TYPE=val['PRD_TYPE'],
    )
    data_rec.append(rec)

  suff = datetime.datetime.now().strftime('%Y%m%d_%H%M')
  output_path = DIR + '/infa_compare_objects_{suff}.csv'.format(suff=suff)
  export_data_to_csv(output_path, headers, data_rec)

  # Get workflows

  # Get sessions

  # Get Mappings

  # Compare list of Workflows
  # Compare list of Sessions
  # Compare list of Mappings
  # For each mapping compare the field types