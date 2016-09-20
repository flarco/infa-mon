from api import engine
from infa_classes import (
  Infa_Rep,
  eUI_FolderTreeInfaObjects
)

import datetime, time, threading, atexit


import gevent
from gevent.wsgi import WSGIServer
from gevent.queue import Queue

from flask import *
import json

from helpers import (
  make_celery,
  ServerSentEvent,
  all_threads,
  run_async,
  interrupt
)

application = Flask(__name__)
subscriptions = []

# atexit.register(interrupt)



def push_event(text):
  msg = str(text)
  for sub in subscriptions[:]:
    sub.put(msg)



@run_async
def refresh_run_stat():
  Repo.get_latest_run_stats()


@run_async
def keep_refreshing_run_stat():
  while True:
    time.sleep(3)
    if Repo.keep_refreshing:
      Repo.get_latest_run_stats()
      gevent.spawn(push_event('refreshMonData'))
    else:
      break

def stop_refreshes():
  Repo.keep_refreshing =False

# atexit.register(stop_refreshes)

@run_async
def refresh_folder(folder_name):
  gevent.spawn(push_event(str(datetime.datetime.now()) + ' - START - ' + folder_name))
  folder = Repo.folders[folder_name]
  folder.get_list_sources()
  folder.get_list_targets()
  folder.get_list_mappings()
  folder.get_list_sessions()
  folder.get_list_workflows()
  infa_objects.add_folder(folder)
  gevent.spawn(push_event(str(datetime.datetime.now()) + ' - END - ' + folder_name))
  gevent.spawn(push_event('refreshObjectTree'))




infa_objects = eUI_FolderTreeInfaObjects()
Repo = Infa_Rep(engine)
Repo.get_list_folders()
refresh_folder('BIDW_RMS')
refresh_run_stat()


@application.route('/objects', methods=['GET'])
def objects():
  return render_template('objects.html')


@application.route('/monitor', methods=['GET'])
def monitor():
  return render_template('monitor.html')

test_content = '''
<div class="easyui-layout" data-options="fit:true">
            <div data-options="region:'west',split:true" style="width:30%;max-width:400px;padding:10px">
                <ul id='object-tree2' class="easyui-tree" data-options="url:'object_tree.json'"></ul>
            </div>
            <div data-options="region:'center'" style="padding:10px">
                Center Content!!!!
            </div>
        </div>
'''

run_stats_detail = '''
<div class="easyui-layout" data-options="fit:true">
<p>{error_message}</p>
</div>
'''

@application.route('/<object>.html', methods=['GET','POST'])
def get_content(object):
  record = request.values.to_dict()
  content = globals[object]

  if object == 'run_stats_detail':
    combo = record['combo']
    content = content.format(error_message=Repo.run_stats_data[combo].error)
  
  return content

@application.route('/test', methods=['GET','POST'])
def test():
 return render_template('test.html')

@application.route('/switch', methods=['GET'])
def monitor_switch():
  record = request.values.to_dict()
  if record['status'] == 'true':
    Repo.keep_refreshing = True
    keep_refreshing_run_stat()
  else:
    Repo.keep_refreshing = False
  
  return 'OK! Switched ' + record['status']

@application.route('/<object>.json', methods=['GET','POST'])
def get_data(object):
  if object == 'test_data1':
    data = [
      {
        "id":1,
        "text":"Laks"
      },{
          "id":2,
          "text":"Nicole"
      },{
          "id":3,
          "text":"Marco",
          "selected":True
      },{
          "id":4,
          "text":"text4"
      },{
          "id":5,
          "text":"text5"
      }
    ]

  if object == 'object_tree':
    data = [
      dict(
        text='DEV',
        state='closed',
        children = infa_objects.root,
      ),
      dict(
        text='QA',
        state='closed',
        children = infa_objects.root,
      ),
      dict(
        text='PRD',
        state='closed',
        children = infa_objects.root,
      ),
    ]

  if object == 'object_tree_search':
    q_text = ''
    for folder_name in infa_objects.folders:
      infa_objects.add_folder(folder_name, q_text=q_text)
    
    data = [
      dict(
        text='DEV',
        state='closed',
        children = infa_objects.root,
      ),
      dict(
        text='QA',
        state='closed',
        children = infa_objects.root,
      ),
      dict(
        text='PRD',
        state='closed',
        children = infa_objects.root,
      ),
    ]

  if object == 'monitor_data_dev':
    data = [
      dict(
        folder='G',
        workflow='G',
        session='G',
        mapping='G',
        start=2,
        duration='G',
        success='Yes',
        error='',
      ),
      dict(
        folder='Fa',
        workflow='G',
        session='G',
        mapping='G',
        start=33,
        duration='G',
        success='No',
        error='ERORO!',
      ),
      dict(
        folder='Faa',
        workflow='G',
        session='G',
        mapping='G',
        start=1,
        duration='G',
        success='No',
        error='ERORO!',
      ),
    ]

    data = [Repo.run_stats_data[i] for i in sorted(Repo.run_stats_data, reverse=True)]
    
  return json.dumps(data)

@application.route("/refresh")
def refresh1():
  Repo.get_list_folders()
  folders = [
    'SOR_BLUEBOX',
    'BIDW_RMS',
    'BIDW_PROCUREMENT',
    'ARIBA',
  ]

  for i, folder_name in enumerate(sorted(Repo.folders)):
    if not folder_name in folders: continue
    refresh_folder(folder_name)
  
  return "OK"

@application.route("/publish")
def publish():
  #Dummy data - pick up from request for real data
  def notify():
    msg = str(time.time())
    for sub in subscriptions[:]:
      sub.put(msg)
  
  gevent.spawn(notify)
  
  return "OK"

@application.route("/subscribe")
def subscribe():
  def gen():
    q = Queue()
    subscriptions.append(q)
    try:
      while True:
        result = q.get()
        ev = ServerSentEvent(str(result))
        yield ev.encode()
    except GeneratorExit: # Or maybe use flask signals
      subscriptions.remove(q)

  return Response(gen(), mimetype="text/event-stream")



if __name__ == '__main__':
  
  application.debug = True
  server = WSGIServer(("", 5000), application)
  server.serve_forever()

  # application.run(debug=True)