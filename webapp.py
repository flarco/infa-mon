from api import engines
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
  interrupt,
  d2,
)

application = Flask(__name__)
subscriptions = []

# atexit.register(interrupt)



def push_event(text):
  msg = str(text)
  for sub in subscriptions[:]:
    sub.put(msg)



@run_async
def refresh_run_stat(env):
  Repo[env].get_latest_run_stats()



@run_async
def refresh_folder(env, folder_name):
  gevent.spawn(push_event(str(datetime.datetime.now()) + ' - START - ' + folder_name))
  Repo[env].objects.get_folder_objects(folder_name).join()
  gevent.spawn(push_event(str(datetime.datetime.now()) + ' - END - ' + folder_name))
  gevent.spawn(push_event('refreshObjectTree'))


def create_repo(name, engine):
  repo = Infa_Rep(name, engine)
  repo.get_list_folders()
  repo.get_latest_run_stats()

  return repo

Repo = d2()
Repo['dev'] = create_repo('dev', engines.dev)
Repo['qa'] = create_repo('qa', engines.qa)
Repo['prd'] = create_repo('prd', engines.prd)

# RepoDev = create_repo(engines.dev)
# refresh_folder('BIDW_RMS')

@application.route('/objects', methods=['GET'])
def objects():
  return render_template('objects.html')

@application.route('/poll_mon_data', methods=['GET'])
def poll_mon_data():
  record = request.values.to_dict()
  refresh_run_stat(record['env'])
  return "OK Polling " + record['env']


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
    content = content.format(error_message=Repo[env].run_stats_data[combo].error)
  
  return content

@application.route('/test', methods=['GET','POST'])
def test():
 return render_template('test.html')


@application.route('/<object>.json', methods=['GET','POST'])
def get_data(object):
  data = []
  record = request.values.to_dict()
  env = record['env']

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
        children = Repo[env].objects.root,
      ),
      dict(
        text='QA',
        state='closed',
        children = Repo[env].objects.root,
      ),
      dict(
        text='PRD',
        state='closed',
        children = Repo[env].objects.root,
      ),
    ]

  if object == 'object_tree_search':
    q_text = ''
    for folder_name in RepoDev.objects.folders:
      RepoDev.objects.add_folder(folder_name, q_text=q_text)
    
    data = [
      dict(
        text='DEV',
        state='closed',
        children = RepoDev.objects.root,
      ),
      dict(
        text='QA',
        state='closed',
        children = RepoDev.objects.root,
      ),
      dict(
        text='PRD',
        state='closed',
        children = RepoDev.objects.root,
      ),
    ]

  if object == 'monitor_data':
    data = [Repo[env].run_stats_data[i] for i in sorted(Repo[env].run_stats_data, reverse=True)]
      
    
  return json.dumps(data)

@application.route("/refresh")
def refresh1():
  RepoDev.get_list_folders()
  env = request.values.to_dict()['env']
  folders = [
    'SOR_BLUEBOX',
    'BIDW_RMS',
    'BIDW_PROCUREMENT',
    'ARIBA',
  ]

  for i, folder_name in enumerate(sorted(Repo[env].folders)):
    if not folder_name in folders: continue
    refresh_folder(env, folder_name)
  
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