from api import engine
from infa_classes import (
  Infa_Rep,
  eUI_FolderTreeInfaObjects
)

from flask import *
import json

from helpers import make_celery

application = Flask(__name__)

application.config.update(
    CELERY_BROKER_URL='sqla+sqlite:///',
    CELERY_BACKEND='sqla+sqlite:///'
)

celery = make_celery(application)

@celery.task()
def add_together(a, b):
  return a + b

@application.route('/monitor', methods=['GET'])
def status():
  return render_template('index.html')

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

@application.route('/<object>.html', methods=['GET','POST'])
def get_content(object):
  return test_content

@application.route('/test', methods=['GET','POST'])
def test():
 return render_template('test.html')

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

    
  return json.dumps(data)


infa_objects = eUI_FolderTreeInfaObjects()
if __name__ == '__main__':
  folders = [
    # 'SOR_BLUEBOX',
    'BIDW_RMS',
    'BIDW_PROCUREMENT',
    # 'ARIBA',
  ]

  Repo = Infa_Rep(engine)
  Repo.get_list_folders()
  for i, folder_name in enumerate(sorted(Repo.folders)):
    if not folder_name in folders: continue

    folder = Repo.folders[folder_name]
    folder.get_list_sources()
    folder.get_list_targets()
    folder.get_list_mappings()
    folder.get_list_sessions()
    folder.get_list_workflows()
    infa_objects.add_folder(folder)

  application.run(debug=True)