import os, yaml, itertools
from celery import Celery

class dict2(dict):
  """ Dict with attributes getter/setter. """
  def __getattr__(self, name):
    return self[name]
  
  def __setattr__(self, name, value):
    self[name] = value

def parse_yaml(file_path):
  with open(file_path, 'r') as stream:
    data = yaml.load(stream)
    return dict2(data)


def make_celery(app):
  celery = Celery(app.import_name, backend=app.config['CELERY_BACKEND'],
                  broker=app.config['CELERY_BROKER_URL'])
  celery.conf.update(app.config)
  TaskBase = celery.Task
  class ContextTask(TaskBase):
    abstract = True
    def __call__(self, *args, **kwargs):
      with app.app_context():
        return TaskBase.__call__(self, *args, **kwargs)
  celery.Task = ContextTask
  return celery

def split_list(iterable, size):
  "Yield successive size-sized chunks from iterable."
  it = iter(iterable)
  item = list(itertools.islice(it, size))
  while item:
    yield item
    item = list(itertools.islice(it, size))

d = dict
d2 = dict2
dir_path = os.path.dirname(os.path.abspath(__file__))