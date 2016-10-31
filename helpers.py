import os, yaml, itertools, threading

from threading import Thread, Lock
from functools import wraps

from collections import (
  OrderedDict,
  namedtuple
)

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


'''
from celery import Celery
def make_celery(app):
  class CELERY_CONFIG(object):
    CELERY_BACKEND='sqlalchemy'
    CELERY_BROKER_URL='sqla+sqlite:///:memory:'
    # BROKER_URL = "memory://"
    CELERY_CACHE_BACKEND = "cache://memory"
    # CELERY_RESULT_BACKEND = "memory://"
  
  # celery = Celery(app.import_name, backend=app.config['CELERY_BACKEND'],
  #                 broker=app.config['CELERY_BROKER_URL'])
  # celery.conf.update(app.config)

  celery = Celery("task")
  celery.config_from_object(CELERY_CONFIG)
  TaskBase = celery.Task
  class ContextTask(TaskBase):
    abstract = True
    def __call__(self, *args, **kwargs):
      with app.app_context():
        return TaskBase.__call__(self, *args, **kwargs)
  celery.Task = ContextTask
  return celery
'''

def split_list(iterable, size):
  "Yield successive size-sized chunks from iterable."
  it = iter(iterable)
  item = list(itertools.islice(it, size))
  while item:
    yield item
    item = list(itertools.islice(it, size))

def export_data_to_csv(output_path, headers, data_records):
  q = lambda x: '"' + x.replace('"','""') + '"'
  with open(output_path, 'w') as out_file:
    out_file.write(','.join(headers) + '\n')
    for rec in data_records:
      out_file.write(','.join([q(rec[h]) for h in headers]) + '\n')

class ServerSentEvent():
  "SSE 'protocol' is described here: http://mzl.la/UPFyxY"
  def __init__(self, data):
    self.data = data
    self.event = None
    self.id = None
    self.desc_map = {
      self.data : "data",
      self.event : "event",
      self.id : "id"
    }

  def encode(self):
    if not self.data:
      return ""
    lines = ["%s: %s" % (v, k)
              for k, v in self.desc_map.iteritems() if k]
    
    return "%s\n\n" % "\n".join(lines)

all_threads = OrderedDict()
thLock = Lock()

def interrupt():
  global all_threads, thLock
  for th in all_threads.values():
    with thLock:
      th.cancel()

def run_async(func):
  """
    run_async(func)
      function decorator, intended to make "func" run in a separate
      thread (asynchronously).
      Returns the created Thread object

      E.g.:
      @run_async
      def task1():
        do_something

      @run_async
      def task2():
        do_something_too

      t1 = task1()
      t2 = task2()
      ...
      t1.join()
      t2.join()
  """

  # clear out idle threads
  # with thLock:
  #   for k in all_threads:
  #     if not all_threads[k].isAlive():
  #       del(all_threads[k])
  
  @wraps(func)
  def async_func(*args, **kwargs):
    global all_threads, thLock

    f_name = '_'.join([func.__name__, str(args), str(kwargs)])
    func_hl = None

    if f_name in all_threads:
      if all_threads[f_name].isAlive():
        func_hl = all_threads[f_name]
    
    if not func_hl:
      func_hl = Thread(target = func, args = args, kwargs = kwargs)
      func_hl.start()
    
    with thLock:
      all_threads[f_name] = func_hl

    return func_hl

  return async_func


d = dict
d2 = dict2
dir_path = os.path.dirname(os.path.abspath(__file__))