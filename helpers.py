import os, yaml

dir_path = os.path.dirname(os.path.abspath(__file__))

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