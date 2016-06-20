import json

class ConfigReader:

  def __init__(self, config_file):
    """
    Config Reader for TSV Import.
    """

    with open(config_file, "r") as conf:
      config = json.load(conf)

    # default framespace
    self.db_name = config.get('db_name', 'framespace')
    self.transpose = config.get('transpose', False)
    self.infer_units = config.get('infer_units', False)

    # keyspace from file map and associated fields
    self.ksf_map = config.get('keyspace_file', None)
    if self.ksf_map is not None:
        self.ksf_file = self.ksf_map.get('file', None)
        self.ksf_name = self.ksf_map.get('name', None)
        self.ksf_keys = self.ksf_map.get('keys', None)
        # restricts all keyspaces in a file to correspond to same keyspace
        self.ksf_axis = self.ksf_map.get('axis', None)

    # embedded keyspace registration, often minor keyspace
    self.ksemb_map = config.get('keyspace_embedded', None)
    if self.ksemb_map is not None:
        self.ksemb_id = self.ksemb_map.get('id', None)
        self.ksemb_name = self.ksemb_map.get('name', None)
        self.ksemb_filter = self.ksemb_map.get('filter', None)
        self.ksemb_axis = self.ksemb_map.get('axis', None)
 
    # axes
    self.axes = config.get('axes', None)
    # valid format correct
    if self.axes is not None:
      for ax in self.axes:
        getRequired(ax, 'name')

    # units
    if not self.infer_units:
        self.units = config.get('units', [])
        if len(self.units) >= 1 and self.ksemb_map is not None:
          for unit in self.units:
            getRequired(unit, 'name')
            # units require a description
            getRequired(unit, 'description')
        elif len(self.units) < 1 and self.ksemb_map is not None:
          raise ValueError("At least one unit must be specified.")

    # units
    self.units = config.get('units', None)

def getRequired(dictionary, field):
  item = dictionary.get(field, None)
  if item == None:
    raise ValueError(field + " must be set in import config.")
  return item

