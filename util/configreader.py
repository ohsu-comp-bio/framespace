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
    print 'this is the value of transpose', self.transpose
    print 'this is the value of infer_units', self.infer_units

    # major keyspace information
    # not required
    self.ksmajor_map = config.get('keyspace_major', None)
    if self.ksmajor_map is not None:
        self.ksmajor_file = self.ksmajor_map.get('file', None)
        self.ksmajor_name = self.ksmajor_map.get('name', None)
        self.ksmajor_keys = self.ksmajor_map.get('keys', None)
        self.ksmajor_axis = self.ksmajor_map.get('axis', None)
    # self.ksmajor_tsvmap = getRequired(_ksmajor_map, 'tsv_keyspace_map')

    # minor keyspace information
    _ksminor_map = getRequired(config, 'keyspace_minor')
    self.ksminor_id = getRequired(_ksminor_map, 'id')
    self.ksminor_name = getRequired(_ksminor_map, 'name')
    self.ksminor_filter = _ksminor_map.get('filter', None)
    self.ksminor_axis = getRequired(_ksminor_map, 'axis')
 
    # axes
    self.axes = config.get('axes', None)
    # valid format correct
    if self.axes is not None:
      for ax in self.axes:
        getRequired(ax, 'name')

    # units
    if not self.infer_units:
        self.units = config.get('units', [])
        if len(self.units) >= 1:
          for unit in self.units:
            getRequired(unit, 'name')
            # units require a description
            getRequired(unit, 'description')
        else:
          raise ValueError("At least one unit must be specified.")

    # units
    self.units = config.get('units', None)

def getRequired(dictionary, field):
  item = dictionary.get(field, None)
  if item == None:
    raise ValueError(field + " must be set in import config.")
  return item

