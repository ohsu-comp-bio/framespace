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

    # major keyspace information
    _ksmajor_map = getRequired(config, 'keyspace_major')
    self.ksmajor_file = getRequired(_ksmajor_map, 'file')
    self.ksmajor_name = getRequired(_ksmajor_map, 'name')
    self.ksmajor_keys = getRequired(_ksmajor_map, 'keys')
    self.ksmajor_axis = getRequired(_ksmajor_map, 'axis')
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

