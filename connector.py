import pymongo
from pymongo import MongoClient
import pandas as pd

class Connector:

  def __init__(self, database):
    """
    Connect to specified database, and ensure proper setup.
    """
    self.conn = MongoClient()
    self.db = self.conn[database]

    # check collections
    self.axis = self.db.axis
    self.keyspace = self.db.keyspace
    self.units = self.db.units
    self.vector = self.db.vector
    self.dataframe = self.db.dataframe

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    # create indexes, where should this happen?
    self.keyspace = keyspace.create_index([{"keys", pymongo.ASCENDING}])
    self.dataframe = dataframe.create_index([{"contents": 1}])
    self.vector = vector.create_index([{"contents":1}])
    # reindex?
    self.conn.close()

  def registerAxes(self, axes, ksmajor_ax, ksminor_ax):
    """
    Register any new axes secified in config. 
    """
    # first register available axes in config
    for axis in axes:
      self.axis.update({'name': axis['name']}, axis, upsert=True)

    # validate keyspace axis existence
    self.checkAxes(ksmajor_ax)
    self.checkAxes(ksminor_ax)

  def checkAxes(self, axis_name):
    """
    Check to make sure keyspace axes are actually in database.
    """
    ax = self.axis.find_one({'name': axis_name})
    if ax is None:
      raise ValueError("KeySpace Axis must be registered of specified in config axes section.")


  def registerUnits(self, units):
    """
    Register any newly specified units.
    Units is a required field in the config, so no checking is necessary.
    """
    for unit in units:
      self.units.update({'name': unit['name']}, unit, upsert=True)


  def registerMajorKeySpaces(self, metadata, name, keys, axis):
    """
    Registration of keyspaces from a metadata file.
    The Keyspaces to be registered are assumed to be unique. 
    """
    with open(metadata) as meta:
      # construct a list of unique keyspaces
      m_df = pd.read_table(meta)
      kspaces = m_df[name].unique()
      # print kspaces

      # construct a dictionary of keyspaces for bulk insert
      keyspaces = []
      for ks in kspaces:
        ks_df = m_df[m_df[name].str.contains(ks)]
        key_list = list(ks_df[keys])
        keyspaces.append({'name': ks, 'axis_name': axis, 'keys': key_list})

      del m_df
      self.keyspace.insert_many(keyspaces)

    # return keyspaces to later use during tsv processing
    return keyspaces


  def registerMinorKeySpace(self, df, ksminor_id, ksminor_name, ksminor_axis, rename=None):
    """
    Registers a minor KeySpace, any filtering is assumed to have happened prior to registration.
    ie. ksminor_filter must occur on df before running minor ks registration.
    """
    # get keys
    print 'in register minor keyspace'
    print ksminor_id
    print ksminor_name
    if rename is None:
      keys = list(df[str(ksminor_name)])
    else:
      print 'rename flag set', rename
      keys = list(df[str(rename[ksminor_id])])

    # register minor keyspace
    minor_keyspace = {"name": ksminor_name, "axis_name": ksminor_axis, "keys": keys}
    min_ks = self.keyspace.insert(minor_keyspace)

    return min_ks


  def registerDataFrame(self, df, ksminor_objid, units):
    """
    Gets respecitve major keyspace.
    Registers the lines of the tsv file as vectors.
    Register the tsv as a dataframe with pointer to vectors.
    """

    # get major keyspace
    keys = df.columns.tolist()[1:]
    md_ks = self.keyspace.find({"keys": keys[0]})
    if set(keys) <= set(md_ks[0]['keys']):
      maj_dim = md_ks[0]['_id']
    else:
      raise ValueError("Not all keys in dimension are registered to keyspace.")

    # vectors are inserted into dataframe as ids to get around data storage limit
    # print df.columns
    # vectors = map(createVector, df.reset_index(drop=True).to_dict(orient='records'))
    # for vec in df.reset_index(drop=True).to_dict(orient='records'):
    # print df_dict
    # print df_dict['key']
    # vectors = self.vector.insert_many(df.reset_index(drop=True).to_dict(orient='records'))
    vectors = self.vector.insert_many(map(createVector, df.reset_index(drop=True).to_dict(orient='records')))
    dataframe = {"major": md_ks[0]['_id'], "minor": ksminor_objid, "units": units, "contents": list(vectors.inserted_ids)}

    _id = self.dataframe.insert_one(dataframe)
    return _id

def createVector(vector):
  key = vector.pop('key')
  return {'key': key, 'contents': vector, 'info':{}}
