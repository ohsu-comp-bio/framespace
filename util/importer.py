import argparse, traceback, sys
from multiprocessing import Pool

import pandas as pd

from configreader import ConfigReader
from connector import Connector

class Importer:

  def __init__(self, config, files, host='0.0.0.0'):
    """
    Importer will setup config and perform parallel import of tsv files into mongo.
    """

    if len(files) == 0 or type(files) is not list:
      raise ValueError("List of one of more tsv files required for import.")
    
    self.config = ConfigReader(config)
    init_file = files[0]
    tsv_files = files[1:]

    self.conn = Connector(self.config.db_name, host=host)
    self.conn.registerAxes(self.config.axes, self.config.ksmajor_axis, self.config.ksminor_axis)
    self.conn.registerUnits(self.config.units)
    self.major_keyspaces = self.conn.registerMajorKeySpaces(self.config.ksmajor_file, self.config.ksmajor_name, self.config.ksmajor_keys, self.config.ksmajor_axis)

    # register the first file to establish minor keyspace
    # replace minor keyspace in tsv identifier with ksminor name
    # fix this later
    # self.rename = {self.config.ksminor_id: self.config.ksminor_name}
    self.rename = {self.config.ksminor_id: 'key'}
    print self.rename
    init_df = getDataFrame(init_file, ksminor_filter=self.config.ksminor_filter, ksminor_id=self.config.ksminor_id, rename=self.rename)
    self.minor_keyspace = self.conn.registerMinorKeySpace(init_df, self.config.ksminor_id, self.config.ksminor_name, self.config.ksminor_axis, rename=self.rename)
    
    # construct first dataframe and add to list to be registered
    try:
      self.conn.registerDataFrame(init_df, self.minor_keyspace, self.config.units)
      print "Completed: {0}".format(init_file)
    
    except Exception, e:
      traceback.print_exc(file=sys.stdout)
      print "Error processing {0}".format(tsv)
      print str(e)


    # work on the rest at once
    parallelGen(tsv_files, self.minor_keyspace, self.config.units, self.config.db_name, self.config.ksminor_filter, self.config.ksminor_id, self.rename, host)

def poolLoadTSV((tsv, ks_minor, units, db, ksm_filter, ksm_id, rename, host)):
  """
  This function is used by multiprocess.Pool to populate framespace with a new dataframe.
  """
  try:
    # register vectors and get the dataframe object for insert
    conn = Connector(db, host=host)
    df_id = conn.registerDataFrame(getDataFrame(tsv, ksminor_id=ksm_id, ksminor_filter=ksm_filter, rename=rename), ks_minor, units)
  
  except Exception, e:
    traceback.print_exc(file=sys.stdout)
    print "Error processing {0}".format(tsv)
    print str(e)
    return (-1, tsv)
  return (0, tsv)

def parallelGen(tsv_files, ks_minor, units, db, ksm_filter, ksm_id, rename, host):
  """
  Function that spawns the Pool of vector registration and dataframe production
  """

  function_args = [None] * len(tsv_files)
  index = 0
  for tsv in tsv_files:
    function_args[index] = (tsv, ks_minor, units, db, ksm_filter, ksm_id, rename, host)
    index += 1

  pool = Pool()
  failed = list()
  for returncode in pool.imap_unordered(poolLoadTSV, function_args):
    if returncode[0] == -1:
      failed.append(returncode[1])
    else:
      print "Completed: {0}".format(returncode[1])

  if len(failed):
    print "\nERROR: Following files failed to process:"
    for f in failed:
      print "\t{0}".format(f)
    raise Exception("Execution failed on {0}".format(failed))


def getDataFrame(tsv, ksminor_filter=None, ksminor_id=None, rename=None):
  """
  Tsv to pandas dataframe, and filters if specified.
  """
  # filter
  if ksminor_filter:
    if ksminor_id is None:
      raise ValueError("Provide Keyspace ID for filtering.")

    d = pd.read_table(tsv)
    df = d[d[str(ksminor_id)].str.contains(ksminor_filter)==False]

  # return no filter
  else:
    df = pd.read_table(tsv)

  # explore set with copy warning
  if rename:
    df.rename(columns=rename, inplace=True)

  return df


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description = "Populate FrameSpace Reference Server")

  parser.add_argument("-c", "--config", required=False, type=str, 
                      help="Input config.")

  parser.add_argument("-H", "--host", required=False, type=str, 
                      help="Mongo host.")

  parser.add_argument("-i", "--inputs", nargs='+', type=str, required=False, default=[], 
                    help="List of tsvs to input as DataFrames")

  args = parser.parse_args()

  importer = Importer(args.config, args.inputs, host=args.host)

  print importer.config.db_name
  
