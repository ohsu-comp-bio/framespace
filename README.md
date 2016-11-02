# FrameSpace Reference Server

Reference server implementation for FrameSpace using Python Flask, MongoDB, and Protobufs. See [here](https://github.com/ohsu-computational-biology/ccc_api/tree/master/proto/framespace) for more information on FrameSpace.

## Spin up with docker-compose

Use docker-compose to spin up the FrameSpace reference server, assuming usage with a docker-machine named `default`:

1. Clone framespace-ref: `git clone https://github.com/ohsu-computational-biology/framespace-ref.git`

1. `cd framespace-ref`

1. `docker-compose build`

1. `docker-compose up`

1. Set up a virtualenv for importing:

```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
pip install pandas
```

1. Load test gene-expression data:

```
cd util
python importer.py -c ../test/data/import.config -i ../test/data/tcgaLive*.tsv -H $(docker-machine ip default)
```

1. Load test clinical data:
```
python importer.py -c ../test/data/clinical/import.config -i ../test/data/clinical/nationwidechildrens.org_ACC_bio.patient.tsv -H $(docker-machine ip default)
```

1. Query the system: `curl -H "Content-Type: application/json" -X POST -d '{}' http://$(docker-machine ip default):5000/axes/search`

1. Make more requests by following endpoint documentation below!

## Endpoint Descriptions

See [here](http://ohsu-computational-biology.github.io/ccc_api/) for full documentation. 
The below is a summary of the available POST methods.

### SearchAxes `/axes/search`

Search available axes.

#### SearchAxesRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
names | repeated string | [] | No | List of axes names to search over. | Yes
pageSize | int32 | 0 | No | Number of units to return. | No
pageToken | string | "" | No | Page token to begin searching over. | No

#### SearchAxesResponse



Example:

```
{
  "names": ["sample"],
  "pageSize": null,
  "pageToken": null
}
```

Note that a blank response object will return all available axes:

```
$ curl -H "Content-Type: application/json" -X POST -d {} http://$(docker-machine ip default):5000/axes/search

{
  "axes": [
    {
      "description": "sample identifiers",
      "name": "sample"
    },
    {
      "description": "gene identifiers",
      "name": "gene"
    },
    {
      "description": "clinical variables",
      "name": "clinical"
    }
  ],
  "nextPageToken": null
}
```

### SearchUnits `/units/search`

Search available units.

#### SearchUnitsRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
ids | repeated string | [] | No | List of unit ids to search over. | Yes
names | repeated string | [] | No | List of unit names to search over. | Yes
pageSize | int32 | 0 | No | Number of axes to return. | No
pageToken | string | "" | No | Page token to begin searching over. | No

**Note**: When ids and names are both specified, units satisfying both filter fields will only be returned. If an invalid id/name is in the list, no value will be returned.

#### SearchUnitsResponse

```
{
  "names": [
    "tcga-gene-expression"
  ]
}
```

Note that a blank response object will return all available units.

```
curl -H "Content-Type: application/json" -X POST -d {} http://$(docker-machine ip default):5000/units/search

{
  "nextPageToken": null,
  "units": [
    {
      "description": "RSEM expression estimates are normalized to set the upper quartile count at 1000 for gene level",
      "id": "5746202b2ed58fc9cfcb37a1",
      "name": "tcga-gene-expression"
    }
  ]
}
```

### SearchKeySpaces `/keyspaces/search`

Search available KeySpaces.


##### SearchKeySpacesRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
axisNames | repeated string | [] | Yes | Return keyspaces from these axes only. | Yes
keyspaceIds | repeated string | [] | No | List of keyspace ids to search over. | Yes
names | repeated string | [] | No | List of keyspace names to search over. | Yes
keys | repeated string | [] | No | Return keyspaces with these keys only. | Yes
pageSize | int32 | 0 | No | Number of keyspaces to return. | No
pageToken | string | "" | No | Page token to begin searching over. | No

##### SearchKeySpacesResponse

Example:

```
{
  "axisNames": [
    "sample"
  ],
  "keys": [
    "TCGA-ZQ-A9CR-01A-11R-A39E-31", "TCGA-ZR-A9CJ-01B-11R-A38D-31"
  ]
}

```

```
curl -H "Content-Type: application/json" -X POST --data @searchobj http://$(docker-machine ip default):5000/keyspaces/search

{
  "keyspaces": [
    {
      "axisName": "sample",
      "id": "5746202cb52628d31deecb94",
      "keys": [
        "TCGA-3M-AB46-01A-11R-A414-31",
        "TCGA-3M-AB47-01A-22R-A414-31",
        ...
      ],
      "metadata": {},
      "name": "tcga.STAD"
    },
    {
      "axisName": "sample",
      "id": "5746202cb52628d31deecb95",
      "keys": [
        "TCGA-2H-A9GF-01A-11R-A37I-31",
        "TCGA-2H-A9GG-01A-11R-A37I-31",
        ...
      ],
      "metadata": {},
      "name": "tcga.ESCA"
    }
  ],
  "nextPageToken": null
}
```

**Note**: If no keys returned is the desired behavior, specify "mask" as the first elemnet in the `keys` field:

```
{
  "axisNames": [
    "sample"
  ],
  "keys": [
    "mask", 
    "TCGA-ZQ-A9CR-01A-11R-A39E-31", "TCGA-ZR-A9CJ-01B-11R-A38D-31"
  ]
}
```

```

curl -H "Content-Type: application/json" -X POST --data @searchobj http://$(docker-machine ip default):5000/keyspaces/search

{
  "keyspaces": [
    {
      "axisName": "sample",
      "id": "5746202cb52628d31deecb94",
      "keys": [],
      "metadata": {},
      "name": "tcga.STAD"
    },
    {
      "axisName": "sample",
      "id": "5746202cb52628d31deecb95",
      "keys": [],
      "metadata": {},
      "name": "tcga.ESCA"
    }
  ],
  "nextPageToken": null
}

```

### DataFramesSearch `/dataframes/search`

Search available dataframes. Contents are omitted from dataframes by default, since contents of a dataframe can be retrieved from `/dataslice/search` endpoint.

#### SearchDataFramesRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
keyspaceIds | repeated string | [] | Yes | Return dataframes with dimensions from the following keyspace ids | Yes
dataframeIds | repeated string | [] | No | Return dataframes with the following dataframe ids | Yes
unitIds | repeated string | [] | No | Return dataframes with the following units | No
pageSize | int32 | 0 | No | Number of dataframes to return. | No
pageToken | string | "" | No | Page token to begin searching over. | No

**Note**: keyspaceIds can be retrieved from a `/keyspaces/search` request.


#### SearchDataFramesResponse

```
{
  "keyspaceIds": ["575f1216b526282d2c120192"]
}
```

```
curl -H "Content-Type: application/json" -X POST --data @searchobj http://$(docker-machine ip default):5000/dataframes/search

{
  "dataframes": [
    {
      "contents": {},
      "id": "575f1216b526282d2c120966",
      "major": {
        "keys": [
          "TCGA-OR-A5J1-01A-11R-A29S-07",
          "TCGA-OR-A5J2-01A-11R-A29S-07",
          ...
        ],
        "keyspaceId": "575f1216b526282d2c120192"
      },
      "metadata": {},
      "minor": {
        "keys": [
          "A1BG|1",
          "A1CF|29974",
          ...,
        ],
        "keyspaceId": "5746202cb52628d31deecb96"
      },
      "units": [
        {
          "description": "RSEM expression estimates are normalized to set the upper quartile count at 1000 for gene level",
          "id": "",
          "name": "tcga-gene-expression"
        }
      ]
    }
  ],
  "nextPageToken": null
}
```


**Note**: If no keys returned is the desired behavior, specify "keys-mask" as the first elemnet in the `keyspaceIds` field:

```
{
  "keyspaceIds": ["mask-keys", "575f1216b526282d2c120192"]
}
```

```
curl -H "Content-Type: application/json" -X POST --data @searchobj http://$(docker-machine ip default):5000/dataframes/search

{
  "dataframes": [
    {
      "contents": {},
      "id": "575f1216b526282d2c120966",
      "major": {
        "keys": [],
        "keyspaceId": "575f1216b526282d2c120192"
      },
      "metadata": {},
      "minor": {
        "keys": [],
        "keyspaceId": "575f1216b526282d2c1201b3"
      },
      "units": [
        {
          "description": "RSEM expression estimates are normalized to set the upper quartile count at 1000 for gene level",
          "id": "",
          "name": "tcga-gene-expression"
        }
      ]
    }
  ],
  "nextPageToken": null
}
```

### SliceDataFrame `/dataframe/slice`

Return a dataframe, subset of a dataframe, or a transposed dataframe.

#### DataFramesRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
dataframeId | string | "" | Yes | Return dataframe with this id. | Yes
newMajor | Dimension | major Dimension of dataframe | No | Return dataframe with this major dimension. | No
newMinor | Dimension | minor Dimension of dataframe | No | Return dataframe with this minor dimension. | No
pageStart | int32 | 0 | No | Start (zero-based) index of vector to return. | No
pageEnd | int32 | length of dataframe contents | No | End (zero-based, exclusive) index of vector to return. | No

#### DataFramesResponse

```
{
  "dataframeId": "575f1216b526282d2c120966",
  "pageEnd": 1
}
```

The above request will return a single vector based on database index. Keys omitted in the request below for display.

```

curl -H "Content-Type: application/json" -X POST --data @searchobj http://localhost:5000/dataframe/slice

{
  "contents": {
    "A1BG|1": {
      "TCGA-OR-A5J1-01A-11R-A29S-07": 16.3305,
      "TCGA-OR-A5J2-01A-11R-A29S-07": 9.5987,
        ...,
      "TCGA-PK-A5HB-01A-11R-A29S-07": 152.378
    }
  },
  "id": "575f1216b526282d2c120966",
  "major": {
    "keys": null,
    "keyspaceId": "575f1216b526282d2c120192"
  },
  "minor": {
    "keys": null,
    "keyspaceId": "575f1216b526282d2c1201b3"
  }
}
```

To subset based on keys:

```
{
  "dataframeId": "57607404b5262843990d406c",
  "newMajor": {
    "keys": ["TCGA-OR-A5J1-01A-11R-A29S-07", "TCGA-OR-A5J1-01A-11R-A29S-07"]
  },
  "newMinor": {
    "keys": ["A1BG|1"]
  }
}
```

```

curl -H "Content-Type: application/json" -X POST --data @searchobj http://localhost:5000/dataframe/slice

{
  "contents": {
    "A1BG|1": {
      "TCGA-OR-A5J1-01A-11R-A29S-07": 16.3305,
      "TCGA-OR-A5J2-01A-11R-A29S-07": 9.5987
    }
  },
  "id": "575f1216b526282d2c120966",
  "major": {
    "keys": null,
    "keyspaceId": "575f1216b526282d2c120192"
  },
  "minor": {
    "keys": null,
    "keyspaceId": "575f1216b526282d2c1201b3"
  }
}
```

**Note**: Transpose is not yet supported, so `newMajor` keys must be a list of keys from the existing major dimension.

## Import Process

The import process uses a config file to understand how to import the specified tsvs. The fields in this config file are summarized in the table below. Example configs can be found in the `test/data/` folder.

Field | Type | Required | Default | Description 
--- | --- | --- | --- | --- | ---
db_name | string | No | framespace | Name of mongo database to import, creates new if not existing.
keyspace_file | dict | No | {} | Dictionary of keyspace information that is to be read from a file. If this is set, all keyspace_file fields must be specified. Keyspaces are created from a metadata file (see `test/data/metadata.txt`).
keyspace_file/file | string | Conditionally | - | Path to metadata file used to create the keyspaces.
keyspace_file/name | string | Conditionally | - | Column name in metadata file that holds keyspace names.
keyspace_file/keys | string | Conditionally | - | Column name in metadata file that hold the keys.
keyspace_file/axis | string | Conditionally | - | Assign these keyspaces to this axis. All keyspaces in the metadata file must be this keyspace.
keyspace_embedded | dict | For dataframe registration | - | Map that holds import information for a keyspace that is embedded in a dataframe. Fields labelled conditionally below are required if this is set.
keyspace_embedded/id | string | Conditionally | - | Column name in tsvs to be imported which holds the embedded keyspace.
keyspace_embedded/name | string | Conditionally | - | Name of the keyspace.
keyspace_embedded/filter | string | No | - | Remove this from values in the keyspace before creating. Often useful for genes, ie. remove genes labelled "?"
keyspace_embedded/axis | string | Conditionally | - | Assign this keyspace to this axis.
axes | list | No | [] | List of axis objects to register. If empty, keyspace_embedded/axis (or keyspace_file/axis) must already be registered with framespace.
units | list | Conditionally | - | A list of at least one unit to assign to this database. Will registered only units that are not already registered (based on name).
infer_units | bool | No | - | True if units are inferred from embedded keyspace. Currently used for clinical tsvs. 
transpose | bool | No | false | True if embedded keyspace is vertical.

### TSV Translation

The initial usage is designed to support bulk loading of a set of tsvs with variable major dimensions derived from the same axis and a consistent minor dimension. For example, the tsvs in `test/data` which have samples from various groups on the X and consistent hugo gene sets on the Y. Given the bulk entry of these as major dimensions, the keyspace information is read from a metadata file like the one in `test/data/metadata.tsv`. This setup requires keyspace_file and keyspace_embedded maps defined in the import config like `test/data/import.config`. The minor keyspace in this situation is assumed to be embedded in the dataframe.

<img width="533" alt="screen shot 2016-06-16 at 11 15 50 am" align="middle" src="https://cloud.githubusercontent.com/assets/6373975/16128093/180d5fe2-33b4-11e6-846a-90c6a866732b.png">

If a user wishes to upload additional tsvs associated to prexisiting keyspaces, the keyspace_file object is omitted. If the minor dimension is associated to an already registered dimension, the matrix can be transposed by setting `"transpose": true`.

<img width="412" alt="screen shot 2016-06-16 at 11 16 25 am" align="middle" src="https://cloud.githubusercontent.com/assets/6373975/16128079/0eb0f378-33b4-11e6-8e9c-012076ef62b9.png">




