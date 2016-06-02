# FrameSpace Reference Server

Reference server implementation for FrameSpace using Python Flask, MongoDB, and Protobufs. See [here](https://github.com/ohsu-computational-biology/ccc_api/tree/master/proto/framespace) for more information on FrameSpace.

## Endpoint Descriptions

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
$ curl -H "Content-Type: application/json" -X POST -d {} http://localhost:5000/axes/search

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

#### SearchUnitsRequest

```
{
  "names": [
    "tcga-gene-expression"
  ]
}
```

Note that a blank response object will return all available units.

```
curl -H "Content-Type: application/json" -X POST -d {} http://localhost:5000/units/search

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
curl -H "Content-Type: application/json" -X POST -d searchobj http://localhost:5000/keyspaces/search

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
    "mask", "TCGA-ZQ-A9CR-01A-11R-A39E-31", "TCGA-ZR-A9CJ-01B-11R-A38D-31"
  ]
}
```

```

curl -H "Content-Type: application/json" -X POST -d searchobj http://localhost:5000/keyspaces/search

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
