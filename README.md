# FrameSpace Reference Server

Reference server implementation for FrameSpace using Python Flask, MongoDB, and Protobufs. See [here](https://github.com/ohsu-computational-biology/ccc_api/tree/master/proto/framespace) for more information on FrameSpace.

## Endpoint Descriptions

### SearchAxes `/axes/search`

Search available axes.

#### AxesSearchRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
names | repeated string | [] | No | List of axes names to search over. | Yes
pageSize | int32 | 0 | No | Number of axes to return. | No
pageToken | string | "" | No | Page token to begin searching over. | No

#### AxesSearchResponse



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

#### UnitsSearchRequest

Field | Type | Default | Required | Description | Supported
--- | --- | --- | --- | --- | ---
ids | repeated string | [] | No | List of unit ids to search over. | Yes
names | repeated string | [] | No | List of unit names to search over. | Yes
pageSize | int32 | 0 | No | Number of axes to return. | No
pageToken | string | "" | No | Page token to begin searching over. | No

**Note**: when ids and names are both specified, units satisfying both filter fields will only be returned. 

#### UnitsSearchRequest

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



