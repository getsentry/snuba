(
  (
    (
      sum(c:custom/data_request@none){DataProvider:\"FesDataProvider\" AND environment:[\"Prod\"]} by (DataProvider)
      -
      sum(c:custom/data_request@none){(DataProvider:\"FesDataProvider\" AND !StatusCode:\"200\") AND environment:[\"Prod\"]} by (DataProvider)
    )
  /
  sum(c:custom/data_request@none){DataProvider:\"FesDataProvider\" AND environment:[\"Prod\"]} by (DataProvider)
  )
  *
  100.0
)

"mql_context":{
  "end":"2024-08-15T00:00:00+00:00",
  "indexer_mappings":{
    "DataProvider":88000988,
    "StatusCode":88000986,
    "c:custom/data_request@none":88000985,
    "environment":9223372036854776010
  },
  "limit":715,
  "offset":null,
  "rollup":{
    "granularity":86400,
    "interval":null,
    "orderby":"DESC",
    "with_totals":"True"
  },"scope":{
    "org_ids":[4507056056369152],
    "project_ids":[4507129080643584],
    "use_case_id":"custom"
  },
  "start":"2024-05-09T00:00:00+00:00"
}
