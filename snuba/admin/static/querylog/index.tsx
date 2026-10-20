import React, { useState, useEffect } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import QueryDisplay from "SnubaAdmin/querylog/query_display";
import { stickyTableHeaderStyles } from "SnubaAdmin/editor_panel";
import { parseQuerylogPrefill } from "SnubaAdmin/querylog/prefill";
import { QuerylogResult, PredefinedQuery } from "./types";

function QuerylogQueries(props: { api: Client }) {
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);
  // Pasting a share link while already on this page only changes the hash,
  // which doesn't reload the page. Remount the form so it reads the new link.
  const [linkKey, setLinkKey] = useState(0);

  useEffect(() => {
    function onHashChange() {
      if (parseQuerylogPrefill(window.location.hash)) {
        setLinkKey((key) => key + 1);
      }
    }
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    props.api.getPredefinedQuerylogOptions().then((res) => {
      res.forEach(
        (queryOption) => (queryOption.sql = formatSQL(queryOption.sql))
      );
      setPredefinedQueryOptions(res);
    });
  }, []);

  function tablePopulator(queryResult: QuerylogResult) {
    return (
      <Table
        headerData={queryResult.column_names}
        rowData={queryResult.rows}
        customStyles={stickyTableHeaderStyles}
      />
    );
  }

  function formatSQL(sql: string) {
    const formatted = sql
      .split("\n")
      .map((line) => line.substring(4, line.length))
      .join("\n");
    return formatted.trim();
  }

  return (
    <div>
      <QueryDisplay
        key={linkKey}
        api={props.api}
        resultDataPopulator={tablePopulator}
        predefinedQueryOptions={predefinedQueryOptions}
      />
    </div>
  );
}

const selectStyle = {
  marginBottom: 8,
  height: 30,
};

export default QuerylogQueries;
