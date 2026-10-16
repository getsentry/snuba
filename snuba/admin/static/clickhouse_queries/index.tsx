import React, { useState, useEffect } from "react";
import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import QueryDisplay, {
  STICKY_HEADER_TOP,
} from "SnubaAdmin/clickhouse_queries/query_display";
import { COLORS } from "SnubaAdmin/theme";
import { parseSystemQueryPrefill } from "SnubaAdmin/clickhouse_queries/prefill";
import { QueryResult, PredefinedQuery } from "./types";

function ClickhouseQueries(props: { api: Client }) {
  const [predefinedQueryOptions, setPredefinedQueryOptions] = useState<
    PredefinedQuery[]
  >([]);
  // Pasting a share link while already on this page only changes the hash,
  // which doesn't reload the page. Remount the form so it reads the new link.
  const [linkKey, setLinkKey] = useState(0);

  useEffect(() => {
    function onHashChange() {
      if (parseSystemQueryPrefill(window.location.hash)) {
        setLinkKey((key) => key + 1);
      }
    }
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    props.api.getPredefinedQueryOptions().then((res) => {
      res.forEach(
        (queryOption) => (queryOption.sql = formatSQL(queryOption.sql))
      );
      setPredefinedQueryOptions(res);
    });
  }, []);

  function tablePopulator(queryResult: QueryResult) {
    return (
      <Table
        headerData={queryResult.column_names}
        rowData={queryResult.rows}
        customStyles={stickyHeaderStyles}
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

// Keeps column names in view below the editor pane while scrolling a long
// result. This needs no scrolling ancestor between the table and the page,
// so wide results wrap rather than scroll sideways.
const stickyHeaderStyles = {
  thStyle: {
    position: "sticky" as const,
    top: STICKY_HEADER_TOP,
    zIndex: 1,
    backgroundColor: COLORS.TABLE_HEADER_BG,
    // Collapsed borders don't move with a stuck cell, so draw its bottom
    // edge here.
    boxShadow: `inset 0 -1px 0 ${COLORS.TABLE_BORDER}`,
  },
};

export default ClickhouseQueries;
