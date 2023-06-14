import React, { useState } from "react";
import Client from "../api_client";
import QueryEditor from "../query_editor";

function QueryDisplay(props: { api: Client }) {
  return (
    <div>
      <h2>Construct a Production Query</h2>
      <QueryEditor onQueryUpdate={(sql) => {}} />
      <div style={executeActionsStyle}>
        <div>
          <button
            onClick={(evt) => {
              evt.preventDefault();
            }}
            style={executeButtonStyle}
            disabled={true}
          >
            Execute Query
          </button>
        </div>
      </div>
      <div>
        <h2>Query results</h2>
      </div>
    </div>
  );
}

const executeActionsStyle = {
  display: "flex",
  justifyContent: "space-between",
  marginTop: 8,
};

const executeButtonStyle = {
  height: 30,
  border: 0,
  padding: "4px 20px",
  marginRight: 10,
};

export default QueryDisplay;
