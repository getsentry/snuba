import React, { useState, useEffect } from "react";
import Client from "../api_client";
import QueryDisplay from "./query_display";

function ProductionQueries(props: { api: Client }) {
  return (
    <div>
      {QueryDisplay({
        api: props.api,
      })}
    </div>
  );
}

export default ProductionQueries;
