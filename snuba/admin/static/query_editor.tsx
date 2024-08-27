import React, { useEffect, useState, ReactElement } from "react";

import { Prism } from "@mantine/prism";
import { Textarea } from "@mantine/core";
import { CustomSelect } from "SnubaAdmin/select";

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

type QueryParamValues = {
  [key: string]: string;
};

/** @private */
export function generateQuery(
  queryTemplate: string,
  queryParamValues: QueryParamValues
) {
  let query = queryTemplate;
  Object.keys(queryParamValues).forEach((param) => {
    if (queryParamValues[param]) {
      query = query.split(param).join(queryParamValues[param]);
    }
  });
  return query;
}

/** @private */
export function mergeQueryParamValues(
  newQueryParams: Set<string>,
  oldQueryParamValues: QueryParamValues
) {
  return Array.from(newQueryParams).reduce(
    (o, paramName) => ({
      ...o,
      [paramName]:
        paramName in oldQueryParamValues ? oldQueryParamValues[paramName] : "",
    }),
    {}
  );
}

function QueryEditor(props: {
  onQueryUpdate: (query: string) => void;
  predefinedQueryOptions?: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<string>("");
  const [queryTemplate, setQueryTemplate] = useState<string>("");
  const [queryParamValues, setQueryParamValues] = useState<QueryParamValues>(
    {}
  );
  const [selectedPredefinedQuery, setSelectedPredefinedQuery] = useState<
    PredefinedQuery | undefined
  >(undefined);

  const variableRegex = /{{([a-zA-Z0-9_]+)}}/;

  useEffect(() => {
    const newQueryParams = new Set(
      queryTemplate.match(
        new RegExp(variableRegex.source, variableRegex.flags + "g")
      )
    );
    setQueryParamValues((oldQueryParamValues) =>
      mergeQueryParamValues(newQueryParams, oldQueryParamValues)
    );
  }, [queryTemplate]);

  useEffect(() => {
    const newQuery = generateQuery(queryTemplate, queryParamValues);
    setQuery(newQuery);
    props.onQueryUpdate(newQuery);
  }, [queryTemplate, queryParamValues]);

  function updateQueryParameter(name: string, value: string) {
    setQueryParamValues((queryParams) => ({ ...queryParams, [name]: value }));
  }

  function renderPredefinedQueriesSelectors() {
    return (
      <div>
        <label>Predefined query: </label>
        <div style={predefinedQueryStyle}>
          <CustomSelect
            value={selectedPredefinedQuery?.name ?? "undefined"}
            onChange={(value) => {
              let selectedPredefinedQuery = props?.predefinedQueryOptions?.find(
                (predefinedQuery) => predefinedQuery.name == value
              );
              setSelectedPredefinedQuery(selectedPredefinedQuery);
              setQueryTemplate(selectedPredefinedQuery?.sql ?? "");
            }}
            name="predefined query"
            options={props.predefinedQueryOptions ? props.predefinedQueryOptions.map((predefinedQuery) => predefinedQuery.name) : []}
          />
        </div>
      </div>
    );
  }

  function renderParameterSetters() {
    let setters: Array<ReactElement> = [];
    Object.keys(queryParamValues).forEach((paramName) => {
      setters.push(
        <div key={paramName}>
          <div>
            <label>
              {paramName.match(variableRegex)?.[1]}
              <br />
              <textarea
                value={queryParamValues[paramName]}
                onChange={(evt) => {
                  updateQueryParameter(paramName, evt.target.value);
                }}
                data-testid="parameter-value"
              />
            </label>
          </div>
          <hr />
        </div>
      );
    });
    return setters;
  }

  return (
    <form>
      {props.predefinedQueryOptions != null &&
        renderPredefinedQueriesSelectors()}
      {selectedPredefinedQuery?.description ? (
        <p>{selectedPredefinedQuery?.description}</p>
      ) : null}
      <Textarea
        value={queryTemplate || ""}
        onChange={(evt) => {
          setSelectedPredefinedQuery(undefined);
          setQueryTemplate(evt.target.value);
        }}
        placeholder="Write your query here. To add variables, use '{{ }}' around substrings you wish to replace, e.g. {{ label }}"
        autosize
        minRows={2}
        maxRows={8}
        data-testid="text-area-input"
      />
      {renderParameterSetters()}
      <Prism withLineNumbers language="sql">
        {query || ""}
      </Prism>
    </form>
  );
}

const predefinedQueryStyle = {
  display: "inline-block",
};

export default QueryEditor;
