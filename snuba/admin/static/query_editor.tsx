import React, { useEffect, useState, ReactElement } from "react";

import { Box } from "@mantine/core";
import { SQLEditor } from "./common/components/sql_editor";
import { useLocalStorage } from "@mantine/hooks";

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
  queryParamValues: QueryParamValues,
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
  oldQueryParamValues: QueryParamValues,
) {
  return Array.from(newQueryParams).reduce(
    (o, paramName) => ({
      ...o,
      [paramName]:
        paramName in oldQueryParamValues ? oldQueryParamValues[paramName] : "",
    }),
    {},
  );
}

function QueryEditor(props: {
  onQueryUpdate: (query: string) => void;
  predefinedQueryOptions?: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<string>("");
  const hash = window.location.hash;

  // Namespace the storage by the hash, which corresponds to the screen
  const [queryTemplate, setQueryTemplate] = useLocalStorage<string>({
    key: `${hash}-query-editor-query`,
    defaultValue: "",
  });

  const [queryParamValues, setQueryParamValues] = useState<QueryParamValues>(
    {},
  );
  const [selectedPredefinedQuery, setSelectedPredefinedQuery] = useState<
    PredefinedQuery | undefined
  >(undefined);

  const variableRegex = /{{([a-zA-Z0-9_]+)}}/;

  useEffect(() => {
    const newQueryParams = new Set(
      queryTemplate.match(
        new RegExp(variableRegex.source, variableRegex.flags + "g"),
      ),
    );
    setQueryParamValues((oldQueryParamValues) =>
      mergeQueryParamValues(newQueryParams, oldQueryParamValues),
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
        <select
          value={selectedPredefinedQuery?.name ?? "undefined"}
          onChange={(evt) => {
            let selectedPredefinedQuery = props?.predefinedQueryOptions?.find(
              (predefinedQuery) => predefinedQuery.name == evt.target.value,
            );
            setSelectedPredefinedQuery(selectedPredefinedQuery);
            setQueryTemplate(selectedPredefinedQuery?.sql ?? "");
          }}
          data-testid="select"
        >
          <option value={"undefined"} data-testid="select-option">
            Custom query
          </option>
          {props.predefinedQueryOptions?.map((predefinedQuery) => (
            <option
              key={predefinedQuery.name}
              value={predefinedQuery.name}
              data-testid="select-option"
            >
              {predefinedQuery.name}
            </option>
          ))}
        </select>
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
        </div>,
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

      <Box my="md">
        <SQLEditor
          value={queryTemplate}
          onChange={(newValue) => {
            setSelectedPredefinedQuery(undefined);
            setQueryTemplate(newValue);
          }}
        />
      </Box>

      {renderParameterSetters()}
    </form>
  );
}

export default QueryEditor;
