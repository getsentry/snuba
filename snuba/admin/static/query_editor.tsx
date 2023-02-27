import React, { useEffect, useState, ReactElement } from "react";

type PredefinedQuery = {
  name: string;
  queryTemplate: string;
  description?: string;
};

function QueryEditor(props: {
  onQueryUpdate: (query: string) => void;
  predefinedQueries?: Array<PredefinedQuery>;
}) {
  const [queryTemplate, setQueryTemplate] = useState<string>("");
  const [queryParams, setQueryParams] = useState<Map<string, string>>(
    new Map<string, string>()
  );
  const [query, setQuery] = useState<string>("");
  const [predefinedQuery, setPredefinedQuery] = useState<
    PredefinedQuery | undefined
  >(undefined);

  useEffect(() => {
    applyQueryTemplate();
  }, [queryTemplate, queryParams]);

  function updateQueryTemplate(template: string) {
    setQueryTemplate((_) => template);
    let paramNames = new Set(template.match(/{{{([a-zA-Z0-9_]+)}}}/g));
    setQueryParams((queryParams) => {
      queryParams.forEach((_, key: string) => {
        if (!paramNames.has(key)) {
          queryParams.delete(key);
        }
      });
      paramNames.forEach((paramName) => {
        if (!queryParams.has(paramName)) {
          queryParams.set(paramName, "");
        }
      });
      return queryParams;
    });
  }

  function updateQueryParameter(key: string, value: string) {
    setQueryParams((queryParams) => {
      return new Map(queryParams.set(key, value));
    });
  }

  function applyQueryTemplate() {
    let sql = queryTemplate;
    queryParams.forEach((value, key) => {
      if (value) {
        sql = sql.replace(new RegExp(key, "g"), value);
      }
    });
    setQuery(sql);
    props.onQueryUpdate(sql);
  }

  function renderParameterSetters() {
    let setters: Array<ReactElement> = [];
    queryParams.forEach((value, key) => {
      setters.push(
        <div key={key}>
          <div>
            <label>{`Parameter: ${key.match(/{{{(.*?)}}}/)?.[1]}`}</label>
          </div>
          <div>
            <label>{`Value:`}</label>
          </div>
          <textarea
            value={value}
            onChange={(evt) => {
              updateQueryParameter(key, evt.target.value);
            }}
          />
          <hr />
        </div>
      );
    });
    return setters;
  }

  function renderPredefinedQueriesSelector() {
    return (
      <div>
        <label>Query template: </label>
        <select
          value={predefinedQuery?.name ?? "undefined"}
          onChange={(evt) => {
            let selectedPredefinedQuery = props?.predefinedQueries?.find(
              (predefinedQuery) => predefinedQuery.name == evt.target.value
            );
            setPredefinedQuery(selectedPredefinedQuery);
            updateQueryTemplate(selectedPredefinedQuery?.queryTemplate ?? "");
          }}
        >
          <option value={"undefined"}>Custom query template</option>
          {props.predefinedQueries?.map((predefinedQuery) => (
            <option key={predefinedQuery.name} value={predefinedQuery.name}>
              {predefinedQuery.name}
            </option>
          ))}
        </select>
      </div>
    );
  }

  return (
    <form>
      {renderPredefinedQueriesSelector()}
      {predefinedQuery?.description ? (
        <p>{predefinedQuery?.description}</p>
      ) : null}
      <textarea
        value={queryTemplate || ""}
        placeholder={"Edit your queries here"}
        style={{ width: "100%", height: 140 }}
        onChange={(evt) => {
          setPredefinedQuery(undefined);
          updateQueryTemplate(evt.target.value);
        }}
      />
      {renderParameterSetters()}
      <textarea
        disabled={true}
        placeholder={"Edit your queries in the text fields above"}
        style={{ width: "100%", height: 140 }}
        value={query}
      ></textarea>
    </form>
  );
}

export default QueryEditor;
