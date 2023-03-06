import React, { useEffect, useState, ReactElement } from "react";

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

function QueryEditor(props: {
  onQueryUpdate: (query: string) => void;
  predefinedQueryOptions?: Array<PredefinedQuery>;
}) {
  const [query, setQuery] = useState<string>("");
  const [queryTemplate, setQueryTemplate] = useState<string>("");
  const [queryParamValues, setQueryParamValues] = useState<{
    [key: string]: string;
  }>({});
  const [selectedPredefinedQuery, setSelectedPredefinedQuery] = useState<
    PredefinedQuery | undefined
  >(undefined);

  const variableRegex = /{{([a-zA-Z0-9_]+)}}/;
  const textAreaStyle = { width: "100%", height: 140 };

  useEffect(() => {
    generateQuery();
  }, [queryTemplate, queryParamValues]);

  useEffect(() => {
    onQueryTemplateUpdate();
  }, [queryTemplate]);

  function generateQuery() {
    let sql = queryTemplate;
    Object.keys(queryParamValues).forEach((param) => {
      if (queryParamValues[param]) {
        sql = sql.split(param).join(queryParamValues[param]);
      }
    });
    setQuery(sql);
    props.onQueryUpdate(sql);
  }

  function onQueryTemplateUpdate() {
    let paramNames = new Set(
      queryTemplate.match(
        new RegExp(variableRegex.source, variableRegex.flags + "g")
      )
    );
    setQueryParamValues((prevQueryParamValues) =>
      Array.from(paramNames).reduce(
        (o, paramName) => ({
          ...o,
          [paramName]:
            paramName in prevQueryParamValues
              ? prevQueryParamValues[paramName]
              : "",
        }),
        {}
      )
    );
  }

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
              (predefinedQuery) => predefinedQuery.name == evt.target.value
            );
            setSelectedPredefinedQuery(selectedPredefinedQuery);
            setQueryTemplate(selectedPredefinedQuery?.sql ?? "");
          }}
        >
          <option value={"undefined"}>Custom query</option>
          {props.predefinedQueryOptions?.map((predefinedQuery) => (
            <option key={predefinedQuery.name} value={predefinedQuery.name}>
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
      {renderPredefinedQueriesSelectors()}
      {selectedPredefinedQuery?.description ? (
        <p>{selectedPredefinedQuery?.description}</p>
      ) : null}
      <textarea
        value={queryTemplate || ""}
        placeholder={
          "Edit your queries here, add '{{ }}' around substrings you wish to replace, e.g. {{ label }}"
        }
        style={textAreaStyle}
        onChange={(evt) => {
          setSelectedPredefinedQuery(undefined);
          setQueryTemplate(evt.target.value);
        }}
      />
      {renderParameterSetters()}
      <textarea
        disabled={true}
        placeholder={"The final query you send to snuba will be here"}
        style={textAreaStyle}
        value={query}
      ></textarea>
    </form>
  );
}

export default QueryEditor;
