import React, { useEffect, useState, ReactElement } from "react";

import { RichTextEditor } from "@mantine/tiptap";
import { useEditor } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import Placeholder from "@tiptap/extension-placeholder";
import { Editor } from "@tiptap/core";
import { Prism } from "@mantine/prism";

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
  const textAreaStyle = { width: "100%", height: 140 };

  const editor = useEditor({
    extensions: [
      StarterKit,
      Placeholder.configure({
        placeholder:
          "Write your queries here. To add variables, use '{{ }}' around substrings you wish to replace, e.g. {{ label }}",
      }),
    ],
    content: `${queryTemplate}`,
    onUpdate({ editor }) {
      setQueryTemplate(editor.getText());
    },
  });

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

  function renderPredefinedQueriesSelectors(editor?: Editor | null) {
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
            editor?.commands.clearContent();
            editor?.commands.insertContent(selectedPredefinedQuery?.sql ?? "");
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
        </div>
      );
    });
    return setters;
  }

  return (
    <form>
      {renderPredefinedQueriesSelectors(editor)}
      {selectedPredefinedQuery?.description ? (
        <p>{selectedPredefinedQuery?.description}</p>
      ) : null}

      <RichTextEditor editor={editor}>
        <RichTextEditor.Content />
      </RichTextEditor>

      {/* <textarea
        value={queryTemplate || ""}
        placeholder={
          "Edit your queries here, add '{{ }}' around substrings you wish to replace, e.g. {{ label }}"
        }
        style={textAreaStyle}
        onChange={(evt) => {
          setSelectedPredefinedQuery(undefined);
          setQueryTemplate(evt.target.value);
        }}
        data-testid="text-area-input"
      /> */}
      {renderParameterSetters()}
      {/* <textarea
        disabled={true}
        placeholder={"The final query you send to snuba will be here"}
        style={textAreaStyle}
        value={query}
        data-testid="text-area-output"
      ></textarea> */}
      <Prism withLineNumbers language="sql">
        {query || ""}
      </Prism>
    </form>
  );
}

export default QueryEditor;
