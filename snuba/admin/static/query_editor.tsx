import React, { useEffect, useState, ReactElement } from "react";

import { Box } from "@mantine/core";
import { SQLEditor } from "SnubaAdmin/common/components/sql_editor";
import { useLocalStorage } from "@mantine/hooks";
import { CustomSelect } from "SnubaAdmin/select";

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

type QueryParamValues = {
  [key: string]: string;
};

type TimeRangeMode = "relative" | "absolute";
type RelativeTimeUnit = "MINUTE" | "HOUR" | "DAY";

const START_TIME_PARAM = "{{start_time}}";
const END_TIME_PARAM = "{{end_time}}";

/** @private */
export function formatAbsoluteDateTime(value: string): string {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return `toDateTime('${date.toISOString().slice(0, 19).replace("T", " ")}', 'UTC')`;
}

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
  const hash = window.location.hash;

  // Namespace the storage by the hash, which corresponds to the screen
  const [queryTemplate, setQueryTemplate] = useLocalStorage<string>({
    key: `${hash}-query-editor-query`,
    defaultValue: "",
  });

  const [queryParamValues, setQueryParamValues] = useState<QueryParamValues>(
    {}
  );
  const [selectedPredefinedQuery, setSelectedPredefinedQuery] = useState<
    PredefinedQuery | undefined
  >(undefined);
  const [timeRangeMode, setTimeRangeMode] =
    useState<TimeRangeMode>("relative");
  const [relativeTimeAmount, setRelativeTimeAmount] = useState("24");
  const [relativeTimeUnit, setRelativeTimeUnit] =
    useState<RelativeTimeUnit>("HOUR");
  const [absoluteStartTime, setAbsoluteStartTime] = useState("");
  const [absoluteEndTime, setAbsoluteEndTime] = useState("");

  const variableRegex = /{{([a-zA-Z0-9_]+)}}/;
  const hasTimeRangeParams =
    queryTemplate.includes(START_TIME_PARAM) &&
    queryTemplate.includes(END_TIME_PARAM);

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
    let values = queryParamValues;
    if (hasTimeRangeParams) {
      values = {
        ...values,
        [START_TIME_PARAM]:
          timeRangeMode === "relative"
            ? `now() - INTERVAL ${relativeTimeAmount} ${relativeTimeUnit}`
            : formatAbsoluteDateTime(absoluteStartTime),
        [END_TIME_PARAM]:
          timeRangeMode === "relative"
            ? "now()"
            : formatAbsoluteDateTime(absoluteEndTime),
      };
    }

    const newQuery = generateQuery(queryTemplate, values);
    setQuery(newQuery);
    props.onQueryUpdate(newQuery);
  }, [
    queryTemplate,
    queryParamValues,
    hasTimeRangeParams,
    timeRangeMode,
    relativeTimeAmount,
    relativeTimeUnit,
    absoluteStartTime,
    absoluteEndTime,
  ]);

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
            options={
              props.predefinedQueryOptions
                ? props.predefinedQueryOptions.map(
                    (predefinedQuery) => predefinedQuery.name
                  )
                : []
            }
          />
        </div>
      </div>
    );
  }

  function renderTimeRangeSetter() {
    if (!hasTimeRangeParams) {
      return null;
    }

    return (
      <fieldset style={timeRangeStyle}>
        <legend>Time range</legend>
        <label>
          <input
            type="radio"
            name="time-range-mode"
            checked={timeRangeMode === "relative"}
            onChange={() => setTimeRangeMode("relative")}
          />{" "}
          Relative to now
        </label>
        <label>
          <input
            type="radio"
            name="time-range-mode"
            checked={timeRangeMode === "absolute"}
            onChange={() => setTimeRangeMode("absolute")}
          />{" "}
          Absolute dates
        </label>
        {timeRangeMode === "relative" ? (
          <div style={timeRangeInputsStyle}>
            <label>
              Look back{" "}
              <input
                aria-label="Look back amount"
                type="number"
                min="1"
                value={relativeTimeAmount}
                onChange={(event) => setRelativeTimeAmount(event.target.value)}
              />
            </label>
            <select
              aria-label="Look back unit"
              value={relativeTimeUnit}
              onChange={(event) =>
                setRelativeTimeUnit(event.target.value as RelativeTimeUnit)
              }
            >
              <option value="MINUTE">minutes</option>
              <option value="HOUR">hours</option>
              <option value="DAY">days</option>
            </select>
          </div>
        ) : (
          <div style={timeRangeInputsStyle}>
            <label>
              Start{" "}
              <input
                aria-label="Start date and time"
                type="datetime-local"
                value={absoluteStartTime}
                onChange={(event) => setAbsoluteStartTime(event.target.value)}
              />
            </label>
            <label>
              End{" "}
              <input
                aria-label="End date and time"
                type="datetime-local"
                value={absoluteEndTime}
                onChange={(event) => setAbsoluteEndTime(event.target.value)}
              />
            </label>
          </div>
        )}
      </fieldset>
    );
  }

  function renderParameterSetters() {
    let setters: Array<ReactElement> = [];
    Object.keys(queryParamValues)
      .filter(
        (paramName) =>
          paramName !== START_TIME_PARAM && paramName !== END_TIME_PARAM
      )
      .forEach((paramName) => {
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

      <Box my="md">
        <SQLEditor
          value={queryTemplate}
          onChange={(newValue) => {
            setSelectedPredefinedQuery(undefined);
            setQueryTemplate(newValue);
          }}
        />
      </Box>

      {renderTimeRangeSetter()}
      {renderParameterSetters()}
    </form>
  );
}

const predefinedQueryStyle = {
  display: "inline-block",
};

const timeRangeStyle = {
  display: "flex",
  gap: 16,
  alignItems: "center",
  flexWrap: "wrap" as const,
  marginBottom: 16,
};

const timeRangeInputsStyle = {
  display: "flex",
  gap: 8,
  alignItems: "center",
};

export default QueryEditor;
