import React, { useEffect, useState, ReactElement } from "react";

import {
  Box,
  Group,
  NumberInput,
  SegmentedControl,
  Select,
  Stack,
  Text,
} from "@mantine/core";
import { DateTimePicker } from "@mantine/dates";
import { SQLEditor } from "SnubaAdmin/common/components/sql_editor";
import { useLocalStorage } from "@mantine/hooks";
import { CustomSelect } from "SnubaAdmin/select";

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

type EnumOption = {
  value: string;
  label: string;
};

type QueryParamValues = {
  [key: string]: string;
};

type TimeRangeMode = "relative" | "absolute";
type RelativeTimeUnit = "MINUTE" | "HOUR" | "DAY";

const START_TIME_PARAM = "{{start_time}}";
const END_TIME_PARAM = "{{end_time}}";
const CATEGORY_PARAM = "{{category}}";
const OUTCOME_PARAM = "{{outcome}}";
const ORG_ID_PARAM = "{{org_id}}";
const PROJECT_ID_PARAM = "{{project_id}}";
const LIMIT_PARAM = "{{limit}}";

const DEFAULT_PARAM_VALUES: QueryParamValues = {
  [LIMIT_PARAM]: "100",
};

const NUMERIC_PARAMS = new Set([
  ORG_ID_PARAM,
  PROJECT_ID_PARAM,
  LIMIT_PARAM,
]);

/** @private */
export function formatAbsoluteDateTime(value: Date | null): string {
  if (!value || Number.isNaN(value.getTime())) {
    return "";
  }

  return `toDateTime('${value.toISOString().slice(0, 19).replace("T", " ")}', 'UTC')`;
}

function getRelativeStart(
  end: Date,
  amount: number,
  unit: RelativeTimeUnit
): Date {
  const millisecondsByUnit = {
    MINUTE: 60 * 1000,
    HOUR: 60 * 60 * 1000,
    DAY: 24 * 60 * 60 * 1000,
  };
  return new Date(end.getTime() - amount * millisecondsByUnit[unit]);
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
  return Array.from(newQueryParams).reduce((o, paramName) => {
    if (paramName in oldQueryParamValues) {
      return { ...o, [paramName]: oldQueryParamValues[paramName] };
    }
    if (paramName in DEFAULT_PARAM_VALUES) {
      return { ...o, [paramName]: DEFAULT_PARAM_VALUES[paramName] };
    }
    return { ...o, [paramName]: "" };
  }, {});
}

function QueryEditor(props: {
  onQueryUpdate: (query: string) => void;
  predefinedQueryOptions?: Array<PredefinedQuery>;
  categoryOptions?: Array<EnumOption>;
  outcomeOptions?: Array<EnumOption>;
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
  const [relativeTimeAmount, setRelativeTimeAmount] = useState(24);
  const [relativeTimeUnit, setRelativeTimeUnit] =
    useState<RelativeTimeUnit>("HOUR");
  const [absoluteStartTime, setAbsoluteStartTime] = useState<Date | null>(null);
  const [absoluteEndTime, setAbsoluteEndTime] = useState<Date | null>(null);

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
      const hasValidRelativeRange =
        Number.isInteger(relativeTimeAmount) && relativeTimeAmount > 0;
      const hasValidAbsoluteRange =
        absoluteStartTime !== null &&
        absoluteEndTime !== null &&
        absoluteStartTime < absoluteEndTime;
      const hasValidTimeRange =
        timeRangeMode === "relative"
          ? hasValidRelativeRange
          : hasValidAbsoluteRange;

      if (!hasValidTimeRange) {
        setQuery("");
        props.onQueryUpdate("");
        return;
      }

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

  function changeTimeRangeMode(value: string) {
    const mode = value as TimeRangeMode;
    if (mode === "absolute") {
      const end = new Date();
      setAbsoluteEndTime(end);
      setAbsoluteStartTime(
        getRelativeStart(end, relativeTimeAmount, relativeTimeUnit)
      );
    }
    setTimeRangeMode(mode);
  }

  function renderTimeRangeSetter() {
    if (!hasTimeRangeParams) {
      return null;
    }

    const absoluteRangeError =
      timeRangeMode === "absolute" &&
      absoluteStartTime !== null &&
      absoluteEndTime !== null &&
      absoluteStartTime >= absoluteEndTime
        ? "Start must be before end"
        : undefined;

    return (
      <Box component="fieldset" p="md" mb="md" style={timeRangeStyle}>
        <Text component="legend" fw={600} px={4}>
          Time range
        </Text>
        <Stack spacing="sm">
          <SegmentedControl
            aria-label="Time range mode"
            value={timeRangeMode}
            onChange={changeTimeRangeMode}
            data={[
              { label: "Relative", value: "relative" },
              { label: "Absolute", value: "absolute" },
            ]}
          />
          {timeRangeMode === "relative" ? (
            <Group align="flex-end" grow>
              <NumberInput
                label="Look back"
                aria-label="Look back amount"
                min={1}
                step={1}
                precision={0}
                value={relativeTimeAmount}
                onChange={(value) =>
                  setRelativeTimeAmount(
                    typeof value === "number" && Number.isInteger(value)
                      ? value
                      : 0
                  )
                }
              />
              <Select
                label="Unit"
                aria-label="Look back unit"
                value={relativeTimeUnit}
                onChange={(value) => {
                  if (value) {
                    setRelativeTimeUnit(value as RelativeTimeUnit);
                  }
                }}
                data={[
                  { label: "Minutes", value: "MINUTE" },
                  { label: "Hours", value: "HOUR" },
                  { label: "Days", value: "DAY" },
                ]}
              />
            </Group>
          ) : (
            <Group align="flex-start" grow>
              <DateTimePicker
                label="Start"
                aria-label="Start date and time"
                value={absoluteStartTime}
                onChange={setAbsoluteStartTime}
                valueFormat="YYYY-MM-DD HH:mm"
                error={absoluteRangeError}
                clearable
              />
              <DateTimePicker
                label="End"
                aria-label="End date and time"
                value={absoluteEndTime}
                onChange={setAbsoluteEndTime}
                valueFormat="YYYY-MM-DD HH:mm"
                error={absoluteRangeError}
                clearable
              />
            </Group>
          )}
        </Stack>
      </Box>
    );
  }

  function renderEnumParameter(
    paramName: string,
    label: string,
    options: Array<{ value: string; label: string }>
  ) {
    return (
      <Box key={paramName} mb="md" style={{ maxWidth: 420 }}>
        <Select
          label={label}
          aria-label={label}
          placeholder={`Select ${label.toLowerCase()}`}
          searchable
          clearable
          data={options}
          value={queryParamValues[paramName] || null}
          onChange={(value) => updateQueryParameter(paramName, value ?? "")}
          data-testid={`${label.toLowerCase()}-select`}
        />
      </Box>
    );
  }

  function renderNumericParameter(paramName: string, label: string) {
    const rawValue = queryParamValues[paramName];
    const numericValue =
      rawValue && Number.isFinite(Number(rawValue)) ? Number(rawValue) : "";

    return (
      <Box key={paramName} mb="md" style={{ maxWidth: 420 }}>
        <NumberInput
          label={label}
          aria-label={label}
          min={paramName === LIMIT_PARAM ? 1 : 0}
          step={1}
          precision={0}
          hideControls={false}
          value={numericValue}
          placeholder={
            paramName === LIMIT_PARAM ? "e.g. 100" : "e.g. 1"
          }
          onChange={(value) =>
            updateQueryParameter(
              paramName,
              typeof value === "number" && Number.isInteger(value)
                ? String(value)
                : ""
            )
          }
          data-testid="parameter-value"
        />
      </Box>
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
        if (paramName === CATEGORY_PARAM && (props.categoryOptions?.length ?? 0) > 0) {
          setters.push(
            renderEnumParameter(
              paramName,
              "Category",
              props.categoryOptions ?? []
            )
          );
          return;
        }
        if (paramName === OUTCOME_PARAM && (props.outcomeOptions?.length ?? 0) > 0) {
          setters.push(
            renderEnumParameter(
              paramName,
              "Outcome",
              props.outcomeOptions ?? []
            )
          );
          return;
        }
        if (NUMERIC_PARAMS.has(paramName)) {
          const label =
            paramName.match(variableRegex)?.[1]?.replace(/_/g, " ") ??
            paramName;
          setters.push(renderNumericParameter(paramName, label));
          return;
        }

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
  border: "1px solid #ced4da",
  borderRadius: 4,
  maxWidth: 640,
};

export default QueryEditor;
