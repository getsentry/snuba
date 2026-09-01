type QueryResultColumnMetadata = [string];
type QueryResultRow = [string];

type OutcomesQueryRequest = {
  sql: string;
};

type OutcomesQueryResult = {
  input_query: string;
  column_names: QueryResultColumnMetadata;
  rows: [QueryResultRow];
  error?: string;
};

type PredefinedQuery = {
  name: string;
  sql: string;
  description: string;
};

type EnumOption = {
  value: string;
  label: string;
};

type OutcomesEnumOptions = {
  categories: EnumOption[];
  outcomes: EnumOption[];
};

export {
  EnumOption,
  OutcomesEnumOptions,
  OutcomesQueryRequest,
  OutcomesQueryResult,
  PredefinedQuery,
};
