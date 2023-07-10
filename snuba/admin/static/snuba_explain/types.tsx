type SnubaDatasetName = string;
type SnQLQueryState = Partial<SnQLRequest>;

type SnQLRequest = {
  dataset: string;
  query: string;
};

type QueryTransformData = {
  original_query: string;
  new_query: string;
};

type ExplainStep = {
  category: string;
  type: string;
  name: string;
  data: object;
};

type ExplainResult = {
  original_ast: string;
  steps: ExplainStep[];
};

type SnQLResult = {
  input_query?: string;
  sql: string;
  explain?: ExplainResult;
};

export {
  SnubaDatasetName,
  SnQLRequest,
  SnQLResult,
  SnQLQueryState,
  ExplainResult,
  ExplainStep,
  QueryTransformData,
};
