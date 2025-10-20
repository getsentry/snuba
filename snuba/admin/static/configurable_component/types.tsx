import { ReactNode } from "react";


type Configuration = {
  name: string;
  value: string;
  description: string;
  type: string;
  params: object;
};

type ConfigurationParams = {
  name: string;
  type: string;
};

type OptionalConfigurationDefinition = {
  name: string;
  type: string;
  default: string;
  description: string;
  params: ConfigurationParams[];
};

type RowData = {
  name: ReactNode;
  params: ReactNode;
  value: ReactNode;
  description: ReactNode;
  type: ReactNode;
  edit: ReactNode;
};

type ConfigurableComponentData = {
  configurable_component_namespace: string;
  configurable_component_class_name: string;
  resource_identifier: string;
  configurations: Configuration[];
  optional_config_definitions: OptionalConfigurationDefinition[];
};

type AllocationPolicy = ConfigurableComponentData & {
  query_type: string;
};

type StrategyData = ConfigurableComponentData & {
  policies_data: AllocationPolicy[];
};

export {
  AllocationPolicy,
  StrategyData,
  Configuration,
  OptionalConfigurationDefinition,
  ConfigurationParams,
  RowData,
  ConfigurableComponentData,
};
