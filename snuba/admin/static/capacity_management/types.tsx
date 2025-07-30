import { ReactNode } from "react";


interface ConfigurableComponent {
  type: string;
  name: string;
  configs: Configuration[];
  optional_config_definitions: OptionalConfigurationDefinition[];
};

interface AllocationPolicy extends ConfigurableComponent {
  type: "allocation_policy";
  query_type: string;
};

interface RoutingStrategy extends ConfigurableComponent {
  type: "routing_strategy";
}

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

interface StorageEntity {
  type: "storage";
  name: string;
}

interface StrategyEntity {
  type: "strategy";
  name: string;
}

type Entity = StorageEntity | StrategyEntity;

function getEntityName(entity: Entity): string {
  return entity.name;
}

function isStorage(entity: Entity): entity is StorageEntity {
  return entity.type === "storage";
}

function isStrategy(entity: Entity): entity is StrategyEntity {
  return entity.type === "strategy";
}

export {
  AllocationPolicy,
  Configuration,
  OptionalConfigurationDefinition,
  ConfigurationParams,
  RowData,
  Entity,
  getEntityName,
  isStorage,
  isStrategy,
  StorageEntity,
  StrategyEntity, ConfigurableComponent, RoutingStrategy
};
