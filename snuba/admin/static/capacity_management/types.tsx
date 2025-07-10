import { ReactNode } from "react";

type AllocationPolicy = {
  policy_name: string;
  configs: AllocationPolicyConfig[];
  optional_config_definitions: AllocationPolicyOptionalConfigDefinition[];
  query_type: string;
};

type AllocationPolicyConfig = {
  name: string;
  value: string;
  description: string;
  type: string;
  params: object;
};

type AllocationPolicyConfigParams = {
  name: string;
  type: string;
};

type AllocationPolicyOptionalConfigDefinition = {
  name: string;
  type: string;
  default: string;
  description: string;
  params: AllocationPolicyConfigParams[];
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

// Usage
function getEntityName(entity: Entity): string {
  return entity.name; // Works for both types
}

function isStorage(entity: Entity): entity is StorageEntity {
  return entity.type === "storage";
}

function isStrategy(entity: Entity): entity is StrategyEntity {
  return entity.type === "strategy";
}

export {
  AllocationPolicy,
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
  AllocationPolicyConfigParams,
  RowData,
  Entity,
  getEntityName,
  isStorage,
  isStrategy,
  StorageEntity,
  StrategyEntity,
};
