import { ReactNode } from "react";

type AllocationPolicy = {
  storage_name: string;
  allocation_policy: string;
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

type RowData = [
  ReactNode,
  ReactNode,
  ReactNode,
  ReactNode,
  ReactNode,
  ReactNode
];

export {
  AllocationPolicy,
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
  AllocationPolicyConfigParams,
  RowData,
};
