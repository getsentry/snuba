import { ReactNode } from "react";

type AllocationPolicy = {
  storage_name: string;
  allocation_policy: string;
};

type AllocationPolicyConfig = {
  key: string;
  value: string;
  description: string;
  type: string;
  params: object;
};

type RowData = [ReactNode, ReactNode, ReactNode, ReactNode, ReactNode];

export { AllocationPolicy, AllocationPolicyConfig, RowData };
