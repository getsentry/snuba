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
export { AllocationPolicy, AllocationPolicyConfig };
