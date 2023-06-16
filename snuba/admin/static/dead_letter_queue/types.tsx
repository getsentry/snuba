type Topic = {
  logicalName: string;
  physicalName: string;
  slice: number | null;
  storage: string;
};

type Policy = "reinsert-dlq" | "stop-on-error" | "drop-invalid-messages";

type ReplayInstruction = {
  messagesToProcess: number;
  policy: Policy;
};

export { Policy, Topic, ReplayInstruction };
