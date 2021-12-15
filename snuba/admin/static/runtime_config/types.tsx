import { ReactNode } from "react";

type ConfigKey = string;
type ConfigValue = string;
type ConfigType = "string" | "int" | "float";

type Config = { key: ConfigKey; value: ConfigValue; type: ConfigType };

type RowData = [ReactNode, ReactNode, ReactNode, ReactNode];

type ConfigChange = {
  key: ConfigKey;
  user: string | null;
  timestamp: number;
  before: ConfigValue | null;
  beforeType: ConfigType | null;
  after: ConfigValue | null;
  afterType: ConfigType | null;
};

export { Config, ConfigKey, ConfigValue, ConfigType, RowData, ConfigChange };
