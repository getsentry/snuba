import { ReactNode } from "react";

type ConfigKey = string;
type ConfigValue = string;
type ConfigDescription = string;
type ConfigType = "string" | "int" | "float";
type ConfigDescriptions = { [key: string]: string };

type Config = {
  key: ConfigKey;
  value: ConfigValue;
  description: ConfigDescription;
  type: ConfigType;
};

type RowData = [ReactNode, ReactNode, ReactNode, ReactNode, ReactNode];

type ConfigChange = {
  key: ConfigKey;
  user: string | null;
  timestamp: number;
  before: ConfigValue | null;
  beforeType: ConfigType | null;
  after: ConfigValue | null;
  afterType: ConfigType | null;
};

export {
  Config,
  ConfigKey,
  ConfigValue,
  ConfigDescription,
  ConfigDescriptions,
  ConfigType,
  RowData,
  ConfigChange,
};
