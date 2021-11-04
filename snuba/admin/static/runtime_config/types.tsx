import { ReactNode } from "react";

type ConfigKey = string;
type ConfigValue = string | number;
type ConfigType = "string" | "int" | "float";

type Config = { key: ConfigKey; value: ConfigValue; type: ConfigType };

type RowData = [ReactNode, ReactNode, ReactNode];

export { Config, ConfigKey, ConfigValue, ConfigType, RowData };
