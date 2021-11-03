import { ReactNode } from "react";

type ConfigKey = string;
type ConfigValue = string | number;
type ConfigType = "string" | "int" | "float";

type RowData = [ReactNode, ReactNode, ReactNode, ReactNode];

export { ConfigKey, ConfigValue, ConfigType, RowData };
