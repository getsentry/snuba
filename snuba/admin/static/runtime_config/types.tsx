type ConfigKey = string;
type ConfigValue = string;
type ConfigType = "string" | "int" | "float";

type ConfigChange = {
  key: ConfigKey;
  user: string | null;
  timestamp: number;
  before: ConfigValue | null;
  beforeType: ConfigType | null;
  after: ConfigValue | null;
  afterType: ConfigType | null;
};

export { ConfigValue, ConfigType, ConfigChange };
