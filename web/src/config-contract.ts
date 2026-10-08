import type { ConfigDefaults } from "./generated/config";
import rawDefaults from "./generated/config-defaults.json";

export const configDefaults = rawDefaults as ConfigDefaults;
