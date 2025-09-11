import { getTableColor } from "SnubaAdmin/capacity_management/allocation_policy";
import { COLORS } from "SnubaAdmin/theme";
import { it, expect } from "@jest/globals";
import { ConfigurableComponentData } from "SnubaAdmin/capacity_management/types";

let bonus_configs = [{
      name: "key1",
      value: "10",
      description: "something",
      type: "int",
      params: {},
    },
    {
      name: "key2",
      value: "20",
      description: "something params",
      type: "int",
      params: { a: "1", b: "2" },
    },
]


it("should be gray when the policy is inactive", () => {
  let configurableComponentData: ConfigurableComponentData = {
    configurable_component_namespace: "some_namespace",
    configurable_component_class_name: "some_policy",
    resource_identifier: "some_resource",
    configurations: [
      {
        name: "is_active",
        value: "0",
        description: "",
        type: "int",
        params: {},
      },
      {
        name: "is_enforced",
        value: "1",
        description: "",
        type: "int",
        params: {},
      }
    ].concat(bonus_configs),
    optional_config_definitions: [],
  };
    expect(getTableColor(configurableComponentData)).toBe(COLORS.SNUBA_BLUE);
})

it("should be blue when the policy is active and enforced", () => {
  let configurableComponentData: ConfigurableComponentData = {
    configurable_component_namespace: "some_namespace",
    configurable_component_class_name: "some_policy",
    resource_identifier: "some_resource",
    configurations: [
      {
        name: "is_active",
        value: "1",
        description: "",
        type: "int",
        params: {},
      },
      {
        name: "is_enforced",
        value: "1",
        description: "",
        type: "int",
        params: {},
      }
    ].concat(bonus_configs),
    optional_config_definitions: [],
  };
    expect(getTableColor(configurableComponentData)).toBe(COLORS.SNUBA_BLUE);
})


it("should be orange when the policy is active and enforced", () => {
  let configurableComponentData: ConfigurableComponentData = {
    configurable_component_namespace: "some_namespace",
    configurable_component_class_name: "some_policy",
    resource_identifier: "some_resource",
    configurations: [
      {
        name: "is_active",
        value: "1",
        description: "",
        type: "int",
        params: {},
      },
      {
        name: "is_enforced",
        value: "0",
        description: "",
        type: "int",
        params: {},
      }
    ].concat(bonus_configs),
    optional_config_definitions: [],
  };
    expect(getTableColor(configurableComponentData)).toBe(COLORS.SNUBA_BLUE);
})
