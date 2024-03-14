import {getTableColor} from "SnubaAdmin/capacity_management/allocation_policy";
import { COLORS } from "SnubaAdmin/theme";
import { it, expect } from "@jest/globals";

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
  let configs = [
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
    },
  ].concat(bonus_configs);
  expect(getTableColor(configs)).toBe("gray")
})


it("should be blue when the policy is active and enforced", () => {
  let configs = [
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
  ].concat(bonus_configs);
  expect(getTableColor(configs)).toBe(COLORS.SNUBA_BLUE);
})


it("should be orange when the policy is active and enforced", () => {
  let configs = [
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
  ].concat(bonus_configs);
  expect(getTableColor(configs)).toBe("orange")
})
