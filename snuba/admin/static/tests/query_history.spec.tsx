import { it, describe } from "@jest/globals";
import { setRecentHistory, getRecentHistory } from "SnubaAdmin/query_history";

describe("Query editor", () => {
  global.ResizeObserver = require("resize-observer-polyfill");
  describe("When setting history", () => {
    it("should be retrievable from recentHistory", () => {
      const TEST_KEY = "test"
      setRecentHistory(TEST_KEY, {"something": "foo"});
      expect(getRecentHistory(TEST_KEY)).toStrictEqual([{"something": "foo"}]);
    });
    it("inserts items at index 0", () => {
      const TEST_KEY = "test1"
      for (const index of [1,2]) {
        setRecentHistory(TEST_KEY, {"something": index});
      }
      expect(getRecentHistory(TEST_KEY)).toStrictEqual([
        {"something": 2},
        {"something": 1},
      ]);
    });
    it("drops the first item once a fourth item is pushed", () => {
      const TEST_KEY = "test2"
      for (const index of [1,2,3,4]) {
        setRecentHistory(TEST_KEY, {"something": index});
      }
      expect(getRecentHistory(TEST_KEY)).toStrictEqual([
        {"something": 4},
        {"something": 3},
        {"something": 2},
      ]);
    });
    it("different keys don't conflict", () => {
      const TEST_KEY1 = "test3"
      const TEST_KEY2 = "test4"
      setRecentHistory(TEST_KEY1, {"something": "foo"});
      expect(getRecentHistory(TEST_KEY2)).toStrictEqual([]);
    });
  });
});
