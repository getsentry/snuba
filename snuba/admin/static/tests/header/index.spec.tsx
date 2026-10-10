import Header from "SnubaAdmin/header";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest } from "@jest/globals";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { AdminRegion, AllowedTools } from "SnubaAdmin/types";

function mockClient(regions: AdminRegion[]) {
  return {
    ...Client(),
    getAllowedTools: jest
      .fn<() => Promise<AllowedTools>>()
      .mockResolvedValue({ tools: ["all"] }),
    getAdminRegions: jest
      .fn<() => Promise<AdminRegion[]>>()
      .mockResolvedValue(regions),
  };
}

const REGIONS = [
  { name: "us", url: "https://snuba-admin.getsentry.net/", is_main: true },
  { name: "de", url: "https://snuba-admin.de.getsentry.net/", is_main: false },
];

it("should link each region to the current page", async () => {
  const api = mockClient(REGIONS);
  render(<Header api={api} active="querylog" />);

  const pill = await screen.findByText("localhost ▾", { exact: false });
  fireEvent.mouseEnter(pill);

  expect(screen.getByText("SaaS (us)").getAttribute("href")).toBe(
    "https://snuba-admin.getsentry.net/#querylog"
  );
  expect(screen.getByText("de").getAttribute("href")).toBe(
    "https://snuba-admin.de.getsentry.net/#querylog"
  );
});

it("should not offer a region menu when no regions are known", async () => {
  const api = mockClient([]);
  render(<Header api={api} active="querylog" />);

  await waitFor(() => expect(api.getAdminRegions).toBeCalledTimes(1));
  const pill = screen.getByText("localhost");
  expect(pill.textContent).toBe("localhost");
  fireEvent.mouseEnter(pill);
  expect(screen.queryByText("SaaS (us)")).toBeNull();
});

it("should switch pages from the page title menu", async () => {
  const api = mockClient(REGIONS);
  render(<Header api={api} active="querylog" />);

  fireEvent.mouseEnter(screen.getByText("ClickHouse Querylog", { exact: false }));

  const current = await screen.findByRole("link", { current: "page" });
  expect(current.getAttribute("href")).toBe("#querylog");
  expect(screen.getByText("System Queries").closest("a")?.getAttribute("href")).toBe(
    "#system-queries"
  );
});

it("should not show a page title on the home page", async () => {
  const api = mockClient(REGIONS);
  render(<Header api={api} active="overview" />);

  await waitFor(() => expect(api.getAdminRegions).toBeCalledTimes(1));
  expect(screen.queryByText("/")).toBeNull();
});
