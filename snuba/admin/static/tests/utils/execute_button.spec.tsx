import Nav from "SnubaAdmin/nav";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest, afterEach } from "@jest/globals";
import {
  render,
  act,
  waitFor,
  screen,
  fireEvent,
} from "@testing-library/react";
import { AllowedTools } from "SnubaAdmin/types";
import ExecuteButton from "SnubaAdmin/utils/execute_button";

it("should call onClick", async () => {
  let mockCall = jest.fn<() => Promise<any>>().mockResolvedValueOnce({});

  render(<ExecuteButton onClick={mockCall} disabled={false} />);

  const button = screen.getByRole("button");
  fireEvent.click(button);

  await waitFor(() => expect(mockCall).toBeCalledTimes(1));
});

it("should not call if disabled", async () => {
  let mockCall = jest.fn(
    () => new Promise((resolve) => setTimeout(resolve, 1000))
  );

  render(<ExecuteButton onClick={mockCall} disabled={true} />);

  const button = screen.getByRole("button");
  fireEvent.click(button);

  await waitFor(() => expect(mockCall).toBeCalledTimes(0));
});

it("should not call if loading", async () => {
  let mockCall = jest.fn(
    () => new Promise((resolve) => setTimeout(resolve, 1000))
  );

  render(<ExecuteButton onClick={mockCall} disabled={false} />);

  const button = screen.getByRole("button");
  fireEvent.click(button);
  fireEvent.click(button);

  await waitFor(() => expect(mockCall).toBeCalledTimes(1));
});
