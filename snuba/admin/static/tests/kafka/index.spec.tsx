import TopicData from "SnubaAdmin/kafka/index";
import Client from "SnubaAdmin/api_client";
import React from "react";
import { it, expect, jest, afterEach } from "@jest/globals";
import { render, act, waitFor } from "@testing-library/react";
import { KafkaTopicData } from "SnubaAdmin/kafka/types";

it("should display data fetched from API when rendering", async () => {
  let data = [
    {
      key: "My_Key_1",
      user: "My_User_1",
      timestamp: 0,
    },
    {
      key: "My_Key_2",
      user: "My_User_2",
      timestamp: 1,
    },
  ];
  let mockClient = {
    ...Client(),
    getKafkaData: jest
      .fn<() => Promise<KafkaTopicData[]>>()
      .mockResolvedValueOnce(data),
  };

  let { getByText } = render(<TopicData api={mockClient} />);

  await waitFor(() => expect(mockClient.getKafkaData).toBeCalledTimes(1));
  expect(getByText(JSON.stringify(data))).toBeTruthy();
});
