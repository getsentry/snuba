import React, { useState, useEffect } from "react";
import { KafkaTopicData } from "./types";

import Client from "../api_client";

function TopicData(props: { api: Client }) {
  const [data, setData] = useState<KafkaTopicData[] | null>(null);

  useEffect(() => {
    props.api.getKafkaData().then((res) => {
      setData(res);
    });
  }, []);

  return <div>{JSON.stringify(data)}</div>;
}

export default TopicData;
