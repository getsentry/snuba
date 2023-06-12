import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { COLORS } from "../theme";
import { Topic } from "./types";

function DeadLetterQueue(props: { api: Client }) {
  const [dlqTopics, setDlqTopics] = useState<Topic[]>([]);
  const [topic, setTopic] = useState<Topic | null>(null);
  const [messagesToProcess, setMessagesToProcess] = useState(0);

  useEffect(() => {
    props.api.getDlqTopics().then((res) => {
      setDlqTopics(res);
    });
  }, []);

  return (
    <div>
      <div>
        <p>
          <mark>
            This is a mockup of the dead letter queue UI. It does not work yet!
          </mark>
        </p>
      </div>
      <form>
        <fieldset>
          <select
            value={topic ? topic.logicalName : ""}
            style={selectStyle}
            onChange={(evt) => {
              for (let topic of dlqTopics) {
                if (topic.logicalName === evt.target.value) {
                  setTopic(topic);
                  return;
                }
              }

              setTopic(null);
            }}
          >
            <option disabled value="">
              Select DLQ topic
            </option>
            {dlqTopics.map((topic) => (
              <option
                key={topic.logicalName}
                value={topic.logicalName}
              >{`${topic.logicalName} (slice: ${topic.slice})`}</option>
            ))}
          </select>
        </fieldset>
        {topic && (
          <fieldset>
            <div>
              Logical topic: <code>{topic.logicalName}</code>
            </div>
            <div>
              Physical topic: <code>{topic.physicalName}</code>
            </div>
            <div>
              Slice: <code>null</code>
            </div>
            <div>
              Storage: <code>{topic.storage}</code>
            </div>
            <div>
              Current offsets: <code>{`\{0: 3\, 1: 1, 2: 3}`}</code>
            </div>
            <div>
              Latest offsets: <code>{`\{0: 20\, 1: 1, 2: 3}`}</code>
            </div>
          </fieldset>
        )}
      </form>
      {topic && (
        <form style={reprocessForm}>
          <h3 style={{ margin: "0 0 10px 0" }}>Reprocess messages:</h3>
          <fieldset>
            <label htmlFor="policy" style={label}>
              DLQ policy:
            </label>
            <select id="policy" name="policy" style={selectStyle}>
              <option>Select invalid message policy</option>
              <option>Re-produce to DLQ</option>
              <option>Crash on error</option>
              <option>Drop invalid messages</option>
            </select>
          </fieldset>
          <fieldset>
            <label htmlFor="messagesToProcess" style={label}>
              Max messages to process:
            </label>
            <input
              type="number"
              name="messagesToProcess"
              value={messagesToProcess}
              placeholder="Max messages to process"
              onChange={(evt) => {
                setMessagesToProcess(parseInt(evt.target.value, 10));
              }}
            />
          </fieldset>
          <fieldset>
            <button
              type="button"
              style={buttonStyle}
              onClick={(evt) => console.log("Do reprocess")}
            >
              Reprocess messages
            </button>
          </fieldset>
        </form>
      )}
    </div>
  );
}

const selectStyle = {
  marginBottom: 8,
  height: 30,
};

const buttonStyle = {
  height: 30,
  border: 0,
  padding: "4px 20px",
};

const label = {
  display: "block",
};

const reprocessForm = {
  width: 1000,
  padding: 20,
  backgroundColor: COLORS.BG_LIGHT,
};

export default DeadLetterQueue;
