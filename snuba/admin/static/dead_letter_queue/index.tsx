import React, { useEffect, useState } from "react";
import Client from "../api_client";
import { COLORS } from "../theme";
import { Policy, ReplayInstruction, Topic } from "./types";

function DeadLetterQueue(props: { api: Client }) {
  // Undefined means not loaded yet, null means no instruction was set
  const [instruction, setInstruction] = useState<
    ReplayInstruction | null | undefined
  >(undefined);
  const [isEditing, setIsEditing] = useState(false);
  const [dlqTopics, setDlqTopics] = useState<Topic[]>([]);
  const [topic, setTopic] = useState<Topic | null>(null);
  const [messagesToProcess, setMessagesToProcess] = useState(1);
  const [policy, setPolicy] = useState<Policy | null>(null);

  useEffect(() => {
    props.api.getDlqInstruction().then((res) => {
      setInstruction(res);
    });
    props.api.getDlqTopics().then((res) => {
      setDlqTopics(res);
    });
  }, []);

  function clearInstruction() {
    props.api.clearDlqInstruction().then((res) => {
      setInstruction(res);
    });
  }

  function replayDlq() {
    if (policy === null || topic === null) {
      return;
    }

    let instruction: ReplayInstruction = {
      messagesToProcess,
      policy,
    };

    props.api.setDlqInstruction(topic, instruction).then((res) => {
      setInstruction(res);
      setIsEditing(false);
      setTopic(null);
      setMessagesToProcess(0);
      setPolicy(null);
    });
  }

  if (typeof instruction === undefined) {
    return null;
  }

  return (
    <div>
      <div style={currentValue}>
        <p style={paragraphStyle}>This is the currently set DLQ instruction:</p>
        <p>
          <code>{JSON.stringify(instruction) || "None set"}</code>
        </p>
      </div>
      {!isEditing && (
        <div>
          <a
            style={linkStyle}
            onClick={() => {
              setIsEditing(true);
            }}
          >
            edit
          </a>
        </div>
      )}
      {isEditing && (
        <div style={{ margin: "30px 0 0 0" }}>
          <h3 style={{ margin: "0 0 10px 0" }}>Editing</h3>
          {instruction && (
            <a
              style={{ ...linkStyle, color: "red" }}
              onClick={clearInstruction}
            >
              clear instruction
            </a>
          )}
          {!instruction && (
            <form style={formStyle}>
              <fieldset>
                <label htmlFor="topic" style={label}>
                  Storage/topic:
                </label>
                <select
                  id="topic"
                  name="topic"
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
                      key={`${topic.storage}-${topic.logicalName}-${topic.slice}`}
                      value={topic.logicalName}
                    >{`${topic.storage} - ${topic.logicalName} (slice: ${topic.slice})`}</option>
                  ))}
                </select>
              </fieldset>
              <fieldset>
                <label htmlFor="policy" style={label}>
                  DLQ policy:
                </label>
                <select
                  id="policy"
                  name="policy"
                  style={selectStyle}
                  onChange={(evt) => {
                    let value = (evt.target.value as Policy) || null;
                    setPolicy(value);
                  }}
                >
                  <option value="">Select invalid message policy</option>
                  <option value="reinsert-dlq">
                    Re-insert to DLQ some long description
                  </option>
                  <option value="stop-on-error">Crash on error</option>
                  <option value="drop-invalid-messages">
                    Drop invalid messages
                  </option>
                </select>
              </fieldset>
              <fieldset>
                <label htmlFor="messagesToProcess" style={label}>
                  Max messages to process:
                </label>
                <input
                  type="number"
                  id="messagesToProcess"
                  name="messagesToProcess"
                  value={messagesToProcess}
                  placeholder="Max messages to process"
                  onChange={(evt) => {
                    let value = parseInt(evt.target.value, 10);
                    if (!isNaN(value) && value >= 0) {
                      setMessagesToProcess(value);
                    }
                  }}
                />
              </fieldset>
              <fieldset>
                <button
                  type="button"
                  style={buttonStyle}
                  onClick={replayDlq}
                  disabled={policy === null || topic === null}
                >
                  Reprocess messages
                </button>
              </fieldset>
            </form>
          )}
          <div>
            <a
              style={linkStyle}
              onClick={() => {
                setIsEditing(false);
              }}
            >
              cancel
            </a>
          </div>
        </div>
      )}
    </div>
  );
}

const currentValue = {
  display: "block",
  width: "100%",
  border: `1px solid ${COLORS.TABLE_BORDER}`,
  padding: 10,
};

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

const paragraphStyle = {
  fontSize: 15,
  color: COLORS.TEXT_LIGHTER,
};

const linkStyle = {
  cursor: "pointer",
  fontSize: 13,
  color: COLORS.TEXT_LIGHTER,
  textDecoration: "underline",
};

const formStyle = {
  padding: "20px 0",
};

export default DeadLetterQueue;
