import Client from "../api_client";
import React, { useEffect, useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "react-bootstrap/Modal";
import { AllocationPolicyConfig } from "./types";

function ConfigModal(props: {
  api: Client;
  currentlyEditing: boolean;
  currentConfig: AllocationPolicyConfig;
  setCurrentlyEditing: (currentlyEditing: boolean) => void;
}) {
  const { api, currentlyEditing, currentConfig, setCurrentlyEditing } = props;

  const [value, updateValue] = useState("");

  function saveChanges() {
    console.log(value);
    setCurrentlyEditing(false);
  }

  function deleteConfig() {
    if (window.confirm(`Are you sure you want to delete this config?`)) {
      console.log("Deleted " + currentConfig.key);
    }
    setCurrentlyEditing(false);
  }

  return (
    <>
      <link
        rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css"
        integrity="sha384-rbsA2VBKQhggwzxH7pPCaAqO46MgnOM80zW1RWuH61DGLwZJEdK2Kadq2F9CUG65"
        crossOrigin="anonymous"
      />
      <Modal show={currentlyEditing} onHide={() => setCurrentlyEditing(false)}>
        <Modal.Header closeButton>
          <Modal.Title>
            Editing:{" "}
            <code style={{ wordBreak: "break-all", color: "black" }}>
              {currentConfig.key}
            </code>
          </Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Form.Group>
            <Form.Label>Value: </Form.Label>
            <Form.Control
              type="text"
              onChange={(e) => updateValue(e.target.value)}
              placeholder={currentConfig.value}
            />
          </Form.Group>
        </Modal.Body>
        <Modal.Footer>
          <Button
            variant="secondary"
            onClick={() => setCurrentlyEditing(false)}
          >
            Close
          </Button>
          <Button variant="warning" onClick={deleteConfig}>
            Delete
          </Button>
          <Button variant="primary" onClick={saveChanges}>
            Save Changes
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
}

export default ConfigModal;
