import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "react-bootstrap/Modal";
import { AllocationPolicyConfig } from "./types";

function EditConfigModal(props: {
  currentlyEditing: boolean;
  currentConfig: AllocationPolicyConfig;
  setCurrentlyEditing: (currentlyEditing: boolean) => void;
  deleteConfig: (config: AllocationPolicyConfig) => void;
  saveConfig: (config: AllocationPolicyConfig) => void;
}) {
  const {
    currentlyEditing,
    currentConfig,
    setCurrentlyEditing,
    deleteConfig,
    saveConfig,
  } = props;

  const [value, updateValue] = useState("");

  function saveChanges() {
    currentConfig.value = value;
    saveConfig(currentConfig);
    setCurrentlyEditing(false);
  }

  function confirmDeleteConfig() {
    if (
      window.confirm(
        "Are you sure you want to " +
          deleteOrReset().toLowerCase() +
          " this config?"
      )
    ) {
      deleteConfig(currentConfig);
    }
    setCurrentlyEditing(false);
  }

  function deleteOrReset() {
    return Object.keys(currentConfig.params).length ? "Delete" : "Reset";
  }

  function inputType(type: string) {
    return type == "int" || type == "float" ? "number" : "text";
  }

  return (
    <Modal show={currentlyEditing} onHide={() => setCurrentlyEditing(false)}>
      <Modal.Header>
        <Modal.Title>
          Editing:{" "}
          <code style={{ wordBreak: "break-all", color: "black" }}>
            {currentConfig.name}
          </code>
        </Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <Form.Group>
          <Form.Label>Value: </Form.Label>
          <Form.Control
            type={inputType(currentConfig.type)}
            onChange={(e) => updateValue(e.target.value)}
            placeholder={currentConfig.value}
          />
        </Form.Group>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={() => setCurrentlyEditing(false)}>
          Close
        </Button>
        <Button
          variant={deleteOrReset() == "Reset" ? "warning" : "danger"}
          onClick={confirmDeleteConfig}
        >
          {deleteOrReset()}
        </Button>
        <Button variant="primary" onClick={saveChanges} disabled={value == ""}>
          Save Changes
        </Button>
      </Modal.Footer>
    </Modal>
  );
}

export default EditConfigModal;
