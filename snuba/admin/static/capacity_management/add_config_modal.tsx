import Client from "../api_client";
import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "react-bootstrap/Modal";
import {
  AllocationPolicyConfig,
  AllocationPolicyParametrizedConfigDefinition,
} from "./types";

function AddConfigModal(props: {
  api: Client;
  currentlyAdding: boolean;
  setCurrentlyAdding: (currentlyAdding: boolean) => void;
  parameterizedConfigDefinitions: AllocationPolicyParametrizedConfigDefinition[];
  saveConfig: (config: AllocationPolicyConfig) => void;
}) {
  const {
    api,
    currentlyAdding,
    setCurrentlyAdding,
    parameterizedConfigDefinitions,
    saveConfig,
  } = props;

  const [selectedDefinition, selectDefinition] =
    useState<AllocationPolicyParametrizedConfigDefinition>();

  const [config, buildConfig] = useState<AllocationPolicyConfig>({
    key: "",
    value: "",
    description: "",
    type: "",
    params: {},
  });

  function selectConfigDefinition(name: string) {
    parameterizedConfigDefinitions.map((definition) => {
      if (definition.name == name) {
        selectDefinition(definition);
        buildConfig({
          key: definition.name,
          value: "",
          description: "",
          type: "",
          params: {},
        });
      }
    });
  }

  function saveChanges() {
    setCurrentlyAdding(false);
    selectDefinition(undefined);
    saveConfig(config);
  }

  function updateParam(name: string, value: string) {
    buildConfig((prev) => {
      return { ...prev, params: { ...prev.params, [name]: value } };
    });
  }

  return (
    <Modal show={currentlyAdding} onHide={() => setCurrentlyAdding(false)}>
      <Modal.Header closeButton>
        <Modal.Title>Adding a new config:</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <Form.Group>
          <Form.Label>Key: </Form.Label>
          <Form.Select
            aria-label="Default select example"
            onChange={(e) => selectConfigDefinition(e.target.value)}
          >
            <option key="default_selected" className="d-none" value="">
              Select Option
            </option>
            {parameterizedConfigDefinitions.map((definition) => (
              <option key={definition.name}>{definition.name}</option>
            ))}
          </Form.Select>
        </Form.Group>

        <br />

        {selectedDefinition ? (
          <Form.Group>
            {selectedDefinition.params.map((param) => (
              <>
                <Form.Label key={param.name + "_label"}>
                  {param.name + " (" + param.type + ")"}
                </Form.Label>
                <Form.Control
                  key={param.name + "_input"}
                  type={
                    param.type == "int" || param.type == "float"
                      ? "number"
                      : "text"
                  }
                  onChange={(e) => updateParam(param.name, e.target.value)}
                />
              </>
            ))}
          </Form.Group>
        ) : null}
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={() => setCurrentlyAdding(false)}>
          Close
        </Button>
        <Button variant="primary" onClick={saveChanges}>
          Save Changes
        </Button>
      </Modal.Footer>
    </Modal>
  );
}

export default AddConfigModal;
