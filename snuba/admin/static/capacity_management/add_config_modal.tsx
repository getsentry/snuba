import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "react-bootstrap/Modal";
import {
  AllocationPolicyConfig,
  AllocationPolicyParameterizedConfigDefinition,
} from "./types";
import FormGroup from "react-bootstrap/FormGroup";

function AddConfigModal(props: {
  currentlyAdding: boolean;
  setCurrentlyAdding: (currentlyAdding: boolean) => void;
  parameterizedConfigDefinitions: AllocationPolicyParameterizedConfigDefinition[];
  saveConfig: (config: AllocationPolicyConfig) => void;
}) {
  const {
    currentlyAdding,
    setCurrentlyAdding,
    parameterizedConfigDefinitions,
    saveConfig,
  } = props;

  const [selectedDefinition, selectDefinition] =
    useState<AllocationPolicyParameterizedConfigDefinition>();

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

  function setValue(value: string) {
    buildConfig((prev) => {
      return { ...prev, value: value };
    });
  }

  function cancelAdding() {
    selectDefinition(undefined);
    setCurrentlyAdding(false);
  }

  function inputType(type: string) {
    return type == "int" || type == "float" ? "number" : "text";
  }

  return (
    <Modal show={currentlyAdding} onHide={cancelAdding}>
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
          <>
            Description:
            <p>{selectedDefinition.description}</p>
            Params:
            <Form.Group>
              {selectedDefinition.params.map((param) => (
                <div key={param.name + "_input"}>
                  <Form.Label>
                    {param.name + " (" + param.type + ")"}
                  </Form.Label>
                  <Form.Control
                    type={inputType(param.type)}
                    onChange={(e) => updateParam(param.name, e.target.value)}
                  />
                </div>
              ))}
            </Form.Group>
            <br />
            <FormGroup>
              <Form.Label>Value: </Form.Label>
              <Form.Control
                type={inputType(selectedDefinition.type)}
                onChange={(e) => setValue(e.target.value)}
                placeholder={selectedDefinition.default}
              />
            </FormGroup>
          </>
        ) : null}
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={cancelAdding}>
          Close
        </Button>
        <Button
          variant="primary"
          onClick={saveChanges}
          disabled={
            selectedDefinition == undefined ||
            (selectedDefinition &&
              Object.keys(config.params).length !=
                Object.keys(selectedDefinition.params).length)
          }
        >
          Save Changes
        </Button>
      </Modal.Footer>
    </Modal>
  );
}

export default AddConfigModal;
