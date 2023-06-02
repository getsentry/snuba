import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "react-bootstrap/Modal";
import {
  AllocationPolicyConfig,
  AllocationPolicyOptionalConfigDefinition,
} from "./types";
import FormGroup from "react-bootstrap/FormGroup";

function AddConfigModal(props: {
  currentlyAdding: boolean;
  setCurrentlyAdding: (currentlyAdding: boolean) => void;
  optionalConfigDefinitions: AllocationPolicyOptionalConfigDefinition[];
  saveConfig: (config: AllocationPolicyConfig) => void;
}) {
  const {
    currentlyAdding,
    setCurrentlyAdding,
    optionalConfigDefinitions,
    saveConfig,
  } = props;

  const [selectedDefinition, selectDefinition] =
    useState<AllocationPolicyOptionalConfigDefinition>();

  const [config, buildConfig] = useState<AllocationPolicyConfig>({
    name: "",
    value: "",
    description: "",
    type: "",
    params: {},
  });

  function selectConfigDefinition(name: string) {
    optionalConfigDefinitions.map((definition) => {
      if (definition.name == name) {
        selectDefinition(definition);
        buildConfig({
          name: definition.name,
          value: "",
          description: definition.description,
          type: definition.type,
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
      <Modal.Header>
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
            {optionalConfigDefinitions.map((definition) => (
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
                    data-testid={param.name}
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
                data-testid="value_field"
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
              (Object.keys(config.params).length !=
                Object.keys(selectedDefinition.params).length ||
                Object.values(config.params).includes("") ||
                config.value == ""))
          }
        >
          Save Changes
        </Button>
      </Modal.Footer>
    </Modal>
  );
}

export default AddConfigModal;
