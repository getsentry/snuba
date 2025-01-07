import React, { useState } from 'react';
import { Accordion, Code, Button } from '@mantine/core';
import { ExampleRequestAccordionProps } from 'SnubaAdmin/rpc_endpoints/types';

export const ExampleRequestAccordion = ({
  selectedEndpoint,
  selectedVersion,
  exampleRequestTemplates,
  setRequestBody,
  classes,
}: ExampleRequestAccordionProps) => {
  const [isOpened, setIsOpened] = useState(false);

  const getRequestTemplate = () =>
    selectedEndpoint && selectedVersion
      ? exampleRequestTemplates[selectedEndpoint]?.[selectedVersion] || exampleRequestTemplates.default
      : exampleRequestTemplates.default;

  return (
    <Accordion
      classNames={{ item: classes.mainAccordion }}
      variant="filled"
      radius="sm"
      value={isOpened ? 'example' : null}
      onChange={(value) => setIsOpened(value === 'example')}
    >
      <Accordion.Item value="example">
        <Accordion.Control>Example Request Payload</Accordion.Control>
        <Accordion.Panel>
          <Code block style={{ color: 'green' }}>
            <pre>
              {JSON.stringify(getRequestTemplate(), null, 2)}
            </pre>
          </Code>
          <Button
            onClick={() => {
              setRequestBody(JSON.stringify(getRequestTemplate(), null, 2));
              setIsOpened(false);
            }}
            style={{ marginTop: '1rem' }}
          >
            Copy to Request Body
          </Button>
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  );
};
