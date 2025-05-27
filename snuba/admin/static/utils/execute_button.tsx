import React, { useState } from "react";
import { Button } from "@mantine/core";

function ExecuteButton(props: {
  disabled: boolean;
  onClick: () => Promise<any>;
  onError?: (error: Error) => any;
  label?: string;
}) {
  const [isExecuting, setIsExecuting] = useState<boolean>(false);

  let label = props.label || "Execute Query";

  const defaultError = (err: Error) => {
    window.alert("An error occurred: " + err.message)
  };
  let errorCallback = props.onError || defaultError;

  function _convertAnyToError(err: any): Error {
    if (err instanceof Error) {
      return err;
    } else if (typeof err === 'object' && Object.keys(err).length == 1) {
      return _convertAnyToError(err[Object.keys(err)[0]]);
    } else if (typeof err === 'object') {
      return new Error(JSON.stringify(err));
    } else {
      return new Error(err.toString());
    }
  }

  function executeQuery() {
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props
      .onClick()
      .catch((err: any) => {
        console.error("Error: ", err);
        // convert error to Error object for easier handling
        errorCallback(_convertAnyToError((err)));
      })
      .finally(() => {
        setIsExecuting(false);
      });
  }

  return (
    <div>
      <Button
        onClick={(evt: any) => {
          evt.preventDefault();
          executeQuery();
        }}
        loading={isExecuting}
        disabled={isExecuting || props.disabled}
      >
        {label}
      </Button>
    </div>
  );
}

export default ExecuteButton;
