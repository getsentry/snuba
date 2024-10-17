import React, { useState } from "react";
import { Button } from "@mantine/core";

function ExecuteButton(props: {
  disabled: boolean;
  onClick: () => Promise<any>;
  onError?: (error: any) => any;
  label?: string;
}) {
  const [isExecuting, setIsExecuting] = useState<boolean>(false);

  let label = props.label || "Execute Query";

  const defaultError = (err: any) => {
    console.log("ERROR ", err);
    let errmsg = err.toString();
    if (typeof err === 'object' &&
      err !== null &&
      err.hasOwnProperty("error") &&
      err.error.hasOwnProperty("message")) {
      errmsg = err.error.message;
    }
    window.alert("An error occurred: " + errmsg)

  };
  let errorCallback = props.onError || defaultError;

  function executeQuery() {
    if (isExecuting) {
      window.alert("A query is already running");
    }
    setIsExecuting(true);
    props
      .onClick()
      .catch((err: any) => {
        errorCallback(err);
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
