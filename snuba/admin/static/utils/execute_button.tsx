import React, { useEffect, useState } from "react";
import { Button } from "@mantine/core";

// Fast queries finish before this, so they don't flash a spinner.
const SPINNER_DELAY_MS = 150;

function ExecuteButton(props: {
  disabled: boolean;
  onClick: () => Promise<any>;
  onError?: (error: Error) => any;
  label?: string;
  // Mantine color, e.g. "red" for actions that need extra care.
  color?: string;
}) {
  const [isExecuting, setIsExecuting] = useState<boolean>(false);
  const [showSpinner, setShowSpinner] = useState<boolean>(false);

  useEffect(() => {
    if (!isExecuting) {
      setShowSpinner(false);
      return;
    }
    const timer = setTimeout(() => setShowSpinner(true), SPINNER_DELAY_MS);
    return () => clearTimeout(timer);
  }, [isExecuting]);

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
    // Until the spinner shows the button still looks clickable, so extra
    // clicks are ignored rather than queued.
    if (isExecuting) {
      return;
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

  // The label stays put and the spinner covers it, so the button keeps its
  // size and nothing around it moves while a query runs.
  return (
    <>
      <Button
        onClick={(evt: any) => {
          evt.preventDefault();
          executeQuery();
        }}
        color={props.color}
        loading={showSpinner}
        loaderPosition="center"
        disabled={props.disabled}
        aria-busy={isExecuting}
      >
        {label}
      </Button>
      <span role="status" aria-live="polite" style={visuallyHiddenStyle}>
        {isExecuting ? "Query running…" : ""}
      </span>
    </>
  );
}

// Read out by screen readers without taking up space.
const visuallyHiddenStyle = {
  position: "absolute" as const,
  width: 1,
  height: 1,
  overflow: "hidden",
  clip: "rect(0 0 0 0)",
  whiteSpace: "nowrap" as const,
};

export default ExecuteButton;
