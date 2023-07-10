import React, { ReactNode } from "react";

import { Prism } from "@mantine/prism";
import { ExplainStep, QueryTransformData } from "./types";
import { nonCollapsibleStyle } from "./styles";

import { Collapse } from "../collapse";

type StepProps = {
  step: ExplainStep;
};

function QueryTransformStep(props: StepProps) {
  const { step } = props;
  const data = step.data as QueryTransformData;
  if (data.original == data.transformed) {
    return (
      <div style={nonCollapsibleStyle}>
        <span>{step.name} (no change)</span>
      </div>
    );
  }
  const code_diff = <Prism language="sql">{data.diff.join("\n")}</Prism>;

  return (
    <Collapse key={step.name} text={step.name}>
      {code_diff}
    </Collapse>
  );
}

function Step(props: StepProps) {
  const { step } = props;
  if (step.type === "query_transform") {
    return <QueryTransformStep step={step} />;
  } else {
    return <div />;
  }
}

export { Step };
