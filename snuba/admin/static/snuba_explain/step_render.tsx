import React, { ReactNode } from "react";

import { COLORS } from "../theme";
import { ExplainStep, QueryTransformData } from "./types";

import { Collapse } from "../collapse";

type StepProps = {
  step: ExplainStep;
};

function QueryTransformStep(props: StepProps) {
  const { step } = props;
  const data = step.data as QueryTransformData;
  return (
    <div>
      <Collapse key={step.name} text={step.name}>
        <span>{data.original_query}</span>
        <span>{data.new_query}</span>
      </Collapse>
    </div>
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
