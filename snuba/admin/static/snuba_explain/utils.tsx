import React from "react";

function TextArea(props: {
  value: string;
  onChange: (nextValue: string) => void;
}) {
  const { value, onChange } = props;
  return (
    <textarea
      spellCheck={false}
      value={value}
      onChange={(evt) => onChange(evt.target.value)}
      style={{ width: "100%", height: 100 }}
      placeholder={"Write your query here"}
    />
  );
}

function copyText(text: string) {
  window.navigator.clipboard.writeText(text);
}

export { TextArea, copyText };
