import React from "react";
import {Select, SelectItem} from "@mantine/core";

type SelectProps = {
  value: string;
  name?: string;
  disabled?: boolean;
  onChange: (value: string) => void;
  options: string[] | SelectItem[];
}

export function getParamFromStorage(key: string) {
  const item = localStorage.getItem(`select-${key}`);
  return item ?? undefined;
}

export function CustomSelect(props: SelectProps) {
  const {value, onChange, options, name, disabled} = props;
  function updateStorage(value: string) {
    if (name) {
      localStorage.setItem(`select-${name}`, value);
    }
    onChange(value);
  }
  options.sort();
  return (
      <Select
      placeholder={`Select a ${name}`}
      searchable
      selectOnBlur
      disabled={disabled}
      value={value}
      nothingFound={`Could not find a matching ${name}`}
      onChange={updateStorage}
      data={options}
      />
  );
}
