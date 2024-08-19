import React from "react";
import {Select} from "@mantine/core";

type SelectProps = {
  value: string;
  name: string;
  onChange: (value: string) => void;
  options: string[];
}

export function getDatasetFromUrl(key: string) {
  const params = new URLSearchParams(window.location.search)
  const locationDataset = params.get(key) || undefined;
  return locationDataset;
}

export function CustomSelect(props: SelectProps) {
  const {value, onChange, options, name} = props;
  function updateLocation(value: string) {
    const url = new URL(window.location);
    url.searchParams.set(name, value);
    window.history.replaceState('', '', url);
    onChange(value);
  }
  options.sort();
  return (
      <Select
      placeholder={`Select a ${name}`}
      searchable
      selectOnBlur
      value={value ?? locationDataset}
      nothingFound={`Could not find a matching ${name}`}
      onChange={updateLocation}
      data={options}
      />
  );
}
