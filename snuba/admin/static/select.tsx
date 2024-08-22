import React, { forwardRef } from "react";
import { Select, SelectItem, Group, Text } from '@mantine/core';

type SelectProps = {
  value: string;
  name?: string;
  disabled?: boolean;
  onChange: (value: string) => void;
  options: string[] | SelectItem[];
}

interface ItemProps extends React.ComponentPropsWithoutRef<'div'> {
  image: string;
  label: string;
}

export function getParamFromStorage(key: string) {
  const item = localStorage.getItem(`select-${key}`);
  return item ?? undefined;
}

const SelectItem = forwardRef<HTMLDivElement, ItemProps>(
  ({ image, label, ...others }: ItemProps, ref) => (
    <div data-testid={"select-option"} ref={ref} {...others}>
      <Group noWrap>
        <div>
          <Text size="sm">{label}</Text>
        </div>
      </Group>
    </div>
  )
);

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
      itemComponent={SelectItem}
      disabled={disabled}
      value={value}
      nothingFound={`Could not find a matching ${name}`}
      onChange={updateStorage}
      data={options}
      data-testid={"select"}
      />
  );
}
