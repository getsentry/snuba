import React, { createContext, forwardRef, useContext, useState } from "react";
import { Select, SelectItem, Group, Mark, Text } from '@mantine/core';

type SelectProps = {
  value: string;
  name?: string;
  disabled?: boolean;
  onChange: (value: string) => void;
  options: string[] | SelectItem[];
  filter?: (search: string, item: SelectItem) => boolean;
  // The indexes of the label's characters that match the search, to mark
  // them in the dropdown, or null if none do.
  highlight?: (search: string, value: string) => number[] | null;
  // The options in the order to list them for the search.
  sortOptions?: (search: string, options: SelectItem[]) => SelectItem[];
}

interface ItemProps extends React.ComponentPropsWithoutRef<'div'> {
  image: string;
  label: string;
  value: string;
  description?: string;
}

// The search's highlight for an item, by value. The dropdown renders items
// itself, so this reaches them through context.
const HighlightContext = createContext<
  ((value: string) => number[] | null) | null
>(null);

function highlightLabel(label: string, indexes: number[] | null) {
  if (!indexes || indexes.length === 0) {
    return label;
  }
  const marked = new Set(indexes);
  // Consecutive matched characters share one mark.
  const runs: Array<{ text: string; marked: boolean }> = [];
  Array.from(label).forEach((char, idx) => {
    const last = runs[runs.length - 1];
    if (last && last.marked === marked.has(idx)) {
      last.text += char;
    } else {
      runs.push({ text: char, marked: marked.has(idx) });
    }
  });
  return runs.map((run, idx) =>
    run.marked ? <Mark key={idx}>{run.text}</Mark> : run.text
  );
}

export function getParamFromStorage(key: string) {
  const item = localStorage.getItem(`select-${key}`);
  return item ?? undefined;
}

const SelectItem = forwardRef<HTMLDivElement, ItemProps>(
  ({ image, label, value, description, ...others }: ItemProps, ref) => {
    const highlight = useContext(HighlightContext);
    return (
      <div data-testid={"select-option"} ref={ref} {...others}>
        <Group noWrap>
          <div>
            <Text size="sm">
              {highlightLabel(label, highlight ? highlight(value) : null)}
            </Text>
            {description && (
              <Text size="xs" opacity={0.65}>
                {description}
              </Text>
            )}
          </div>
        </Group>
      </div>
    );
  }
);

export function CustomSelect(props: SelectProps) {
  const {value, onChange, name, disabled, filter, highlight, sortOptions} =
    props;
  const [search, setSearch] = useState("");
  function updateStorage(value: string) {
    if (name) {
      localStorage.setItem(`select-${name}`, value);
    }
    onChange(value);
  }
  // With the selected item's label in the box, the dropdown lists everything,
  // so there's nothing to mark or rank by.
  const selected = (props.options as Array<string | SelectItem>).find(
    (option) => (typeof option === "string" ? option : option.value) === value
  );
  const selectedLabel =
    typeof selected === "string" ? selected : selected?.label ?? selected?.value;
  const activeSearch = search && search !== selectedLabel ? search : "";
  const highlightValue =
    highlight && activeSearch
      ? (itemValue: string) => highlight(activeSearch, itemValue)
      : null;
  let options = props.options;
  if (sortOptions) {
    options = sortOptions(activeSearch, options as SelectItem[]);
  } else {
    options.sort();
  }
  return (
    <HighlightContext.Provider value={highlightValue}>
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
      filter={filter}
      onSearchChange={setSearch}
      data-testid={"select"}
      />
    </HighlightContext.Provider>
  );
}
