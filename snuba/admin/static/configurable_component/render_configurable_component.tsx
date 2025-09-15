import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";

interface ConfigurableComponentDropdownRendererProps<T> {
  api: Client;
  /** All configurable components have a resource type. For example, an allocation policy can have a resource type of "storage". */
  resourceType: string;
  /** Function that returns available resource options. For example, all existing Snuba storages (resources) that have allocation policies (configurable component). This is used for the dropdown menu. */
  getOptions: () => Promise<string[]>;
  /** Function that loads the data for the selected resource. For example, all allocation policies for the selected storage. */
  loadData: (resource: string) => Promise<T>;
  /** Function that renders the content for the selected resource. For example, a table of allocation policies and their configurations for the selected storage. It is encouraged to use ConfigurableComponentConfigurations to render the content.*/
  renderContent: (data: T | undefined, selectedResource: string | undefined) => React.ReactNode;
}

/**
 * A reusable React component for rendering configurable components with resource selection and data loading.
 *
 * This component provides a common pattern for:
 * - Loading a list of available resources (e.g., storages, strategies) in a dropdown
 * - Allowing users to select a resource from the dropdown
 * - Loading and displaying configurable component data associated with the selected resource
 * - Persisting the user's selection in browser storage
 * See CapacityManagement and CBRS for examples of how to use this component.
 */

function ConfigurableComponentDropdownRenderer<T>({
  resourceType,
  getOptions,
  loadData,
  renderContent,
}: ConfigurableComponentDropdownRendererProps<T>) {
  const [options, setOptions] = useState<string[]>([]);
  const [selectedResource, setSelectedResource] = useState<string | undefined>();
  const [data, setData] = useState<T | undefined>();

  useEffect(() => {
    getOptions().then((res) => {
      setOptions(res);
      const previousResource = getParamFromStorage(resourceType);
      if (previousResource) {
        selectResource(previousResource);
      }
    });
  }, []);

  function selectResource(resource: string) {
    setSelectedResource(resource);
    loadData(resource)
      .then(setData)
      .catch((err) => {
        window.alert(err);
      });
  }

  return (
    <div>
      <p>
        {resourceType}:
        <CustomSelect
          value={selectedResource || ""}
          onChange={selectResource}
          name={resourceType}
          options={options}
        />
      </p>
      {renderContent(data, selectedResource)}
    </div>
  );
}

export { ConfigurableComponentDropdownRenderer };
