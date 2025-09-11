import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { CustomSelect, getParamFromStorage } from "SnubaAdmin/select";

interface ConfigurableComponentRendererProps<T> {
  api: Client;
  resourceType: string;
  getOptions: () => Promise<string[]>;
  loadData: (resource: string) => Promise<T>;
  renderContent: (data: T | undefined, selectedResource: string | undefined) => React.ReactNode;
}

function ConfigurableComponentRenderer<T>({
  resourceType,
  getOptions,
  loadData,
  renderContent,
}: ConfigurableComponentRendererProps<T>) {
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

export { ConfigurableComponentRenderer };
