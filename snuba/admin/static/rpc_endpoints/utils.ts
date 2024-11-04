import { EndpointData } from 'SnubaAdmin/rpc_endpoints/types';

export const DEBUG_SUPPORTED_VERSIONS = ['v1'];

export async function fetchEndpointsList(
  api: any
): Promise<EndpointData[]> {
  try {
    const fetchedEndpoints: [string, string][] = await api.getRpcEndpoints();
    return fetchedEndpoints.map((endpoint: [string, string]) => ({
      name: endpoint[0],
      version: endpoint[1]
    }));
  } catch (error) {
    console.error("Error fetching endpoints:", error);
    return [];
  }
}

export async function executeEndpoint(
  api: any,
  selectedEndpoint: string | null,
  selectedVersion: string | null,
  requestBody: string,
  debugMode: boolean
): Promise<any> {
  if (!selectedEndpoint || !selectedVersion) {
    throw new Error('Endpoint and version must be selected');
  }

  try {
    const parsedBody = JSON.parse(requestBody);
    if (debugMode && DEBUG_SUPPORTED_VERSIONS.includes(selectedVersion)) {
      parsedBody.meta = parsedBody.meta || {};
      parsedBody.meta.debug = true;
    }
    return await api.executeRpcEndpoint(selectedEndpoint, selectedVersion, parsedBody);
  } catch (error: any) {
    if (error instanceof SyntaxError) {
      throw new Error('Invalid JSON format in request body');
    }
    throw new Error(`Failed to execute endpoint: ${error.message}`);
  }
}

export function getEndpointData(
  endpoints: EndpointData[],
  endpointName: string
): EndpointData | undefined {
  return endpoints.find(e => e.name === endpointName);
}
