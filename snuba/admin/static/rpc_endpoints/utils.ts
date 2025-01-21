import { EndpointData, HostProfileEvents, ProfileEventsResults, TracingSummary } from 'SnubaAdmin/rpc_endpoints/types';
import Client from 'SnubaAdmin/api_client';

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
  debugMode: boolean,
  signal?: AbortSignal
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
    return await api.executeRpcEndpoint(selectedEndpoint, selectedVersion, parsedBody, signal);
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

type ExecuteResponse = {
  meta?: {
    queryInfo?: Array<{
      traceLogs?: string;
    }>;
  };
};

export const processTraceResults = async (
  result: ExecuteResponse,
  api: Client,
  setProfileEvents: (events: HostProfileEvents[] | null) => void,
  setSummarizedTraceOutput: (output: TracingSummary | null) => void,
  signal?: AbortSignal
) => {
  if (result.meta?.queryInfo?.[0]?.traceLogs) {
    const traceResult = await api.summarizeTraceWithProfile(
      result.meta.queryInfo[0].traceLogs,
      "eap_spans",
      signal
    );

    if (signal?.aborted) {
      return;
    }

    if (traceResult?.profile_events_results) {
      const hostProfiles = Object.entries(traceResult.profile_events_results as ProfileEventsResults)
        .map(([hostName, profileData]) => {
          if (profileData.rows[0]) {
            const parsedEvents = JSON.parse(profileData.rows[0]);
            const events = Object.entries(parsedEvents).map(([name, count]) => ({
              name,
              count: count as number,
            }));
            return { hostName, events };
          }
          return { hostName, events: [] };
        });
      setProfileEvents(hostProfiles);
    }

    if (traceResult?.summarized_trace_output) {
      setSummarizedTraceOutput(traceResult.summarized_trace_output);
    }
  }
};
