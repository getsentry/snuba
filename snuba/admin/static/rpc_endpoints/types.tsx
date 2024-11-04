type TimingMarks = {
    durationMs?: number;
    marksMs?: Record<string, number>;
    tags?: Record<string, string>;
    timestamp?: number;
};

type QueryStats = {
    rowsRead?: number;
    columnsRead?: number;
    blocks?: number;
    progressBytes?: number;
    maxThreads?: number;
    timingMarks?: TimingMarks;
};

type QueryMetadata = {
    sql?: string;
    status?: string;
    clickhouseTable?: string;
    final?: boolean;
    queryId?: string;
    consistent?: boolean;
    cacheHit?: boolean;
    clusterName?: string;
};

type QueryInfo = {
    stats: QueryStats;
    metadata: QueryMetadata;
    traceLogs: string;
};

interface EndpointData {
    name: string;
    version: string;
}

interface ExampleRequestAccordionProps {
    selectedEndpoint: string | null;
    selectedVersion: string | null;
    exampleRequestTemplates: Record<string, Record<string, any>>;
    setRequestBody: (value: string) => void;
    classes: Record<string, string>;
}

interface TraceLogProps {
    log: string;
    width?: number;
}

interface StyledSpanProps {
    color: string;
    children: React.ReactNode;
    className?: string;
    style?: React.CSSProperties;
}

type ExecuteSummary = {
    rows_read: number;
    memory_size: string;
    seconds: number;
    rows_per_second: number;
    bytes_per_second: string;
};

type IndexSummary = {
    table_name: string;
    index_name: string;
    dropped_granules: number;
    total_granules: number;
};

type QuerySummary = {
    node_name: string;
    is_distributed: boolean;
    execute_summaries?: ExecuteSummary[];
    index_summaries?: IndexSummary[];
};

type TracingSummary = {
    query_summaries: Record<string, QuerySummary>;
};

type ProfileEvent = {
    name: string;
    count: number;
};

type HostProfileEvents = {
    hostName: string;
    events: ProfileEvent[];
};

type ProfileData = {
    column_names: string[];
    rows: string[];
};

type ProfileEventsResults = {
    [hostName: string]: ProfileData;
};

export type { QueryInfo, QueryStats, QueryMetadata, TimingMarks, EndpointData, ExampleRequestAccordionProps, TraceLogProps, StyledSpanProps, ExecuteSummary, IndexSummary, QuerySummary, TracingSummary, ProfileEvent, HostProfileEvents, ProfileData, ProfileEventsResults };
