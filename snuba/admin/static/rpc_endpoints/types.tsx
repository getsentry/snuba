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

export type { QueryInfo, QueryStats, QueryMetadata, TimingMarks, EndpointData, ExampleRequestAccordionProps };
