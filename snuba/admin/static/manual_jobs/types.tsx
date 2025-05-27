type JobSpec = {
  job_id: string;
  job_type: string;
  params: { [key: string]: string };
};

type JobSpecMap = {
  [key: string]: {
    spec: JobSpec;
    status: string;
  };
};
