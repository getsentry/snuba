import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import React, { useEffect, useState } from "react";

function ViewCustomJobs(props: { api: Client }) {
  const [jobSpecs, setJobSpecs] = useState<JobSpecMap>({});

  useEffect(() => {
    props.api.listJobSpecs().then((res) => {
      setJobSpecs(res);
    });
  }, []);

  function jobSpecsAsRows() {
    return Object.entries(jobSpecs).map(([_, job_info]) => {
      return [
        job_info.spec.job_id,
        job_info.spec.job_type,
        JSON.stringify(job_info.spec.params),
        job_info.status,
        "TODO",
      ];
    });
  }

  return (
    <div>
      <Table
        headerData={["ID", "Job Type", "Params", "Status", "Execute"]}
        rowData={jobSpecsAsRows()}
      />
    </div>
  );
}

export default ViewCustomJobs;
