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
    return Object.entries(jobSpecs).map(([_, spec]) => {
      return [
        spec.job_id,
        spec.job_type,
        JSON.stringify(spec.params),
        "TODO",
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
