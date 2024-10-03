import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import React, { useEffect, useState } from "react";
import Button from "react-bootstrap/esm/Button";

function ViewCustomJobs(props: { api: Client }) {
  const [jobSpecs, setJobSpecs] = useState<JobSpecMap>({});

  useEffect(() => {
    props.api.listJobSpecs().then((res) => {
      setJobSpecs(res);
    });
  }, []);

  function updateJobStatus(job_id: string, new_status: string): any {
    const updatedJobs = Object.fromEntries(Object.entries(jobSpecs).map(([_, job]) => {
      if (job.spec.job_id === job_id) {
        return [job_id, {
          ...job,
          status: new_status,
        }];
      }
      return [job_id, job];
    }));
    setJobSpecs(updatedJobs);
  }

  function runButtonForJobId(status: string, job_id: string) {
    if (status === "not_started") {
      return <Button onClick={() => props.api.runJob(job_id).then((new_status: String) => updateJobStatus(job_id, new_status.toString()))}>Run</Button>;
    }
    return <Button disabled>Not Available</Button>;
  }

  function jobSpecsAsRows() {
    return Object.entries(jobSpecs).map(([_, job_info]) => {
      return [
        job_info.spec.job_id,
        job_info.spec.job_type,
        JSON.stringify(job_info.spec.params),
        job_info.status,
        runButtonForJobId(job_info.status, job_info.spec.job_id),
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
