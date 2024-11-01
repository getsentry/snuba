import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import React, { useEffect, useState } from "react";
import { useDisclosure } from '@mantine/hooks';
import { Modal, Button } from "@mantine/core";

function ViewCustomJobs(props: { api: Client }) {
  const [jobSpecs, setJobSpecs] = useState<JobSpecMap>({});
  const [modalVisible, { open, close }] = useDisclosure(false);
  const [modalLogs, setModalLogs] = useState<string[]>([]);
  const [modalJobId, setModalJobId] = useState<string>("");

  useEffect(() => {
    props.api.listJobSpecs().then((res) => {
      setJobSpecs(res);
    });
  }, []);

  function updateJobStatus(jobId: string, new_status: string): any {
    const updatedJobs = Object.fromEntries(Object.entries(jobSpecs).map(([mapJobId, job]) => {
      if (job.spec.job_id === jobId) {
        return [mapJobId, {
          ...job,
          status: new_status,
        }];
      }
      return [mapJobId, job];
    }));
    setJobSpecs(updatedJobs);
  }

  function runButtonForJobId(status: string, jobId: string) {
    if (status === "not_started") {
      return <Button onClick={() => props.api.runJob(jobId).then((newStatus: String) => updateJobStatus(jobId, newStatus.toString()))}>Run</Button>;
    }
    return <>
      <Button disabled>Not Available</Button>
      <Button onClick={() => {
        props.api.getJobLogs(jobId).then((logs) => {
          setModalLogs(logs);
          setModalJobId(jobId);
          open();
        });
      }}>View Logs</Button>
    </>;
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

  function modalTitle() {
    return "Job Logs for " + modalJobId;
  }

  return (
    <>
      <Modal opened={modalVisible}
        onClose={close}
        title={modalTitle()}
        size="100%"
        styles={{ title: { fontWeight: "bold" } }}
      >
        {modalLogs.map((log, i) => <><pre key={i} style={{ margin: 0 }}>{log}</pre></>)}
      </Modal >
      <div>
        <Table
          headerData={["ID", "Job Type", "Params", "Status", "Execute"]}
          rowData={jobSpecsAsRows()}
        />
      </div>
    </>
  );
}

export default ViewCustomJobs;
