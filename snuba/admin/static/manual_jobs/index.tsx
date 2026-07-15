import Client from "SnubaAdmin/api_client";
import { Table } from "SnubaAdmin/table";
import React, { useEffect, useState } from "react";
import { useDisclosure } from '@mantine/hooks';
import { Modal, Button, Select, Textarea, Group, Stack, Text } from "@mantine/core";

function ViewCustomJobs(props: { api: Client }) {
  const [jobSpecs, setJobSpecs] = useState<JobSpecMap>({});
  const [modalVisible, { open, close }] = useDisclosure(false);
  const [modalLogs, setModalLogs] = useState<string[]>([]);
  const [modalJobId, setModalJobId] = useState<string>("");

  const [jobTypes, setJobTypes] = useState<string[]>([]);
  const [selectedJobType, setSelectedJobType] = useState<string | null>(null);
  const [paramsText, setParamsText] = useState<string>("");
  const [runResult, setRunResult] = useState<string>("");
  const [lastRunJobId, setLastRunJobId] = useState<string>("");

  useEffect(() => {
    props.api.listJobSpecs().then((res) => {
      setJobSpecs(res);
    });
    props.api.listJobTypes().then((res) => {
      setJobTypes(res);
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

  function openLogsModal(jobId: string) {
    props.api.getJobLogs(jobId).then((logs) => {
      setModalLogs(logs);
      setModalJobId(jobId);
      open();
    });
  }

  function runButtonForJobId(status: string, jobId: string) {
    if (status === "not_started") {
      return <Button onClick={() => props.api.runJob(jobId).then((newStatus: String) => updateJobStatus(jobId, newStatus.toString()))}>Run</Button>;
    }
    return <>
      <Button disabled>Not Available</Button>
      <Button onClick={() => openLogsModal(jobId)}>View Logs</Button>
    </>;
  }

  function runSelectedJobType() {
    if (!selectedJobType) {
      return;
    }
    let params: { [key: string]: any } = {};
    if (paramsText.trim() !== "") {
      try {
        params = JSON.parse(paramsText);
      } catch (e) {
        setRunResult("Invalid JSON params: " + e);
        return;
      }
    }
    setRunResult("Running " + selectedJobType + "...");
    setLastRunJobId("");
    props.api
      .runJobByType(selectedJobType, params)
      .then((res) => {
        setRunResult(`${selectedJobType} finished with status: ${res.status}`);
        setLastRunJobId(res.job_id);
      })
      .catch((err) => {
        setRunResult("Error: " + err.message);
      });
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
      <Stack style={{ marginBottom: 24 }}>
        <Text fw={700}>Run a job (no manifest entry needed)</Text>
        <Select
          label="Job type"
          placeholder="Select a job to run"
          data={jobTypes}
          value={selectedJobType}
          onChange={setSelectedJobType}
          searchable
          style={{ maxWidth: 480 }}
        />
        <Textarea
          label="Params (JSON, optional)"
          placeholder='{"key": "value"}'
          value={paramsText}
          onChange={(e) => setParamsText(e.currentTarget.value)}
          autosize
          minRows={2}
          style={{ maxWidth: 480 }}
        />
        <Group>
          <Button onClick={runSelectedJobType} disabled={!selectedJobType}>Run job</Button>
          {lastRunJobId !== "" && (
            <Button variant="outline" onClick={() => openLogsModal(lastRunJobId)}>View Logs</Button>
          )}
        </Group>
        {runResult !== "" && <Text>{runResult}</Text>}
      </Stack>
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
