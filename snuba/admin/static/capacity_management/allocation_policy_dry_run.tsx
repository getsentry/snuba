import React, { ChangeEvent, FormEvent, useState } from "react";

function AllocationPolicyDryRun() {
  // State to hold the values of the text fields
  const [referrer, setReferrer] = useState<string>("");
  const [project_id, setProject] = useState<number | "">("");
  const [organization_id, setOrganization] = useState<number | "">("");
  const [allocationPolicies, setAllocationPolicies] = useState<Array<any>>([]);

  // Function to handle the form submission
  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();

    setAllocationPolicies([
      {
        policyName: "test_policy",
        threads: 1,
        concurrent: 1,
      },
      {
        policyName: "test_policy_2",
        threads: 2,
        concurrent: 2,
      },
    ]);
  };

  const handleNumberChange =
    (setter: React.Dispatch<React.SetStateAction<number | "">>) =>
    (e: ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setter(value === "" ? "" : Number(value));
    };

  return (
    <>
      <h3>Allocation Policy Dry Run</h3>
      <p>
        Use the dry run mode to see what allocation policies get applied and
        what the final decision of all allocation policies combined would be.
      </p>
      <form onSubmit={handleSubmit}>
        <div>
          <label htmlFor="referrer">Referrer (Mandatory):</label>
          <input
            id="referrer"
            type="text"
            value={referrer}
            onChange={(e) => setReferrer(e.target.value)}
            required
          />
        </div>
        <div>
          <label htmlFor="project_id">Project ID (Optional):</label>
          <input
            id="project_id"
            type="number"
            value={project_id}
            onChange={handleNumberChange(setProject)}
          />
        </div>
        <div>
          <label htmlFor="organization_id">Organization ID (Optional):</label>
          <input
            id="organization_id"
            type="number"
            value={organization_id}
            onChange={handleNumberChange(setOrganization)}
          />
        </div>
        <button type="submit">Submit</button>
      </form>

      {allocationPolicies.length > 0 ? (
        <>
          <h4>Response:</h4>
          <table>
            <thead>
              <tr>
                <th style={{ border: "1px solid black" }}>Policy Name</th>
                <th style={{ border: "1px solid black" }}>Concurrent</th>
                <th style={{ border: "1px solid black" }}>Threads</th>
              </tr>
            </thead>
            <tbody>
              {allocationPolicies.map((item, index) => (
                <tr key={index}>
                  <td style={{ border: "1px solid black" }}>{item.policyName}</td>
                  <td style={{ border: "1px solid black" }}>{item.concurrent}</td>
                  <td style={{ border: "1px solid black" }}>{item.threads}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}
    </>
  );
}

export default AllocationPolicyDryRun;
