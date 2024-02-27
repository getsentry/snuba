import React, { ChangeEvent, FormEvent, useState } from "react";

function AllocationPolicyDryRun() {
  // State to hold the values of the text fields
  const [referrer, setReferrer] = useState<string>("");
  const [project_id, setProject] = useState<number | "">("");
  const [organization_id, setOrganization] = useState<number | "">("");

  // Function to handle the form submission
  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    console.log("Form submitted with the following values:");
    console.log("Input 1:", referrer);
    console.log("Input 2:", project_id);
    console.log("Input 3:", organization_id);
    // Here, you can also perform actions like sending data to a server
  };


  const handleNumberChange = (setter: React.Dispatch<React.SetStateAction<number | ''>>) => (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setter(value === '' ? '' : Number(value));
  };

  return (
    <>
        <h3>Allocation Policy Dry Run</h3>
        <p> Use the dry run mode to see what allocation policies get applied and what the final decision
            on the allocation policy would be.
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
    </>
  );
}

export default AllocationPolicyDryRun;
