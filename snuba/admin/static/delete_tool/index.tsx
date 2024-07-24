import Client from "SnubaAdmin/api_client";
import React, { useEffect, useState } from "react"

function DeleteTool(props: { api: Client }) {
    const [inputValue, setInputValue] = useState('')
    const [submittedVal, setSubmittedVal] = useState('')


    return (
        <div>
            <input type="text" value={inputValue} onChange={(event) => setInputValue(event.target.value)} />
            <button type="submit" onClick={(event) => setSubmittedVal(inputValue)}>Submit</button>
            <p>sent: {submittedVal}</p>
        </div>
      );
}

export default DeleteTool
