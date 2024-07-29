import Client from "SnubaAdmin/api_client";
import React, { useEffect, useState } from "react"

function DeleteTool(props: { api: Client }) {
    const [storageName, setStorageName] = useState('')
    const [columnConditions, setColumnConditions] = useState('')
    const [result, setResult] = useState('')

    return (
        <div>
            <input type="text" value={storageName} placeholder="storage name" onChange={(event) => setStorageName(event.target.value)} /><br/>
            <textarea value={columnConditions} placeholder="column conditions" onChange={(event) => setColumnConditions(event.target.value)} /><br/>
            <button type="submit" onClick={
                (event) => {
                    let conds;
                    try {
                        conds = JSON.parse(columnConditions)
                    } catch (error) {
                        alert("expect columnConditions to be valid json but its not");
                        return;
                    }
                    let resp_status = ""
                    props.api.runLightweightDelete(storageName, conds).then(res => {
                        resp_status = `${res.status} ${res.statusText}\n`
                        if (res.headers.get("Content-Type") == "application/json") {
                            return res.json().then(json => JSON.stringify(json))
                        } else {
                            return res.text()
                        }
                    }).then(data_str => setResult(resp_status + data_str))
                }
            }>Submit</button>
            <p>latest result:</p><br/>
            {result}
        </div>
      );
}

export default DeleteTool
