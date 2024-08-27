import Client from "SnubaAdmin/api_client";
import React, { useEffect, useState } from "react"

function DeleteTool(props: { api: Client }) {
    const [storageName, setStorageName] = useState('')
    const [columnConditions, setColumnConditions] = useState('')
    const [result, setResult] = useState('')
    const [showHelpMessage, setShowHelpMessage] = useState(false)
    const [isDeletesEnabled, setIsDeletesEnabled] = useState(false)

    function getHelpMessage() {
        if (showHelpMessage) {
            return <div style={{"backgroundColor":"#DDD"}}>
                <h3><u>Inputs:</u></h3>
                <p><u>Storage name</u> - the name of the storage you want to delete from.<br/>
                <u>Column Conditions</u> - example input:
                <pre><code>{
`{
    "project_id": [1]
    "resource_id": ["123456789"]
}`
                }</code></pre>
                which represents <pre><code>DELETE FROM ... WHERE project_id=1 AND resource_id='123456789'</code></pre></p>
            </div>
        } else {
            return <div><p></p></div>
        }
    }

    function submitRequest() {
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

    useEffect(() => {
        fetch("/deletes-enabled").then(res => res.json()).then(data => {
            if (!(data === true || data === false)) {
                throw Error("Expected deletes-enabled to return true/false value but it didnt")
            }
            setIsDeletesEnabled(data)
        })
    }, [])

    if (!isDeletesEnabled) {
        return <p>Deletion is not enabled for this region</p>
    }

    return (
        <div>
            <div>
            <button type="button" onClick={(event) => setShowHelpMessage(!showHelpMessage)}>Help</button>
            {getHelpMessage()}
            </div>
            <input type="text" value={storageName} placeholder="storage name" onChange={(event) => setStorageName(event.target.value)} /><br/>
            <textarea value={columnConditions} placeholder="column conditions" onChange={(event) => setColumnConditions(event.target.value)} /><br/>
            <button type="submit" onClick={(event) => submitRequest()}>Submit</button>
            <p>latest result:</p>
            {result}
        </div>
      );
}

export default DeleteTool
