import React from 'react'
import { useEffect, useState } from "react";


type SystemQuery = {
    description: string | null,
    name: string,
    sql: string,
}

type QueryRequest = {
    host: string,
    storage: string,
    query_name: string,
}

const ClickhouseSystemQueries = () => {

    const [availableQueries, setQueries] = useState<SystemQuery[]>([])

    async function requestQueries() {
        const res = await fetch("clickhouse_queries")
        setQueries(await res.json());
    }

    async function runQuery(queryName: string) {
        const params = {
            host: "localhost", // TODO (this should be a dropdown)
            storage: "transactions", // TODO This should be a dropdown
            query_name: queryName
        }

        const result = await fetch("run_clickhouse_query", {headers: {"Content-Type": "application/json"}, method: "POST", body: JSON.stringify(params)})
        console.log(await result.text())
    }

    useEffect(() => { requestQueries() }, []);

    return (
        <ul>
            {availableQueries.map((item) => (
                <li key={item.name}>
                  <div className="query-name">
                    {item.name}
                  </div>
                  <div className="query-description" style={nameStyle}>
                    {item.description}
                  </div>
                  <pre>
                    <code className="code" style={codeStyle}>
                      {item.sql}
                    </code>
                  </pre>
                  <button onClick={async () => {await runQuery(item.name)}}>{"Run me!"}</button>
                </li>
            ))}
        </ul>
    )
}


const nameStyle = {
    fontSize: 22,
    fontWeight: 100
};

const codeStyle = {
    fontSize: 10
}


export default ClickhouseSystemQueries
