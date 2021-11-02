import React from 'react'
import { useEffect, useState } from "react";


type SystemQuery = {
    description: string | null,
    name: string,
    sql: string,
}

const ClickhouseSystemQueries = () => {

    const [availableQueries, setQueries] = useState<SystemQuery[]>([])

    async function requestQueries() {
        const res = await fetch("clickhouse_queries")
        setQueries(await res.json());
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
