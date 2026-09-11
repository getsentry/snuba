import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { AdminRegion } from "SnubaAdmin/types";

function Welcome(props: { api: Client }) {
  const [adminRegions, setAdminRegions] = useState<AdminRegion[]>([]);

  useEffect(() => {
    props.api.getAdminRegions().then((res) => {
      setAdminRegions(res);
    });
  }, []);

  function urls() {
    return (
      <div>
        <p>MEREDITH IS #1</p>
        <p>Available regions:</p>
        <ul>
          {adminRegions.map((region) => (
            <li key={region.name}>
              <a href={region.url} target="_blank">
                {region.is_main ? `SaaS (${region.name})` : region.name}
              </a>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  return <div>{urls()}</div>;
}

export default Welcome;
