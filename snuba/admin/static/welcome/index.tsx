import React, { useEffect, useState } from "react";
import Client from "../api_client";

function Welcome(props: { api: Client }) {
  const [adminRegions, setAdminRegions] = useState<string[]>([]);

  useEffect(() => {
    props.api.getAdminRegions().then((res) => {
      setAdminRegions(res);
    });
  }, []);

  function urls() {
    return (
      <div>
        <p>Available regions:</p>
        <ul>
          <li>
            <a href="https://snuba-admin.getsentry.net/" target="_blank">
              SaaS
            </a>
          </li>
          {adminRegions.map((region) => (
            <li>
              <a
                href={"https://snuba-admin." + region + ".getsentry.net/"}
                target="_blank"
              >
                {region}
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
