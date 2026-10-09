import React, { useEffect, useState } from "react";
import { Button } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import { AdminRegion } from "SnubaAdmin/types";

function Welcome(props: { api: Client }) {
  const [adminRegions, setAdminRegions] = useState<AdminRegion[]>([]);

  useEffect(() => {
    props.api.getAdminRegions().then((res) => {
      setAdminRegions(res);
    });
  }, []);

  async function clearSiteData() {
    localStorage.clear();
    sessionStorage.clear();
    document.cookie.split(";").forEach((cookie) => {
      const name = cookie.split("=")[0].trim();
      document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/`;
    });
    const cacheNames = await caches.keys();
    await Promise.all(cacheNames.map((name) => caches.delete(name)));
    const databases = await indexedDB.databases();
    databases.forEach((db) => db.name && indexedDB.deleteDatabase(db.name));
    window.location.reload();
  }

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
        <Button color="red" onClick={clearSiteData}>
          Clear site data
        </Button>
      </div>
    );
  }

  return <div>{urls()}</div>;
}

export default Welcome;
