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
    if (!window.confirm("Clear all site data for Snuba Admin?")) return;
    localStorage.clear();
    sessionStorage.clear();
    document.cookie.split(";").forEach((cookie) => {
      const name = cookie.split("=")[0].trim();
      document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/`;
    });
    const [cacheNames, databases] = await Promise.all([
      caches.keys(),
      indexedDB.databases(),
    ]);
    await Promise.all(cacheNames.map((name) => caches.delete(name)));
    await Promise.all(
      databases.map(
        (db) =>
          new Promise((resolve) => {
            if (!db.name) return resolve(null);
            const request = indexedDB.deleteDatabase(db.name);
            request.onsuccess = request.onerror = request.onblocked = resolve;
          })
      )
    );
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
