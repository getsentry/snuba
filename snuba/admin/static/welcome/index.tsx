import React, { useEffect, useState } from "react";
import { Button } from "@mantine/core";
import Client from "SnubaAdmin/api_client";
import { NAV_ITEMS, isToolAllowed } from "SnubaAdmin/data";
import { COLORS } from "SnubaAdmin/theme";
import { AdminRegion } from "SnubaAdmin/types";
import { regionColor } from "SnubaAdmin/utils/region_color";

function Welcome(props: { api: Client }) {
  const [adminRegions, setAdminRegions] = useState<AdminRegion[]>([]);
  const [allowedTools, setAllowedTools] = useState<string[] | null>(null);
  const [toolsError, setToolsError] = useState<string | null>(null);

  useEffect(() => {
    props.api.getAdminRegions().then((res) => {
      setAdminRegions(res);
    });
    props.api
      .getAllowedTools()
      .then((res) => {
        if (!Array.isArray(res?.tools)) {
          throw new Error("unexpected response from /tools");
        }
        setAllowedTools(res.tools);
      })
      .catch((err) => {
        setToolsError(err?.message ?? String(err));
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

  const tools = NAV_ITEMS.filter(
    (item) => item.id !== "overview" && isToolAllowed(item.id, allowedTools)
  );

  return (
    <div style={containerStyle}>
      {adminRegions.length > 0 && (
        <section style={{ ...sectionStyle, ...regionSectionStyle }}>
          <h2 style={{ ...headingStyle, margin: 0 }}>Regions</h2>
          <div style={regionRowStyle}>
            {adminRegions.map((region) => (
              <a
                key={region.name}
                href={region.url}
                target="_blank"
                className="region-button"
                style={{
                  ...regionButtonStyle,
                  // Match the header banner, which calls the main region "SaaS".
                  backgroundColor: regionColor(
                    region.is_main ? "SaaS" : region.name
                  ),
                }}
              >
                {region.is_main ? `SaaS (${region.name})` : region.name}
              </a>
            ))}
          </div>
        </section>
      )}
      <section style={sectionStyle}>
        <h2 style={headingStyle}>Tools</h2>
        {toolsError && (
          <p style={errorStyle}>
            Couldn't load the tools you have access to: {toolsError}
          </p>
        )}
        <div style={tileGridStyle}>
          {tools.map((item) => {
            const [icon, ...title] = item.display.split(" ");
            return (
              <a
                key={item.id}
                href={`#${item.id}`}
                className="tool-tile"
                style={tileStyle}
              >
                <span style={tileIconStyle}>{icon}</span>
                <span style={tileTitleStyle}>{title.join(" ")}</span>
                <span style={tileDescriptionStyle}>{item.description}</span>
              </a>
            );
          })}
        </div>
      </section>
      <section style={sectionStyle}>
        <Button color="red" onClick={clearSiteData}>
          Clear site data
        </Button>
      </section>
    </div>
  );
}

const containerStyle = {
  padding: "10px 20px",
};

const sectionStyle = {
  marginBottom: 30,
};

const headingStyle = {
  fontSize: 14,
  fontWeight: 700,
  letterSpacing: "0.1em",
  textTransform: "uppercase" as const,
  color: COLORS.TEXT_LIGHTER,
  margin: "0 0 12px 0",
};

// Heading and pills share one line.
const regionSectionStyle = {
  display: "flex",
  alignItems: "center",
  gap: 16,
};

const regionRowStyle = {
  display: "flex",
  flexWrap: "wrap" as const,
  gap: 10,
};

// Same treatment as the region banner in the header.
const regionButtonStyle = {
  color: COLORS.REGION_TEXT,
  fontSize: 16,
  fontWeight: 700,
  letterSpacing: "0.05em",
  textTransform: "uppercase" as const,
  textDecoration: "none",
  padding: "6px 16px",
  borderRadius: 4,
  whiteSpace: "nowrap" as const,
};

const tileGridStyle = {
  display: "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
  gap: 16,
};

const tileStyle = {
  display: "flex",
  flexDirection: "column" as const,
  gap: 6,
  padding: 18,
  borderRadius: 8,
  textDecoration: "none",
  color: COLORS.TEXT_DEFAULT,
};

const tileIconStyle = {
  fontSize: 28,
  lineHeight: 1,
};

const tileTitleStyle = {
  fontSize: 18,
  fontWeight: 700,
};

const tileDescriptionStyle = {
  fontSize: 14,
  lineHeight: 1.4,
  color: COLORS.TEXT_LIGHTER,
};

const errorStyle = {
  fontSize: 14,
  color: COLORS.ERROR,
};

export default Welcome;
