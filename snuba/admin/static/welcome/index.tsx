import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";
import { COLORS } from "SnubaAdmin/theme";
import ToolGrid from "SnubaAdmin/tool_grid";
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

  return (
    <div>
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
        <ToolGrid allowedTools={allowedTools} />
      </section>
    </div>
  );
}

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

const errorStyle = {
  fontSize: 14,
  color: COLORS.ERROR,
};

export default Welcome;
