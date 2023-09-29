import React from "react";

function Welcome() {
  const PROD_URL_START = "https://snuba-admin";

  const AVAILABLE_REGIONS =
    "https://www.notion.so/sentry/Snuba-Admin-8344d5d508014d4a867cdb1b8e8d22cf";

  const URL = window.location.origin;

  function urls() {
    let current_region = "localhost";
    if (URL.startsWith(PROD_URL_START)) {
      current_region = currentRegion();
    }
    return (
      <div>
        <p>
          Current region: <strong>{current_region}</strong>
        </p>
        <p>
          Available{" "}
          <a href={AVAILABLE_REGIONS} target="_blank">
            regions
          </a>
        </p>
      </div>
    );
  }

  function currentRegion() {
    if (URL.startsWith(PROD_URL_START + ".getsentry.net")) {
      return "SaaS";
    }

    let regex = /https:\/\/snuba-admin\.(.*?)\.getsentry\.net/;
    let match = URL.match(regex);

    if (match && match[1]) {
      return match[1];
    }
    return "<unknown current region>";
  }

  return <div>{urls()}</div>;
}

export default Welcome;
