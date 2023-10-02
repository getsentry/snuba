import React from "react";

function Welcome() {
  const AVAILABLE_REGIONS =
    "https://www.notion.so/sentry/Snuba-Admin-8344d5d508014d4a867cdb1b8e8d22cf";

  function urls() {
    return (
      <div>
        <p>
          Available{" "}
          <a href={AVAILABLE_REGIONS} target="_blank">
            regions
          </a>
        </p>
      </div>
    );
  }

  return <div>{urls()}</div>;
}

export default Welcome;
