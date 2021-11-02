import React, { useState } from "react";
import ReactDOM from "react-dom";

import Header from "./header";
import Nav from "./nav";
import Body from "./body";
import { NAV_ITEMS } from "./data";

const ADMIN_HOST = process.env.ADMIN_HOST;
const ADMIN_PORT = process.env.ADMIN_PORT;

if (!ADMIN_HOST || !ADMIN_PORT) {
  throw new Error("Invalid host/port provided");
}

const bodyStyle = {
  height: "100%",
  display: "flex",
};

ReactDOM.render(<App />, document.getElementById("root"));

function App() {
  const [activeTab, setActiveTab] = useState<string | null>(null);

  // state.activeTab is only be null on the initial page load
  // In this case, attempt to parse window.location.hash to determine
  // the active page
  if (activeTab === null) {
    let currentHash = window.location.hash;
    try {
      const tab = getTab(currentHash);
      setActiveTab(tab);
    } catch {
      setActiveTab(NAV_ITEMS[0].id);
    }
  }

  function navigate(nextTab: string) {
    setActiveTab(nextTab);
    window.location.hash = nextTab;
  }

  return (
    <React.Fragment>
      <Header />
      <div style={bodyStyle}>
        <Nav active={activeTab} navigate={navigate} />
        {activeTab && <Body active={activeTab} />}
      </div>
    </React.Fragment>
  );
}

function getTab(locationHash: string): string {
  if (locationHash.charAt(0) !== "#") {
    throw new Error("invalid hash");
  }

  const navItem = NAV_ITEMS.find((item) => "#" + item.id === locationHash);

  if (typeof navItem === "undefined") {
    throw new Error("invalid hash");
  }

  return navItem.id;
}
