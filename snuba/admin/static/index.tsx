import React, { useState } from "react";
import ReactDOM from "react-dom/client";

import * as Sentry from "@sentry/react";
import Header from "./header";
import Nav from "./nav";
import Body from "./body";
import { NAV_ITEMS } from "./data";
import Client from "./api_client";

const containerStyle = {
  display: "flex",
  flexDirection: "column" as const,
  height: "100%",
};

const bodyStyle = {
  flexGrow: 1,
  display: "flex",
};

let client = Client();
client.getSettings().then((settings) => {
  if (settings.dsn != "") {
    Sentry.init({
      dsn: settings.dsn,
      integrations: [new Sentry.BrowserTracing(), new Sentry.Replay()],
      // Performance Monitoring
      tracesSampleRate: settings.tracesSampleRate,
      // Session Replay
      replaysSessionSampleRate: settings.replaysSessionSampleRate,
      replaysOnErrorSampleRate: settings.replaysOnErrorSampleRate,
    });
  }
});

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);

function App() {
  const [activeTab, setActiveTab] = useState<string | null>(null);

  // state.activeTab is only null on the initial page load
  // In this case, attempt to parse window.location.hash to determine
  // the active page
  if (activeTab === null) {
    let currentHash = window.location.hash;
    try {
      const tab = getTab(currentHash);
      setActiveTab(tab);
    } catch {
      navigate(NAV_ITEMS[0].id);
    }
  }

  function navigate(nextTab: string) {
    setActiveTab(nextTab);
    window.location.hash = nextTab;
  }

  return (
    <div style={containerStyle}>
      <Header />
      <div style={bodyStyle}>
        <Nav active={activeTab} navigate={navigate} api={client} />
        {activeTab && <Body active={activeTab} api={client} />}
      </div>
    </div>
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
