import React, { useState } from "react";
import ReactDOM from "react-dom/client";

import * as Sentry from "@sentry/react";
import Header from "SnubaAdmin/header";
import Nav from "SnubaAdmin/nav";
import Body from "SnubaAdmin/body";
import { NAV_ITEMS } from "SnubaAdmin/data";
import Client from "SnubaAdmin/api_client";
import { MantineProvider } from "@mantine/core";
import { ShellStateProvider } from "SnubaAdmin/sql_shell/shell_context";

import 'bootstrap/dist/css/bootstrap.min.css';

const containerStyle = {
  display: "flex",
  flexDirection: "column" as const,
  height: "100%",
};

const bodyStyle = {
  flexGrow: 1,
  display: "flex",
  minHeight: 0,
  overflow: "auto",
};

let client = Client();
client.getSettings().then((settings) => {
  if (settings.dsn != "") {
    Sentry.init({
      dsn: settings.dsn,
      integrations: [
        new Sentry.BrowserTracing(),
        new Sentry.Replay({ maskAllText: false, blockAllMedia: false }),
        new Sentry.BrowserProfilingIntegration(),
      ],
      // Performance Monitoring
      tracesSampleRate: settings.tracesSampleRate,
      // Profiles
      profilesSampleRate: settings.profilesSampleRate,
      tracePropagationTargets: settings.tracePropagationTargets ?? undefined,
      // Session Replay
      replaysSessionSampleRate: settings.replaysSessionSampleRate,
      replaysOnErrorSampleRate: settings.replaysOnErrorSampleRate,
    });
    Sentry.setUser({ email: settings.userEmail });
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
    <MantineProvider withGlobalStyles withNormalizeCSS>
      <ShellStateProvider>
        <div style={containerStyle}>
          <Header />
          <div style={bodyStyle}>
            <Nav active={activeTab} navigate={navigate} api={client} />
            {activeTab && <Body active={activeTab} api={client} />}
          </div>
        </div>
      </ShellStateProvider>
    </MantineProvider>
  );
}

function getTab(locationHash: string): string {
  if (locationHash.charAt(0) !== "#") {
    throw new Error("invalid hash");
  }

  const hash = locationHash.split("/")[0];

  const navItem = NAV_ITEMS.find((item) => "#" + item.id === hash);

  if (typeof navItem === "undefined") {
    throw new Error("invalid hash");
  }

  return navItem.id;
}

export default {};
