import React from "react";

import { NAV_ITEMS } from "SnubaAdmin/data";
import Client from "SnubaAdmin/api_client";

type Props = {
  active: string;
  api: Client;
};

function Body(props: Props) {
  const { active, ...rest } = props;
  const activeItem = NAV_ITEMS.find((item) => item.id === active)!;

  return (
    <div style={bodyStyle}>
      <div style={{ marginBottom: 20 }}>{activeItem.display}</div>
      <activeItem.component {...rest} />
    </div>
  );
}

const bodyStyle = {
  width: "100%",
  maxWidth: "calc(100% - 290px)",
  margin: 20,
  fontSize: 20,
};

export default Body;
