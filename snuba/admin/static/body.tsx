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
      <activeItem.component {...rest} />
    </div>
  );
}

const bodyStyle = {
  flex: 1,
  minWidth: 0,
  padding: "20px 24px",
  fontSize: 20,
};

export default Body;
