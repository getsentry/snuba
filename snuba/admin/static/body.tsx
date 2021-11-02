import React from "react";

import { NAV_ITEMS } from "./data";

type Props = {
  active: string;
};

function Body(props: Props) {
  const activeItem = NAV_ITEMS.find((item) => item.id === props.active)!;

  return <div style={bodyStyle}>{activeItem.display}</div>;
}

const bodyStyle = {
  width: "100%",
  margin: "20px",
  fontSize: 20,
};

export default Body;
