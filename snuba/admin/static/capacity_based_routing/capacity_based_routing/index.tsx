import React, { useEffect, useState } from "react";
import Client from "SnubaAdmin/api_client";

interface CapacityBasedRoutingProps {
  // Add props as needed
}

const CapacityBasedRouting: React.FC<CapacityBasedRoutingProps> = () => {
  return (
    <div className="capacity-based-routing">
      <h2>Capacity Based Routing</h2>
      <div className="content">
        {/* Add your routing configuration UI here */}
        <p>Configure capacity-based routing settings</p>
      </div>
    </div>
  );
};

export default CapacityBasedRouting;
