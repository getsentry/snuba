import { ConfigChange } from "../../runtime_config/types";
import AuditLog from "../../runtime_config/auditlog";
import Adapter from "enzyme-adapter-react-16";
import Client from "../../api_client";
import React from "react";
import Enzyme, { shallow } from "enzyme";

Enzyme.configure({ adapter: new Adapter() });

it("should fetch data from API when rendering", () => {
  let mockClient = {
    ...Client(),
    getAuditlog: jest.fn<Promise<ConfigChange[]>, []>().mockResolvedValueOnce([
      {
        key: "key",
        user: "user",
        timestamp: 0,
        before: "before",
        beforeType: "string",
        after: "after",
        afterType: "string",
      },
    ]),
  };

  shallow(<AuditLog api={mockClient} />);
  expect(mockClient.getAuditlog).toBeCalledTimes(1);
});
