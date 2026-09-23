import { regionColor, regionHue } from "../../utils/region_color";

// Region names as displayed by the header.
const KNOWN_REGIONS = ["localhost", "de", "SaaS", "s4s2"];

function hueDistance(a: number, b: number): number {
  const d = Math.abs(a - b);
  return Math.min(d, 360 - d);
}

describe("regionColor", () => {
  it("Should keep known regions at least 60 degrees apart in hue", () => {
    for (let i = 0; i < KNOWN_REGIONS.length; i++) {
      for (let j = i + 1; j < KNOWN_REGIONS.length; j++) {
        expect(
          hueDistance(regionHue(KNOWN_REGIONS[i]), regionHue(KNOWN_REGIONS[j]))
        ).toBeGreaterThanOrEqual(60);
      }
    }
  });

  it("Should be stable for the same region", () => {
    expect(regionColor("de")).toEqual(regionColor("de"));
  });

  it("Should return an oklch color with fixed lightness and chroma", () => {
    expect(regionColor("de")).toMatch(/^oklch\(0\.8 0\.1 \d+\)$/);
  });
});
