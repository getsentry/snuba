// High-contrast light palette: near-black text on near-white surfaces.
const COLORS = {
  PAGE_BG: "#fafafa",
  HEADER_BG: "#ffffff",
  HEADER_TEXT: "#111111",
  PANEL_BG: "#ffffff",
  REGION_TEXT: "black",
  NAV_BORDER: "#d4d4d4",
  // Slightly darker than TABLE_HEADER_BG so it shows on the header and body.
  TABLE_BORDER: "#b9c6d6",
  TABLE_BG: "#ffffff",
  // Light slate blue, calmer than SNUBA_BLUE.
  TABLE_HEADER_BG: "#cfdbea",
  TABLE_HEADER_TEXT: "#111111",
  SNUBA_BLUE: "#1f6feb",
  TEXT_DEFAULT: "#111111",
  TEXT_LIGHTER: "#3d3d3d",
  TEXT_INACTIVE: "#2b2b2b",
  BG_LIGHT: "#e3e3e3",
  RED: "#c42e2b",
  WHITE: "#fff",
  BG_GRAY_LIGHT: "#f0f0f0",
  BG_GRAY_LIGHTER: "#f7f7f7",
  BORDER_GRAY: "#c8c8c8",
  BORDER_GRAY_LIGHT: "#dddddd",
  ERROR: "#c62828",
  DANGER: "#a8322d",
  SUCCESS: "#1b5e20",
};

// Diagonal, staggered tiling of the slogan. The fill is only a few shades off
// the page background so the pattern stays visible without competing with text.
function sloganTiling(): string {
  const fill = "#ececec";
  const text = (x: number, y: number) =>
    `<text x='${x}' y='${y}' text-anchor='middle' dominant-baseline='middle' ` +
    `transform='rotate(-30 ${x} ${y})' font-family='-apple-system,sans-serif' ` +
    `font-size='22' font-weight='800' letter-spacing='2' fill='${fill}'>MEREDITH IS #1</text>`;
  const svg =
    `<svg xmlns='http://www.w3.org/2000/svg' width='360' height='220'>` +
    text(90, 55) +
    text(270, 165) +
    `</svg>`;
  return `url("data:image/svg+xml,${encodeURIComponent(svg)}")`;
}

export { COLORS, sloganTiling };
