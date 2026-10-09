// High-contrast light palette: near-black text on near-white surfaces.
const COLORS = {
  PAGE_BG: "#fafafa",
  HEADER_BG: "#ffffff",
  HEADER_TEXT: "#111111",
  NAV_BG: "#f2f2f2",
  PANEL_BG: "#ffffff",
  REGION_TEXT: "black",
  NAV_BORDER: "#d4d4d4",
  TABLE_BORDER: "#cbcbcb",
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
  SUCCESS: "#1b5e20",
};

// Diagonal, staggered tiling of the slogan. The fill is only a few shades off
// the page background so the pattern stays visible without competing with text.
function sloganTiling(): string {
  const fill = "#ececec";
  const angle = -55;
  const width = 340;
  const height = 340;
  const text = (x: number, y: number) =>
    `<text x='${x}' y='${y}' text-anchor='middle' dominant-baseline='middle' ` +
    `transform='rotate(${angle} ${x} ${y})' font-family='-apple-system,sans-serif' ` +
    `font-size='22' font-weight='800' letter-spacing='2' fill='${fill}'>MEREDITH IS #1</text>`;
  // Steep text overflows the tile, so repeat each copy one tile over in every
  // direction; the overflow then lines up with the neighbouring tiles.
  const wrapped = (x: number, y: number) =>
    [-1, 0, 1]
      .flatMap((dx) => [-1, 0, 1].map((dy) => text(x + dx * width, y + dy * height)))
      .join("");
  const svg =
    `<svg xmlns='http://www.w3.org/2000/svg' width='${width}' height='${height}'>` +
    wrapped(width / 4, height / 4) +
    wrapped((3 * width) / 4, (3 * height) / 4) +
    `</svg>`;
  return `url("data:image/svg+xml,${encodeURIComponent(svg)}")`;
}

export { COLORS, sloganTiling };
