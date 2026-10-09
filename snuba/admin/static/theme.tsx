// Solarized dark (https://ethanschoonover.com/solarized/).
const SOLARIZED = {
  BASE03: "#002b36",
  BASE02: "#073642",
  BASE01: "#586e75",
  BASE00: "#657b83",
  BASE0: "#839496",
  BASE1: "#93a1a1",
  BASE2: "#eee8d5",
  BASE3: "#fdf6e3",
  YELLOW: "#b58900",
  ORANGE: "#cb4b16",
  RED: "#dc322f",
  MAGENTA: "#d33682",
  VIOLET: "#6c71c4",
  BLUE: "#268bd2",
  CYAN: "#2aa198",
  GREEN: "#859900",
};

const COLORS = {
  PAGE_BG: SOLARIZED.BASE03,
  // A shade darker than the page so the header reads as a separate band.
  HEADER_BG: "#001e26",
  HEADER_TEXT: SOLARIZED.BASE1,
  NAV_BG: SOLARIZED.BASE02,
  PANEL_BG: SOLARIZED.BASE02,
  REGION_TEXT: "black",
  NAV_BORDER: "#0e4553",
  TABLE_BORDER: SOLARIZED.BASE01,
  SNUBA_BLUE: SOLARIZED.BLUE,
  TEXT_DEFAULT: SOLARIZED.BASE1,
  TEXT_LIGHTER: SOLARIZED.BASE0,
  TEXT_INACTIVE: SOLARIZED.BASE0,
  BG_LIGHT: SOLARIZED.BASE02,
  RED: SOLARIZED.RED,
  WHITE: "#fff",
  BG_GRAY_LIGHT: "#0e4553",
  BG_GRAY_LIGHTER: SOLARIZED.BASE02,
  BORDER_GRAY: SOLARIZED.BASE01,
  BORDER_GRAY_LIGHT: SOLARIZED.BASE01,
  ERROR: SOLARIZED.RED,
  SUCCESS: SOLARIZED.GREEN,
};

// Mantine's dark scheme reads text from the light end of this scale and
// backgrounds from the dark end (body is index 7, inputs index 6).
const MANTINE_DARK_SHADES: [
  string, string, string, string, string,
  string, string, string, string, string,
] = [
  SOLARIZED.BASE1,
  SOLARIZED.BASE0,
  SOLARIZED.BASE00,
  SOLARIZED.BASE01,
  "#2f5560",
  "#0e4553",
  SOLARIZED.BASE02,
  SOLARIZED.BASE03,
  "#00232c",
  "#001b22",
];

// Diagonal, staggered tiling of the slogan. The fill sits just above the page
// background so the pattern stays visible without competing with text.
function sloganTiling(): string {
  const fill = "#06343f";
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

export { COLORS, SOLARIZED, MANTINE_DARK_SHADES, sloganTiling };
