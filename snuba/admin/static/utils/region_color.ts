// Picks a stable banner color for a region name so each environment is
// visually distinct at a glance.
//
// Hues are snapped to HUE_BUCKETS evenly spaced steps around the OKLCH color
// wheel. Lightness and chroma are fixed, and chosen so every hue stays inside
// the sRGB gamut; OKLCH lightness is perceptually uniform, so every color has
// roughly the same contrast against black text (~11:1).

const HUE_BUCKETS = 12;
const LIGHTNESS = 0.8;
const CHROMA = 0.1;

// 32-bit MurmurHash3 (seed 0). Small, synchronous, and well distributed.
function murmur3(input: string): number {
  let h = 0;
  for (let i = 0; i < input.length; i++) {
    let k = Math.imul(input.charCodeAt(i), 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);
    h ^= k;
    h = (h << 13) | (h >>> 19);
    h = (Math.imul(h, 5) + 0xe6546b64) | 0;
  }
  h ^= input.length;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

function regionHue(region: string): number {
  return (murmur3(region) % HUE_BUCKETS) * (360 / HUE_BUCKETS);
}

function regionColor(region: string): string {
  return `oklch(${LIGHTNESS} ${CHROMA} ${regionHue(region)})`;
}

export { regionColor };
