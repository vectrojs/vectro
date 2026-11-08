// The same orders and changes as bench/bench-live.mjs (same generator, same seed).
import { execSync } from 'node:child_process';
export const regions = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
export const PRODUCTS = 1000;

export function generator(seed = 1) { // mulberry32
  return () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}

export const rowOf = (rand, id) => ({ id, region: regions[Math.floor(rand() * 8)], product: `p${Math.floor(rand() * PRODUCTS)}`, price: Math.round(1 + rand() * 999), qty: 1 + Math.floor(rand() * 5) });

export const q = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
export const r2 = (x) => Math.round(x * 100) / 100;
export const wait = (ms) => new Promise((r) => setTimeout(r, ms));
export function load() {
  try { return Number(String(execSync('sysctl -n vm.loadavg')).replace(/[{}]/g, '').trim().split(/\s+/)[0]); } catch { return null; }
}
