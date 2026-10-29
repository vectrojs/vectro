// A sample shop for trying BI tools on Vectro: orders (a .vectro dataset on
// disk), customers and products (CSV), in one folder for `vectro serve`.
//   node examples/bi/make-data.mjs <folder> [orders, default 10,000,000]
// Two years of orders with growth, a yearly season, weekends, a few big
// customers and missing discounts; the same seed gives the same data.
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { Engine } from '../../src/node.mjs';

const dir = process.argv[2] ?? 'bi-data';
const N = Number(process.argv[3] ?? 10_000_000);
mkdirSync(dir, { recursive: true });
let seed = 42;
const rnd = () => ((seed = (Math.imul(seed, 1103515245) + 12345) >>> 0) / 4294967296);

const REGIONS = ['North America', 'Europe', 'Asia Pacific', 'Latin America', 'Middle East & Africa'];
const REGION_W = [0.38, 0.3, 0.2, 0.08, 0.04];
const COUNTRIES = { 'North America': ['United States', 'Canada', 'Mexico'], Europe: ['Germany', 'United Kingdom', 'France', 'Spain', 'Italy', 'Netherlands'], 'Asia Pacific': ['Japan', 'Australia', 'India', 'Singapore', 'South Korea'], 'Latin America': ['Brazil', 'Argentina', 'Chile', 'Colombia'], 'Middle East & Africa': ['United Arab Emirates', 'South Africa', 'Saudi Arabia', 'Nigeria'] };
const SEGMENTS = ['Consumer', 'Small Business', 'Enterprise'];
const CATEGORIES = { Electronics: ['Aurora', 'Voltex', 'Nimbus'], Home: ['Hearth', 'Linden', 'Casa'], Outdoor: ['Ridge', 'Trailhead'], Fashion: ['Marlow', 'Velvet', 'Thread & Co'], Toys: ['Pebble', 'Kitewind'] };
const CHANNELS = ['Web', 'Store', 'App', 'Partner'], CHANNEL_W = [0.42, 0.28, 0.22, 0.08];
const pickW = (xs, ws) => { let r = rnd(), i = 0; while (i < xs.length - 1 && r >= ws[i]) { r -= ws[i]; i++; } return xs[i]; };

// customers
const C = 100_000, cust = { customer_id: [], name: [], region: [], country: [], segment: [], signup_date: [] };
const FIRST = ['Ava', 'Liam', 'Mia', 'Noah', 'Zoe', 'Omar', 'Yuki', 'Lena', 'Ravi', 'Ines', 'Kofi', 'Sofia', 'Mateo', 'Hana', 'Leo', 'Amara'];
const LAST = ['Smith', 'Müller', 'Tanaka', 'Silva', 'Okafor', 'Rossi', 'Kim', 'Patel', 'García', 'Dubois', 'Novak', 'Haddad', 'Chen', 'Larsen'];
for (let i = 0; i < C; i++) {
  const region = pickW(REGIONS, REGION_W), cs = COUNTRIES[region];
  cust.customer_id.push(i + 1);
  cust.name.push(`${FIRST[Math.floor(rnd() * FIRST.length)]} ${LAST[Math.floor(rnd() * LAST.length)]}`);
  cust.region.push(region);
  cust.country.push(cs[Math.floor(rnd() * cs.length)]);
  cust.segment.push(SEGMENTS[rnd() < 0.7 ? 0 : rnd() < 0.8 ? 1 : 2]);
  cust.signup_date.push(new Date(Date.UTC(2021, 0, 1) + Math.floor(rnd() * 1400) * 86_400_000).toISOString().slice(0, 10));
}
// products
const prod = { product_id: [], product: [], category: [], brand: [], list_price: [], unit_cost: [] };
const cats = Object.keys(CATEGORIES);
for (let i = 0; i < 500; i++) {
  const category = cats[i % cats.length], brands = CATEGORIES[category], brand = brands[Math.floor(rnd() * brands.length)];
  const price = Math.round((5 + rnd() ** 2 * (category === 'Electronics' ? 1500 : 300)) * 100) / 100;
  prod.product_id.push(i + 1);
  prod.product.push(`${brand} ${category.slice(0, -1) || category} ${100 + i}`);
  prod.category.push(category);
  prod.brand.push(brand);
  prod.list_price.push(price);
  prod.unit_cost.push(Math.round(price * (0.45 + rnd() * 0.25) * 100) / 100);
}
const csv = (cols) => { const names = Object.keys(cols), n = cols[names[0]].length; let s = `${names.join(',')}\n`; for (let i = 0; i < n; i++) s += `${names.map((k) => { const v = String(cols[k][i]); return /[",\n]/.test(v) ? `"${v.replaceAll('"', '""')}"` : v; }).join(',')}\n`; return s; };
writeFileSync(join(dir, 'customers.csv'), csv(cust));
writeFileSync(join(dir, 'products.csv'), csv(prod));

// orders, written a chunk at a time
const engine = new Engine();
const w = engine.createDataset(join(dir, 'orders.vectro'));
const START = Date.UTC(2024, 0, 1), DAYS = 730, CHUNK = 1_000_000;
let id = 1;
for (let done = 0; done < N; done += CHUNK) {
  const n = Math.min(CHUNK, N - done);
  const o = { order_id: new Float64Array(n), ts: new Float64Array(n), customer_id: new Float64Array(n), product_id: new Float64Array(n), region: new Array(n), channel: new Array(n), qty: new Float64Array(n), unit_price: new Float64Array(n), discount: new Float64Array(n) };
  for (let i = 0; i < n; i++) {
    // a day: more later (growth), more in November and December, fewer on weekends
    let day;
    for (;;) {
      day = Math.floor(rnd() * DAYS);
      const d = new Date(START + day * 86_400_000), m = d.getUTCMonth(), wd = d.getUTCDay();
      const weight = (0.6 + 0.4 * day / DAYS) * (m === 10 ? 1.35 : m === 11 ? 1.6 : m === 0 ? 0.8 : 1) * (wd === 0 || wd === 6 ? 0.75 : 1);
      if (rnd() < weight / 1.6) break;
    }
    const c = rnd() < 0.08 ? Math.floor(rnd() * 200) : Math.floor(rnd() * C); // (a few customers buy a lot)
    const p = Math.floor((rnd() ** 1.6) * 500);
    o.order_id[i] = id++;
    o.ts[i] = START + day * 86_400_000 + Math.floor((8 + rnd() * 14) * 3_600_000) + Math.floor(rnd() * 3_600_000 / 1000) * 1000;
    o.customer_id[i] = c + 1;
    o.product_id[i] = p + 1;
    o.region[i] = cust.region[c];
    o.channel[i] = pickW(CHANNELS, CHANNEL_W);
    o.qty[i] = 1 + Math.floor(rnd() ** 3 * 6);
    o.unit_price[i] = prod.list_price[p];
    o.discount[i] = rnd() < 0.1 ? NaN : rnd() < 0.7 ? 0 : [0.05, 0.1, 0.15, 0.2, 0.25][Math.floor(rnd() * 5)];
  }
  w.append({ ...o, ts: Array.from(o.ts, (t) => new Date(t)) });
  process.stderr.write(`\r${(done + n).toLocaleString()} orders`);
}
w.close();
await engine.close();
process.stderr.write(`\nwrote ${dir}/orders.vectro, customers.csv, products.csv\n`);
