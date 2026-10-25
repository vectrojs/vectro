// The same 10M orders for every system (the generator every benchmark here uses).
export const N = Number(process.env.ROWS ?? 10_000_000);
export const REGIONS = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];

export function makeColumns() {
  let seed = 7;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const price = new Float64Array(N), qty = new Float64Array(N), discount = new Float64Array(N);
  const regionCode = new Uint8Array(N), customer = new Int32Array(N);
  for (let i = 0; i < N; i++) {
    price[i] = Math.round(rnd() * 100000) / 100;
    qty[i] = 1 + Math.floor(rnd() * 9);
    discount[i] = Math.floor(rnd() * 30) / 100;
    regionCode[i] = Math.floor(rnd() * REGIONS.length);
    customer[i] = Math.floor(rnd() * 100_000);
  }
  return { price, qty, discount, regionCode, customer, region: Array.from(regionCode, (c) => REGIONS[c]) };
}
