// Audit round 5 (dates and times): YEAR ... SECOND, EXTRACT(dow / isodow /
// epoch), DATE_TRUNC, STRFTIME (week numbers too), date ± INTERVAL and ± days,
// date - date, comparing with text, on edge dates (29 Feb in 1900 / 2000 / 2024
// / 2100, before 1970, years 1 to 99, 9999, ISO week 53 and week 1 across New
// Year, milliseconds, missing), checked against plain JS on tables and datasets,
// sync and async; the answers were checked against DuckDB (UTC) in
// vectro-qa-2026-11-05/dates. Also: STRFTIME pads years below 1000 and doesn't
// move years 1-99 by 1900; a missing date compared with quoted text is unknown
// (`<>`, `NOT (=)` and `NOT IN` kept those rows); COALESCE / CASE / GREATEST of
// dates are dates, not milliseconds; a CSV column with offsets like `+00`
// (PostgreSQL's export) is read as dates.
import { Engine, MODE } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const norm = (v) => (v instanceof Date ? (Number.isNaN(+v) ? null : v.toISOString()) : v === '' || v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)) ? null : typeof v === 'boolean' ? +v : Object.is(v, -0) ? 0 : v);
const same = (got, want, msg) => {
  checks++;
  const g = got.map(norm), w = want.map(norm), bad = g.length !== w.length ? -1 : g.findIndex((v, i) => (typeof v === 'number' && typeof w[i] === 'number' ? Math.abs(v - w[i]) > 1e-9 * Math.max(1, Math.abs(v)) : v !== w[i]));
  if (g.length === w.length && bad < 0) return;
  failures++;
  if (failures <= 40) console.log(`FAIL ${msg}${bad >= 0 && g.length === EDGE.length ? ` (first difference at ${EDGE[bad]})` : ''}\n  got  ${JSON.stringify(bad >= 0 ? g.slice(bad, bad + 3) : g.slice(0, 5)).slice(0, 300)}\n  want ${JSON.stringify(bad >= 0 ? w.slice(bad, bad + 3) : w.slice(0, 5)).slice(0, 300)}`);
};

const EDGE = ['1900-02-28T00:00:00Z', '1900-03-01T00:00:00Z', '2000-02-29T00:00:00Z', '2100-02-28T00:00:00Z', '2100-03-01T00:00:00Z', '2024-02-29T00:00:00Z', '2023-02-28T23:59:59.999Z',
  '1969-12-31T23:59:59.999Z', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00.001Z', '1600-01-01T00:00:00Z', '1000-06-15T12:00:00Z', '0001-01-01T00:00:00Z', '0050-03-01T10:00:00Z', '0099-12-31T00:00:00Z',
  '9999-12-31T23:59:59.999Z', '2020-12-31T00:00:00Z', '2021-01-01T00:00:00Z', '2021-01-03T23:00:00Z', '2021-01-04T00:00:00Z', '2024-12-30T00:00:00Z', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z',
  '2015-12-31T00:00:00Z', '2016-01-03T00:00:00Z', '2008-12-29T00:00:00Z', '2010-01-03T00:00:00Z', '2024-03-10T12:34:56.789Z', '1950-06-15T13:45:30.500Z', '1899-12-31T23:59:59Z', null];
const MS = EDGE.map((s) => (s ? Date.parse(s) : NaN));
const DAY = 86_400_000, miss = (x) => Number.isNaN(x);
const D = (ms) => new Date(ms);
const utc = (y, m, d) => { const x = new Date(0); x.setUTCFullYear(y, m, d); return x.getTime(); }; // (Date.UTC moves years 0-99 to 1900-1999)
const wd = (ms) => new Date(ms).getUTCDay();
const isoWeek = (ms) => { const d = new Date(ms), th = utc(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 3 - ((wd(ms) + 6) % 7)), y = new Date(th).getUTCFullYear(); return [y, 1 + Math.floor((th - utc(y, 0, 1)) / (7 * DAY))]; };
const p = (x, n = 2) => String(x).padStart(n, '0');
const yday = (ms) => { const d = new Date(ms); return 1 + Math.round((utc(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - utc(d.getUTCFullYear(), 0, 1)) / DAY); };
const weekOfYear = (ms, firstDay) => { const j = yday(ms) - 1, w = (wd(ms) - firstDay + 7) % 7; return Math.floor((j - w + 7) / 7); }; // %U (Sunday) / %W (Monday)
const F = {
  'YEAR(d)': (t) => D(t).getUTCFullYear(), 'MONTH(d)': (t) => D(t).getUTCMonth() + 1, 'DAY(d)': (t) => D(t).getUTCDate(), 'HOUR(d)': (t) => D(t).getUTCHours(), 'MINUTE(d)': (t) => D(t).getUTCMinutes(),
  'SECOND(d)': (t) => D(t).getUTCSeconds(), 'EXTRACT(dow FROM d)': wd, 'EXTRACT(isodow FROM d)': (t) => wd(t) || 7, 'EXTRACT(epoch FROM d)': (t) => t / 1000,
  "DATE_TRUNC('year', d)": (t) => D(utc(D(t).getUTCFullYear(), 0, 1)), "DATE_TRUNC('month', d)": (t) => D(utc(D(t).getUTCFullYear(), D(t).getUTCMonth(), 1)),
  "DATE_TRUNC('week', d)": (t) => D(Math.floor(t / DAY) * DAY - ((wd(t) + 6) % 7) * DAY), "DATE_TRUNC('day', d)": (t) => D(Math.floor(t / DAY) * DAY), "DATE_TRUNC('hour', d)": (t) => D(Math.floor(t / 3_600_000) * 3_600_000),
  "STRFTIME(d, '%Y-%m-%d %H:%M:%S')": (t) => { const d = D(t); return `${p(d.getUTCFullYear(), 4)}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`; },
  "STRFTIME(d, '%G-W%V-%u')": (t) => { const [y, w] = isoWeek(t); return `${p(y, 4)}-W${p(w)}-${wd(t) || 7}`; },
  "STRFTIME(d, '%j %U %W %w')": (t) => `${p(yday(t), 3)} ${p(weekOfYear(t, 0))} ${p(weekOfYear(t, 1))} ${wd(t)}`,
  "d + INTERVAL '1 day'": (t) => D(t + DAY), "d - INTERVAL '90 minutes'": (t) => D(t - 5_400_000), "d + INTERVAL '500 milliseconds'": (t) => D(t + 500),
  "d > DATE '2000-01-01'": (t) => +(t > Date.parse('2000-01-01')), "d = '2024-02-29'": (t) => +(t === Date.parse('2024-02-29')),
  // COALESCE / CASE / GREATEST of dates are dates (they gave milliseconds)
  "COALESCE(d, DATE '2000-01-01')": (t) => D(t), "CASE WHEN d > DATE '2000-01-01' THEN d END": (t) => (t > Date.parse('2000-01-01') ? D(t) : NaN), "GREATEST(d, DATE '1970-01-01')": (t) => D(Math.max(t, 0)),
};
const MISSING_GIVES = { "COALESCE(d, DATE '2000-01-01')": D(Date.parse('2000-01-01')), "GREATEST(d, DATE '1970-01-01')": D(0) };
const WHERES = { // a missing date is unknown in every comparison, so it is never kept
  "d <> '2024-02-29'": (t) => t !== Date.parse('2024-02-29'), "NOT (d = '2024-02-29')": (t) => t !== Date.parse('2024-02-29'), "d NOT IN ('2024-02-29', '1970-01-01')": (t) => t !== Date.parse('2024-02-29') && t !== 0,
  "d <> DATE '2024-02-29'": (t) => t !== Date.parse('2024-02-29'), "NOT (d > '2000-01-01')": (t) => t <= Date.parse('2000-01-01'), "d BETWEEN '1969-12-31' AND '1970-01-01'": (t) => t >= Date.parse('1969-12-31') && t <= 0,
  "d >= TIMESTAMP '2024-02-29 00:00:00'": (t) => t >= Date.parse('2024-02-29'), "d < '2024-03-10T12:34:56.789Z'": (t) => t < Date.parse('2024-03-10T12:34:56.789Z'),
};

const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-dates-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  const dd = MS.map((t) => (miss(t) ? NaN : Math.floor(t / DAY) * DAY)), n = MS.map((_, i) => [0, 1, -1, 30, 365, 59, -400][i % 7]);
  let t = e.table({ id: Float64Array.from(MS, (_, i) => i), d: MS.map((x) => new Date(x)), dd: dd.map((x) => new Date(x)), n: Float64Array.from(n) });
  if (source === 'dataset') { e.writeDataset(join(dir, 't.vectro'), t, { rowsPerChunk: 4 }); t = e.openDataset(join(dir, 't.vectro')); }
  const run = async (q) => (source === 'async' ? sql(q, { t }).toRowsAsync() : sql(q, { t }).toRows());
  for (const [x, f] of Object.entries(F)) {
    let got; try { got = (await run(`SELECT id, ${x} AS x FROM t`)).sort((a, b) => a.id - b.id).map((r) => r.x); } catch (err) { got = [err.message]; }
    same(got, MS.map((ms) => (miss(ms) ? MISSING_GIVES[x] ?? NaN : f(ms))), `${tag} ${x}`);
  }
  for (const [w, f] of Object.entries(WHERES)) {
    let got; try { got = (await run(`SELECT id FROM t WHERE ${w}`)).map((r) => r.id).sort((a, b) => a - b); } catch (err) { got = [err.message]; }
    same(got, MS.map((ms, i) => (!miss(ms) && f(ms) ? i : -1)).filter((i) => i >= 0), `${tag} WHERE ${w}`);
  }
  for (const [x, f] of [['dd + n', (a, k) => new Date(a + k * DAY)], ['dd - 1', (a) => new Date(a - DAY)], ["dd - DATE '2000-02-28'", (a) => (a - Date.parse('2000-02-28')) / DAY]]) {
    let got; try { got = (await run(`SELECT id, ${x} AS x FROM t`)).sort((a, b) => a.id - b.id).map((r) => r.x); } catch (err) { got = [err.message]; }
    same(got, dd.map((a, i) => (miss(a) ? NaN : f(a, n[i]))), `${tag} ${x}`);
  }
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

// a CSV column of timestamps with a two-digit offset (PostgreSQL writes timestamptz as 2024-01-02 03:04:05+00)
if (MODE === 'node') {
  const d2 = mkdtempSync(join(tmpdir(), 'vectro-r5-dates-csv-')), path = join(d2, 'pg.csv');
  writeFileSync(path, 'ts\n2024-01-02 03:04:05+00\n2024-06-30 23:00:00.25+02\n\n');
  const e = new Engine(), rows = e.readCSV(path).toRows();
  same(rows.map((r) => r.ts), [new Date('2024-01-02T03:04:05Z'), new Date('2024-06-30T21:00:00.250Z'), new Date(NaN)].slice(0, rows.length), 'CSV timestamps with a +00 offset are dates');
  await e.close();
  rmSync(d2, { recursive: true, force: true });
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 date checks passed`);
process.exit(failures ? 1 : 0);
