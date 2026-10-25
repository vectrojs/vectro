// Dates and times. A date column holds milliseconds since 1970-01-01 UTC
// (float64, exact for any date people use); missing dates are NaN. This file
// parses ISO 8601 text (from strings or straight from CSV bytes) and expands
// the date functions of expressions into plain arithmetic, so they run in the
// same fused kernels as everything else (no new kernel code):
//   year(ts) month(ts) day(ts) hour(ts) minute(ts) second(ts)
//   weekday(ts)  (1 = Monday ... 7 = Sunday, ISO)
//   dayStart(ts) hourStart(ts) weekStart(ts) (Monday) monthStart(ts)  -> ms
//   date('2024-03-01') / date('2024-03-01T12:00:00Z')                  -> ms
// Times are UTC; text with an offset (+02:00) is converted to UTC.

export const DAY = 86_400_000, HOUR = 3_600_000, MINUTE = 60_000;

// Days since 1970-01-01 of a civil date (proleptic Gregorian, any year).
export function daysFromCivil(y, m, d) {
  y -= m <= 2 ? 1 : 0;
  const era = Math.floor(y / 400), yoe = y - era * 400;
  const doy = Math.floor((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5) + d - 1;
  return era * 146097 + yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + doy - 719468;
}

// ISO 8601 date or date-time from bytes [s, e): YYYY-MM-DD, then optionally
// T or space, HH:MM, :SS, .fraction, and Z or ±HH:MM / ±HHMM. NaN if it isn't one.
export function parseDateBytes(b, s, e) {
  while (s < e && (b[s] === 32 || b[s] === 34)) s++;           // spaces, quotes
  while (e > s && (b[e - 1] === 32 || b[e - 1] === 34 || b[e - 1] === 13)) e--;
  let i = s, neg = false;
  if (b[i] === 45 || b[i] === 43) { neg = b[i] === 45; i++; }  // -YYYY / +YYYYY
  const num = (n) => { let v = 0; for (let k = 0; k < n; k++, i++) { const c = b[i] - 48; if (!(c >= 0 && c <= 9) || i >= e) return -1; v = v * 10 + c; } return v; };
  let y = 0, digits = 0;
  while (i < e && b[i] >= 48 && b[i] <= 57) { y = y * 10 + (b[i] - 48); i++; digits++; }
  if (digits < 4 || b[i] !== 45) return NaN;
  i++;
  const mo = num(2);
  if (mo < 1 || mo > 12 || b[i] !== 45) return NaN;
  i++;
  const d = num(2);
  if (d < 1 || d > 31) return NaN;
  let ms = daysFromCivil(neg ? -y : y, mo, d) * DAY;
  if (i === e) return ms;
  if (b[i] !== 84 && b[i] !== 116 && b[i] !== 32) return NaN;   // T, t or space
  i++;
  const h = num(2);
  if (h < 0 || h > 24 || b[i] !== 58) return NaN;
  i++;
  const mi = num(2);
  if (mi < 0 || mi > 59) return NaN;
  ms += h * HOUR + mi * MINUTE;
  if (i < e && b[i] === 58) {
    i++;
    const sec = num(2);
    if (sec < 0 || sec > 60) return NaN;
    ms += sec * 1000;
    if (i < e && (b[i] === 46 || b[i] === 44)) { // fraction of a second
      i++;
      let scale = 100, any = false;
      while (i < e && b[i] >= 48 && b[i] <= 57) { ms += (b[i] - 48) * scale; scale /= 10; i++; any = true; }
      if (!any) return NaN;
    }
  }
  if (i === e) return ms;
  if (b[i] === 90 || b[i] === 122) return i + 1 === e ? ms : NaN; // Z
  if (b[i] === 43 || b[i] === 45) {
    const sign = b[i] === 45 ? -1 : 1;
    i++;
    const oh = num(2);
    if (oh < 0) return NaN;
    if (i < e && b[i] === 58) i++;
    const om = i < e ? num(2) : 0;
    if (om < 0 || i !== e) return NaN;
    return ms - sign * (oh * HOUR + om * MINUTE);
  }
  return NaN;
}

const enc = new TextEncoder();
export function parseDateText(s) {
  const b = enc.encode(String(s));
  return parseDateBytes(b, 0, b.length);
}

// Does this text look like an ISO date (for guessing CSV column types)?
export const looksLikeDate = (s) => /^\s*"?[+-]?\d{4,}-\d\d-\d\d([Tt ]\d\d:\d\d(:\d\d([.,]\d+)?)?([Zz]|[+-]\d\d:?\d\d)?)?"?\s*$/.test(s) && !Number.isNaN(parseDateText(s));

// A value given for a date: Date object, ISO text or milliseconds.
export function toMillis(v) {
  if (v instanceof Date) return v.getTime();
  if (typeof v === 'number') return v;
  if (v == null || v === '') return NaN;
  return parseDateText(v);
}

// ---- expression functions (AST in, AST out) ----------------------------------
const num = (v) => ({ t: 'num', v });
const bin = (op, l, r) => ({ t: 'bin', op, l, r });
const add = (l, r) => bin('+', l, r), sub = (l, r) => bin('-', l, r), mul = (l, r) => bin('*', l, r), div = (l, r) => bin('/', l, r);
const floor = (x) => ({ t: 'call', fn: 'floor', args: [x] });
const cond = (c, x, y) => ({ t: 'cond', c, x, y });
const fdiv = (x, k) => floor(div(x, num(k)));
const mod = (x, k) => sub(x, mul(fdiv(x, k), num(k)));

// Civil date parts of ms (Howard Hinnant's days -> civil algorithm).
function civil(ts) {
  const z = add(fdiv(ts, DAY), num(719468));
  const era = fdiv(z, 146097);
  const doe = sub(z, mul(era, num(146097)));
  const yoe = fdiv(sub(add(sub(doe, fdiv(doe, 1460)), fdiv(doe, 36524)), fdiv(doe, 146096)), 365);
  const doy = sub(doe, sub(add(mul(num(365), yoe), fdiv(yoe, 4)), fdiv(yoe, 100)));
  const mp = fdiv(add(mul(num(5), doy), num(2)), 153);
  const d = add(sub(doy, fdiv(add(mul(num(153), mp), num(2)), 5)), num(1));
  const m = cond(bin('<', mp, num(10)), add(mp, num(3)), sub(mp, num(9)));
  const y = add(add(yoe, mul(era, num(400))), cond(bin('<=', m, num(2)), num(1), num(0)));
  return { y, m, d };
}

export const DATE_FNS = {
  year: (ts) => civil(ts).y,
  month: (ts) => civil(ts).m,
  day: (ts) => civil(ts).d,
  hour: (ts) => mod(fdiv(ts, HOUR), 24),
  minute: (ts) => mod(fdiv(ts, MINUTE), 60),
  second: (ts) => mod(fdiv(ts, 1000), 60),
  weekday: (ts) => add(mod(add(fdiv(ts, DAY), num(3)), 7), num(1)), // 1970-01-01 was a Thursday
  dayStart: (ts) => mul(fdiv(ts, DAY), num(DAY)),
  hourStart: (ts) => mul(fdiv(ts, HOUR), num(HOUR)),
  weekStart: (ts) => mul(sub(fdiv(ts, DAY), mod(add(fdiv(ts, DAY), num(3)), 7)), num(DAY)),
  monthStart: (ts) => sub(mul(fdiv(ts, DAY), num(DAY)), mul(sub(civil(ts).d, num(1)), num(DAY))),
};
// functions whose result is itself a date
export const DATE_RESULT = new Set(['dayStart', 'hourStart', 'weekStart', 'monthStart', 'date']);
