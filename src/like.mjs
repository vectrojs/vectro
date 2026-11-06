// SQL LIKE patterns: % is any run of characters, _ is exactly one character
// (a code point, so an emoji is one). A regular expression made from the pattern
// backtracks: '%a%e%o%zzz' on 4 KB of text took seconds. This matcher keeps one
// place to go back to, the last %: the text after it is tried one start at a time,
// and an earlier % never needs a second try (whatever it skipped, the later one can
// skip too), so a match costs at most text length × pattern length steps.
export function likeMatcher(pattern) {
  const p = Array.from(pattern), m = p.length;
  if (!p.includes('%') && !p.includes('_')) return (s) => s === pattern;
  return (text) => {
    const s = Array.from(text), n = s.length;
    let i = 0, j = 0, star = -1, from = 0;
    while (i < n) {
      if (j < m && p[j] !== '%' && (p[j] === '_' || p[j] === s[i])) { i++; j++; }
      else if (j < m && p[j] === '%') { star = j++; from = i; }
      else if (star >= 0) { j = star + 1; i = ++from; }
      else return false;
    }
    while (j < m && p[j] === '%') j++;
    return j === m;
  };
}
