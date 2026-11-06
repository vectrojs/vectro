// SQL LIKE patterns: % is any run of characters, _ is exactly one character
// (a code point, so an emoji is one). A regular expression made from the pattern
// backtracks: '%a%e%o%zzz' on 4 KB of text took seconds. This matcher keeps one
// place to go back to, the last %: the text after it is tried one start at a time,
// and an earlier % never needs a second try (whatever it skipped, the later one can
// skip too), so a match costs at most text length × pattern length steps.
// escape (LIKE ... ESCAPE '!'): the character after it is itself ('a!%' is 'a%' exactly).
export function likeMatcher(pattern, escape = '') {
  if ([...escape].length > 1) throw new SyntaxError(`vectro: the LIKE escape must be one character, not '${escape}'`);
  // the pattern as characters, wildcards marked: '%' and '_' are WILD only when not escaped
  const p = [], wild = [];
  const cs = Array.from(pattern);
  for (let i = 0; i < cs.length; i++) {
    if (escape && cs[i] === escape) {
      if (i + 1 >= cs.length) throw new SyntaxError(`vectro: the LIKE pattern '${pattern}' ends with its escape character`);
      p.push(cs[++i]); wild.push(false);
    } else { p.push(cs[i]); wild.push(cs[i] === '%' || cs[i] === '_'); }
  }
  const m = p.length, any = (j) => wild[j] && p[j] === '%', one = (j) => wild[j] && p[j] === '_';
  if (!wild.includes(true)) { const exact = p.join(''); return (s) => s === exact; }
  return (text) => {
    const s = Array.from(text), n = s.length;
    let i = 0, j = 0, star = -1, from = 0;
    while (i < n) {
      if (j < m && !any(j) && (one(j) || p[j] === s[i])) { i++; j++; }
      else if (j < m && any(j)) { star = j++; from = i; }
      else if (star >= 0) { j = star + 1; i = ++from; }
      else return false;
    }
    while (j < m && any(j)) j++;
    return j === m;
  };
}
