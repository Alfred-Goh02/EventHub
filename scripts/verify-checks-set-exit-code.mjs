/**
 * Asserts that every verifier sets its exit code at the point an assertion fails,
 * not only in a gate at the end of the run.
 *
 * CLAUDE.md, "A failed check sets the exit code where it fails": two search
 * verifiers printed FAIL and exited 0. Each had a `return` inside a `try` for the
 * case where its service never started; the `finally` ran, the function returned,
 * and the `if (failures > 0)` gate below it never executed. The rule is that
 * `check()` sets `process.exitCode = 1` itself. At 95475a1, 37 of the 98 verifiers
 * did not: their check() only counted, and the count meant something only if the
 * gate was reached. Run against that tree, this file failed exactly those 37.
 *
 * WHAT IT READS. Every scripts/verify-*.mjs, derived from the directory, with
 * comments stripped first: a comment that says `process.exitCode = 1` is not a
 * line that sets it, and verify-live-fanout.mjs's check() has exactly such a
 * comment beside the real line. Strings, templates and regex literals are kept
 * whole, so a `//` inside one is not taken for a comment.
 *
 * A HELPER is a named function -- a declaration, or a const/let bound to an arrow
 * or function expression, at any depth -- whose own body (named functions inside
 * it are judged on their own) records a failure:
 *   - it increments a counter with "fail" in its name (`failures += 1`), or
 *   - a string or template in it contains the word FAIL.
 * A function that prints FAIL only after reading a counter some helper increments
 * is the end-of-run GATE, and is not judged: it is what the helpers must not
 * depend on.
 *
 * A helper passes when its own body sets `process.exitCode` to a non-zero number,
 * calls `process.exit()` with one, or calls another helper that passes -- a
 * `fail()` that calls `check(false, ...)` sets the code through it.
 *
 * A FAIL PRINTED OUTSIDE A HELPER -- in a gate's early return, or at the top
 * level -- is judged by the block it prints in: that `{ }` must set the code, call
 * a helper that does, or read a helper's counter (the gate's own report, which the
 * helpers have already made true). This is the search verifiers' shape exactly:
 * `if (!up) { console.error('FAIL: ...'); return; }`.
 *
 * WHAT IT DOES NOT SEE. A failure printed without the word FAIL and recorded
 * without a fail counter. A FAIL in a braceless `if` is judged by the block around
 * the `if`, which is coarser. A FAIL in a bare top-level statement, in no block, is
 * not judged: it would print on every run. INLINE below names the verifiers that
 * have no helper at all and says how each records a failure; the list is checked
 * against the files, so a file that gains a helper must leave it and be judged
 * like the rest.
 *
 * Run: node scripts/verify-checks-set-exit-code.mjs [root]
 *   root: read <root>/scripts instead of this tree's, so a planted helper can be
 *   shown red in a copy without committing it. Needs nothing else.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const THIS_FILE = basename(fileURLToPath(import.meta.url));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/**
 * Verifiers with no helper, and how each records a failure instead. Being on this
 * list is a claim, so it is checked: the file must exist, must have no helper, and
 * must exit non-zero somewhere.
 */
const INLINE = {
  'verify-inventory-reconciliation.mjs':
    'each of its two failures is printed where it is found and ends the run there with process.exit(1), and a failed FORCE restore sets process.exitCode = 1',
};

// ---------------------------------------------------------------------------
// The lexer
// ---------------------------------------------------------------------------

const REGEX_AFTER = new Set(['return', 'typeof', 'case', 'do', 'else', 'in', 'of', 'new', 'delete', 'void', 'throw', 'instanceof', 'yield', 'await']);
/**
 * An escape as the string holds it, so `\nFAIL` reads as a newline then FAIL: kept
 * as the two characters, `nFAIL` has no word boundary before the F, and live-fanout's
 * fatal() went unseen until this.
 */
const unescape = (c) => ({ n: '\n', r: '\r', t: '\t' })[c] ?? c ?? '';

/**
 * JavaScript source read three ways, every view the same length as the source so
 * an index in one is an index in all of them, and a line number is the file's:
 *   code     every comment blanked to spaces, newlines kept
 *   skel     code with the contents of every string, template and regex literal
 *            blanked too, so a brace, a call or a `process.exit` inside one is not
 *            read as code
 *   strings  { start, text } for each string, and each text chunk of a template
 */
function lex(src) {
  const code = src.split('');
  const skel = src.split('');
  const strings = [];
  const blank = (arr, i) => {
    if (i < arr.length && arr[i] !== '\n' && arr[i] !== '\r') arr[i] = ' ';
  };
  const n = src.length;
  const templates = []; // brace depth at each open `${`
  let depth = 0;
  let prev = ''; // the last significant character of code
  let prevWord = '';
  let i = 0;

  /** From just inside a template's text to its end or its next `${`. */
  const templateChunk = () => {
    const start = i;
    let text = '';
    while (i < n) {
      const c = src[i];
      if (c === '\\') {
        text += unescape(src[i + 1]);
        blank(skel, i);
        blank(skel, i + 1);
        i += 2;
      } else if (c === '`') {
        strings.push({ start, text });
        i += 1;
        return;
      } else if (c === '$' && src[i + 1] === '{') {
        strings.push({ start, text });
        templates.push(depth);
        i += 2;
        return;
      } else {
        text += c;
        blank(skel, i);
        i += 1;
      }
    }
    strings.push({ start, text });
  };

  while (i < n) {
    const c = src[i];
    const d = src[i + 1];
    if (c === ' ' || c === '\t' || c === '\n' || c === '\r') {
      i += 1;
    } else if (c === '/' && d === '/') {
      while (i < n && src[i] !== '\n') {
        blank(code, i);
        blank(skel, i);
        i += 1;
      }
    } else if (c === '/' && d === '*') {
      const close = src.indexOf('*/', i + 2);
      const stop = close === -1 ? n : close + 2;
      for (; i < stop; i += 1) {
        blank(code, i);
        blank(skel, i);
      }
    } else if (c === "'" || c === '"') {
      const start = i + 1;
      let text = '';
      i += 1;
      while (i < n && src[i] !== c && src[i] !== '\n') {
        if (src[i] === '\\') {
          text += unescape(src[i + 1]);
          blank(skel, i);
          blank(skel, i + 1);
          i += 2;
        } else {
          text += src[i];
          blank(skel, i);
          i += 1;
        }
      }
      strings.push({ start, text });
      i += 1;
      prev = c;
      prevWord = '';
    } else if (c === '`') {
      i += 1;
      templateChunk();
      prev = '`';
      prevWord = '';
    } else if (c === '/' && (prev === '' || '(,=:[!&|?{};+-*%<>~^'.includes(prev) || REGEX_AFTER.has(prevWord))) {
      // A regex literal: its body is blanked in skel, to the closing slash outside a class.
      i += 1;
      let inClass = false;
      while (i < n && src[i] !== '\n') {
        const r = src[i];
        if (r === '\\') {
          blank(skel, i);
          blank(skel, i + 1);
          i += 2;
          continue;
        }
        if (r === '/' && !inClass) break;
        if (r === '[') inClass = true;
        else if (r === ']') inClass = false;
        blank(skel, i);
        i += 1;
      }
      i += 1;
      while (i < n && /[a-z]/i.test(src[i])) i += 1;
      prev = '/';
      prevWord = '';
    } else if (/[\w$]/.test(c)) {
      const start = i;
      while (i < n && /[\w$]/.test(src[i])) i += 1;
      prevWord = src.slice(start, i);
      prev = src[i - 1];
    } else if (c === '}' && templates.length > 0 && templates[templates.length - 1] === depth) {
      // The `}` that closes a template's `${`: back into its text.
      templates.pop();
      i += 1;
      templateChunk();
      prev = '`';
      prevWord = '';
    } else {
      if (c === '{') depth += 1;
      if (c === '}') depth -= 1;
      prev = c;
      prevWord = '';
      i += 1;
    }
  }
  return { code: code.join(''), skel: skel.join(''), strings };
}

// ---------------------------------------------------------------------------
// Functions, and which of them record a failure
// ---------------------------------------------------------------------------

const OPEN = { '(': ')', '[': ']', '{': '}' };

/** The index of the bracket that closes the one at `at`, read on skel. */
function closing(skel, at) {
  const stack = [];
  for (let i = at; i < skel.length; i += 1) {
    const c = skel[i];
    if (OPEN[c]) stack.push(OPEN[c]);
    else if (c === ')' || c === ']' || c === '}') {
      if (stack.pop() !== c) return -1;
      if (stack.length === 0) return i;
    }
  }
  return -1;
}

const skipSpace = (s, i) => {
  while (i < s.length && /\s/.test(s[i])) i += 1;
  return i;
};

/** Where an arrow's expression body ends: a `;` or `,` at its own depth, or a closer that is not its own. */
function expressionEnd(skel, at) {
  let level = 0;
  for (let i = at; i < skel.length; i += 1) {
    const c = skel[i];
    if (OPEN[c]) level += 1;
    else if (c === ')' || c === ']' || c === '}') {
      if (level === 0) return i;
      level -= 1;
    } else if ((c === ';' || c === ',') && level === 0) return i;
  }
  return skel.length;
}

/** Every named function: { name, start, body: [from, to) }, read on skel. */
function functionsOf(skel) {
  const found = [];
  for (const m of skel.matchAll(/\bfunction\s*\*?\s*([A-Za-z_$][\w$]*)\s*\(/g)) {
    const params = closing(skel, m.index + m[0].length - 1);
    if (params < 0) continue;
    const open = skipSpace(skel, params + 1);
    if (skel[open] !== '{') continue;
    const close = closing(skel, open);
    if (close > 0) found.push({ name: m[1], start: m.index, body: [open, close + 1] });
  }
  for (const m of skel.matchAll(/\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s+)?/g)) {
    let at = m.index + m[0].length;
    if (/^function\b/.test(skel.slice(at, at + 9))) {
      const paren = skel.indexOf('(', at);
      const params = paren < 0 ? -1 : closing(skel, paren);
      if (params < 0) continue;
      const open = skipSpace(skel, params + 1);
      if (skel[open] !== '{') continue;
      const close = closing(skel, open);
      if (close > 0) found.push({ name: m[1], start: m.index, body: [open, close + 1] });
      continue;
    }
    if (skel[at] === '(') {
      const params = closing(skel, at);
      if (params < 0) continue;
      at = skipSpace(skel, params + 1);
    } else {
      const param = /^[A-Za-z_$][\w$]*/.exec(skel.slice(at));
      if (!param) continue;
      at = skipSpace(skel, at + param[0].length);
    }
    if (skel.slice(at, at + 2) !== '=>') continue;
    at = skipSpace(skel, at + 2);
    const end = skel[at] === '{' ? closing(skel, at) + 1 : expressionEnd(skel, at);
    if (end > at) found.push({ name: m[1], start: m.index, body: [at, end] });
  }
  // `const f = function g() {}` is found by both patterns: one entry per body.
  const byBody = new Map();
  for (const f of found.sort((a, b) => a.start - b.start)) if (!byBody.has(f.body[0])) byBody.set(f.body[0], f);
  return [...byBody.values()];
}

const INCREMENT = /\b([\w$]*fail[\w$]*)\s*(?:\+\+|\+=\s*[1-9])|\+\+\s*([\w$]*fail[\w$]*)\b/gi;
const SETS_EXIT = /\bprocess\s*\.\s*exitCode\s*=\s*[1-9]\d*\b|\bprocess\s*\.\s*exit\s*\(\s*[1-9]\d*\s*\)/;
const FAIL_WORD = /\bFAIL\b/;

/**
 * One file's helpers and how each was judged:
 *   helpers  [{ name, line, sets, via }]   sets: the exit code is set, by its own body or `via` another helper
 *   gates    [{ name, line }]             printed FAIL after reading a helper's counter
 *   inline   [{ line, ok }]               a FAIL printed outside any helper, and whether its block sets the code
 *   exits    whether anything in the file sets a non-zero exit code
 */
function judge(src) {
  const { skel, strings } = lex(src);
  const lineOf = (i) => src.slice(0, i).split('\n').length;
  const all = functionsOf(skel);
  const fns = all.map((f) => {
    // Its own body: the named functions inside it are blanked, and judged on their own.
    const inner = all.filter((g) => g !== f && g.start > f.body[0] && g.body[1] <= f.body[1]);
    let own = skel.slice(f.body[0], f.body[1]);
    for (const g of inner) {
      const from = g.start - f.body[0];
      const to = g.body[1] - f.body[0];
      own = own.slice(0, from) + own.slice(from, to).replace(/[^\n]/g, ' ') + own.slice(to);
    }
    const insideInner = (i) => inner.some((g) => i >= g.start && i < g.body[1]);
    const texts = strings.filter((s) => s.start >= f.body[0] && s.start < f.body[1] && !insideInner(s.start));
    const counters = [...own.matchAll(INCREMENT)].map((m) => m[1] ?? m[2]);
    return {
      def: f,
      name: f.name,
      line: lineOf(f.start),
      own,
      counters,
      printsFail: texts.some((s) => FAIL_WORD.test(s.text)),
      setsOwn: SETS_EXIT.test(own),
    };
  });
  const counted = new Set(fns.flatMap((f) => f.counters));
  const reads = (f) => [...counted].some((c) => new RegExp(`(?<![\\w$])${c.replace(/\$/g, '\\$')}(?![\\w$])`).test(f.own));
  const gates = fns.filter((f) => f.counters.length === 0 && f.printsFail && reads(f));
  const helpers = fns.filter((f) => (f.counters.length > 0 || f.printsFail) && !gates.includes(f));

  // A helper that calls one that sets the code sets it too. To a fixed point.
  const sets = new Map(helpers.map((h) => [h, h.setsOwn ? null : undefined]));
  for (let changed = true; changed; ) {
    changed = false;
    for (const h of helpers) {
      if (sets.get(h) !== undefined) continue;
      const via = helpers.find((o) => o !== h && sets.get(o) !== undefined && new RegExp(`(?<![\\w$.])${o.name}\\s*\\(`).test(h.own));
      if (via) {
        sets.set(h, via.name);
        changed = true;
      }
    }
  }
  /**
   * A FAIL printed outside any helper: in a gate, or at the top level. The block
   * around it -- its `if`, not the template it sits in -- must set the code, call a
   * helper that does, or read a helper's counter (the gate's own report, which the
   * helpers have already made true). A bare top-level statement has no block and is
   * not judged: it would print on every run, which is a fixture, not an assertion.
   */
  const passing = helpers.filter((h) => sets.get(h) !== undefined);
  const innermost = (i) =>
    all.filter((f) => i >= f.body[0] && i < f.body[1]).sort((a, b) => b.body[0] - a.body[0])[0] ?? null;
  const inline = [];
  for (const s of strings) {
    if (!FAIL_WORD.test(s.text)) continue;
    const owner = innermost(s.start);
    if (owner && helpers.some((h) => h.def === owner)) continue;
    const block = blockAround(skel, s.start);
    if (!block) continue;
    const text = skel.slice(block[0], block[1]);
    const ok =
      SETS_EXIT.test(text) ||
      passing.some((h) => new RegExp(`(?<![\\w$.])${h.name}\\s*\\(`).test(text)) ||
      [...counted].some((c) => new RegExp(`(?<![\\w$])${c.replace(/\$/g, '\\$')}(?![\\w$])`).test(text));
    inline.push({ line: lineOf(s.start), ok });
  }

  return {
    helpers: helpers.map((h) => ({ name: h.name, line: h.line, sets: sets.get(h) !== undefined, via: sets.get(h) ?? null })),
    gates: gates.map((g) => ({ name: g.name, line: g.line })),
    inline,
    exits: SETS_EXIT.test(skel),
  };
}

/** The innermost `{ }` around `at` that is not a template's `${ }`, as [from, to), or null at the top level. */
function blockAround(skel, at) {
  let level = 0;
  for (let i = at - 1; i >= 0; i -= 1) {
    const c = skel[i];
    if (c === '}') level += 1;
    else if (c === '{') {
      if (level > 0) level -= 1;
      else if (skel[i - 1] !== '$') {
        const close = closing(skel, i);
        return close < 0 ? null : [i, close + 1];
      }
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// The controls, then every verifier
// ---------------------------------------------------------------------------

/**
 * The fixtures, held at the top level and not inside controls(): a function with
 * these strings in it would itself read as a helper, since they say FAIL.
 */
const LEXED = "a('//x'); // process.exitCode = 1\nb = /'/.test(c); /* process.exit(1) */ d = `${'}'}//`;\n";
/** A check() that only counts, with the missing line in a comment; a fail() that calls it; a gate. */
const PLANTED = [
  'let failures = 0;',
  'function check(ok, msg) {',
  "  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${msg}`);",
  '  if (!ok) failures += 1; // process.exitCode = 1',
  '}',
  'const fail = (msg) => { console.error(`FAIL: ${msg}`); check(false, msg); throw new Error(msg); };',
  'async function main() {',
  "  check(1 === 2, 'x');",
  '  if (failures > 0) { console.error(`FAIL: ${failures} assertion(s) failed`); process.exitCode = 1; }',
  '}',
].join('\n');
/** A gate whose early return prints FAIL and sets nothing: the search verifiers' shape. */
const INLINE_RETURN = [
  'let failures = 0;',
  'function check(ok, msg) {',
  "  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${msg}`);",
  '  if (!ok) { failures += 1; process.exitCode = 1; }',
  '}',
  'async function main() {',
  "  if (!(await up())) { console.error('FAIL: the service never came up'); return; }",
  '  if (failures > 0) console.error(`FAIL: ${failures} assertion(s) failed`);',
  '}',
].join('\n');
/** A fail() that exits 1, its FAIL after an escaped newline as live-fanout's fatal() has it. */
const EXITS = 'function fail(m) {\n  console.error(`\\nFAIL: ${m}`);\n  process.exit(1);\n}\n';

function controls() {
  console.log('');
  console.log('  the detector reads what it is meant to read');

  // Comments go, strings stay, and a quote or slash inside a regex or template is not a delimiter.
  const sample = LEXED;
  const { code, skel } = lex(sample);
  const spaces = (s) => ' '.repeat(s.length);
  const expected = sample.replace('// process.exitCode = 1', spaces('// process.exitCode = 1')).replace('/* process.exit(1) */', spaces('/* process.exit(1) */'));
  check(
    code === expected,
    `CONTROL: comments are blanked, and strings, a regex holding a quote and a template holding a brace are kept (${JSON.stringify(code)})`,
  );
  check(
    !/exit/.test(skel) && !/\/\//.test(skel) && skel.length === sample.length,
    `CONTROL: and skel blanks the literals' contents too, so nothing inside one is read as code (${JSON.stringify(skel)})`,
  );

  // A check() that only counts, with the missing line in a comment, is red; with the line, green.
  const planted = PLANTED;
  const red = judge(planted);
  const shape = (r) => `${r.helpers.map((h) => `${h.name}:${h.sets ? 'sets' : 'never'}`).join(', ')}; gates ${r.gates.map((g) => g.name).join(', ') || 'none'}`;
  check(
    red.helpers.length === 2 && red.helpers.every((h) => !h.sets) && red.gates.length === 1 && red.gates[0].name === 'main',
    `CONTROL: a planted check() that only counts, and a fail() that calls it, are both judged to never set the code; main() is the gate (${shape(red)})`,
  );
  const green = judge(planted.replace('if (!ok) failures += 1; // process.exitCode = 1', 'if (!ok) { failures += 1; process.exitCode = 1; }'));
  check(
    green.helpers.length === 2 && green.helpers.every((h) => h.sets) && green.helpers.find((h) => h.name === 'fail')?.via === 'check',
    `CONTROL: with the line added, check() sets it and fail() sets it through check() (${shape(green)})`,
  );
  const exits = judge(EXITS);
  // (No message in this function says the marker itself: a string here that did would make it read as a helper.)
  check(exits.helpers.length === 1 && exits.helpers[0].sets, `CONTROL: a fail() that exits 1 sets the code, its marker after an escaped newline (${shape(exits)})`);

  // An early return in a gate that prints the marker and sets nothing is red; with the line, green.
  const sites = (r) => r.inline.map((s) => `line ${s.line} ${s.ok ? 'sets' : 'never'}`).join(', ');
  const returns = judge(INLINE_RETURN);
  check(
    returns.inline.length === 2 && !returns.inline[0].ok && returns.inline[1].ok,
    `CONTROL: a gate's early return that prints the marker and sets nothing is red, and the gate's own report is not (${sites(returns)})`,
  );
  const fixed = judge(INLINE_RETURN.replace("never came up'); return;", "never came up'); process.exitCode = 1; return;"));
  check(fixed.inline.length === 2 && fixed.inline.every((s) => s.ok), `CONTROL: with the line added, both pass (${sites(fixed)})`);
}

function main() {
  const root = process.argv[2] ? resolve(process.argv[2]) : ROOT;
  const dir = join(root, 'scripts');
  controls();

  console.log('');
  console.log(`  every verifier in ${dir}: each helper that records a failure sets the exit code`);
  const files = readdirSync(dir)
    .filter((f) => /^verify-.*\.mjs$/.test(f))
    .sort();
  check(
    files.length > 90 && files.includes(THIS_FILE),
    `CONTROL: ${files.length} verifiers read from scripts/verify-*.mjs, this one among them`,
  );

  let withHelper = 0;
  let helperCount = 0;
  let gateCount = 0;
  let inlineCount = 0;
  const without = [];
  for (const file of files) {
    const src = readFileSync(join(dir, file), 'utf8');
    const { helpers, gates, inline, exits } = judge(src);
    gateCount += gates.length;
    inlineCount += inline.length;
    const unset = inline.filter((s) => !s.ok).map((s) => s.line);
    const outside = unset.length
      ? `line(s) ${unset.join(', ')} print FAIL outside a helper, and nothing in that block sets the exit code`
      : inline.length
        ? `${inline.length} FAIL(s) printed outside a helper set it too`
        : '';
    if (helpers.length === 0) {
      without.push(file);
      if (file in INLINE) {
        check(
          exits && unset.length === 0,
          `${file}: no helper; ${INLINE[file]}${exits ? '' : ' -- but nothing in it exits non-zero'}${outside ? `; ${outside}` : ''}`,
        );
      }
      continue;
    }
    withHelper += 1;
    helperCount += helpers.length;
    if (file in INLINE) check(false, `${file} is listed in INLINE and has a helper; take it off the list so its helpers are judged`);
    const never = helpers.filter((h) => !h.sets);
    const named = helpers.map((h) => `${h.name}() at line ${h.line}${h.via ? ` (through ${h.via}())` : ''}`).join(', ');
    check(
      never.length === 0 && unset.length === 0,
      never.length === 0 && unset.length === 0
        ? `${file}: ${named} set${helpers.length === 1 ? 's' : ''} the exit code${outside ? `; ${outside}` : ''}`
        : `${file}: ${[
            ...never.map((h) => `${h.name}() at line ${h.line} records a failure and never sets process.exitCode, so a FAIL it prints exits 0 on any path that skips the gate; set it where the failure is recorded`),
            ...(unset.length ? [outside] : []),
          ].join('; ')}`,
    );
  }
  const unlisted = without.filter((f) => !(f in INLINE));
  const stale = Object.keys(INLINE).filter((f) => !files.includes(f));
  check(
    unlisted.length === 0,
    unlisted.length === 0
      ? `CONTROL: a helper was found in ${withHelper} of ${files.length} verifiers (${helperCount} helpers); the ${without.length} without one are the ${Object.keys(INLINE).length} listed in INLINE`
      : `no failure-recording helper found in ${unlisted.join(', ')}. Give it a check() that sets process.exitCode = 1, or list it in INLINE with how it records a failure`,
  );
  check(stale.length === 0, stale.length === 0 ? 'CONTROL: every file INLINE names exists' : `INLINE names files that do not exist: ${stale.join(', ')}`);
  console.log(`    note  ${gateCount} end-of-run gate(s) print FAIL after reading a helper's count, and are not judged as helpers`);
  console.log(`    note  ${inlineCount} FAIL(s) printed outside a helper were judged by the block they print in`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every verifier sets its exit code where an assertion fails, not only in a gate at the end');
}

main();
