// PostgreSQL's system catalog for the tables Vectro serves (pg.mjs): the
// pg_catalog and information_schema tables clients read to list tables and
// columns, built as Vectro tables so plain catalog SQL runs on them; exact
// answers for the long metadata queries drivers send (Npgsql's type loading
// for Power BI, pgJDBC's getTables / getColumns for Tableau and others),
// recognised by their text; and for any other catalog query, an empty
// answer with its column names (no keys, no indexes, no comments).
import { columnType } from '../sql.mjs';
import { TYPE_OF, OID } from './pgtypes.mjs';

export const NS = { pg_catalog: 11, public: 2200, information_schema: 13000 };
const OWNER = 10, FIRST_TABLE = 16384;

// [name, oid, length, category, array oid] of the types the catalog lists
const BASE_TYPES = [
  ['bool', 16, 1, 'B', 1000], ['bytea', 17, -1, 'U', 1001], ['char', 18, 1, 'Z', 1002], ['name', 19, 64, 'S', 1003],
  ['int8', 20, 8, 'N', 1016], ['int2', 21, 2, 'N', 1005], ['int4', 23, 4, 'N', 1007], ['regproc', 24, 4, 'N', 1008],
  ['text', 25, -1, 'S', 1009], ['oid', 26, 4, 'N', 1028], ['json', 114, -1, 'U', 199], ['xml', 142, -1, 'U', 143],
  ['float4', 700, 4, 'N', 1021], ['float8', 701, 8, 'N', 1022], ['money', 790, 8, 'N', 791], ['bpchar', 1042, -1, 'S', 1014],
  ['varchar', 1043, -1, 'S', 1015], ['date', 1082, 4, 'D', 1182], ['time', 1083, 8, 'D', 1183], ['timestamp', 1114, 8, 'D', 1115],
  ['timestamptz', 1184, 8, 'D', 1185], ['interval', 1186, 16, 'T', 1187], ['timetz', 1266, 12, 'D', 1270], ['bit', 1560, -1, 'V', 1561],
  ['varbit', 1562, -1, 'V', 1563], ['numeric', 1700, -1, 'N', 1231], ['regclass', 2205, 4, 'N', 2210], ['regtype', 2206, 4, 'N', 2211],
  ['uuid', 2950, 16, 'U', 2951], ['jsonb', 3802, -1, 'U', 3807],
];
const PSEUDO_TYPES = [['record', 2249, -1, 'P', 2287], ['void', 2278, 4, 'P', 0], ['unknown', 705, -2, 'X', 0]];
export const TYPES = [
  ...BASE_TYPES.map(([name, oid, len, cat, arr]) => ({ name, oid, len, cat, arr, typtype: 'b', elem: 0 })),
  ...BASE_TYPES.map(([name, oid, , , arr]) => ({ name: `_${name}`, oid: arr, len: -1, cat: 'A', arr: 0, typtype: 'b', elem: oid })),
  ...PSEUDO_TYPES.map(([name, oid, len, cat, arr]) => ({ name, oid, len, cat, arr, typtype: 'p', elem: 0 })),
];
// information_schema's names for our types
const SQL_NAMES = { int8: 'bigint', float8: 'double precision', bool: 'boolean', text: 'text', timestamp: 'timestamp without time zone', date: 'date' };

// The tables' description: [{ name, oid, rows, columns: [{ name, type (pg type) }] }]
export function describeTables(named) {
  return Object.entries(named).filter(([n]) => !n.includes('.')).map(([name, t], i) => ({
    name, oid: FIRST_TABLE + i, rows: t.length,
    columns: t.columnNames.map((c) => ({ name: c, type: TYPE_OF[columnType(t, c)] ?? 'float8' })),
  }));
}

// A Vectro table from column kinds and rows: spec = { col: 'text' | 'num' }
function table(engine, spec, rows) {
  const names = Object.keys(spec);
  return engine.table(Object.fromEntries(names.map((n, k) => [n, spec[n] === 'text'
    ? (rows.length ? rows.map((r) => (r[k] === null || r[k] === undefined ? '' : String(r[k]))) : { codes: new Uint32Array(0), labels: [] })
    : Float64Array.from(rows, (r) => (r[k] === null || r[k] === undefined ? NaN : typeof r[k] === 'boolean' ? +r[k] : r[k])),
  ])));
}
const cols = (s) => Object.fromEntries(s.split(/\s+/).filter(Boolean).map((x) => { const [n, k] = x.split(':'); return [n, k === 't' ? 'text' : 'num']; }));

// pg_catalog and information_schema as Vectro tables, keyed "schema.name".
export function catalogTables(engine, described, { database, user, settings }) {
  const T = {};
  const add = (key, spec, rows = []) => { T[key] = table(engine, cols(spec), rows); };
  const typeOid = (t) => OID[t] ?? OID.text;
  const schemas = [['pg_catalog', NS.pg_catalog], ['public', NS.public], ['information_schema', NS.information_schema]];
  add('pg_catalog.pg_namespace', 'oid nspname:t nspowner nspacl:t', schemas.map(([n, o]) => [o, n, OWNER, '']));
  add('pg_catalog.pg_class', 'oid relname:t relnamespace relkind:t relowner reltuples relpages relhasindex relpersistence:t relispartition reltype reloftype relam relnatts relchecks relhasrules relhastriggers relhassubclass relrowsecurity relforcerowsecurity relreplident:t relacl:t reloptions:t relpartbound:t reltablespace relfilenode',
    described.map((d) => [d.oid, d.name, NS.public, 'r', OWNER, d.rows, 0, 0, 'p', 0, 0, 0, 2, d.columns.length, 0, 0, 0, 0, 0, 0, 'd', '', '', '', 0, d.oid]));
  add('pg_catalog.pg_attribute', 'attrelid attname:t atttypid attnum attlen atttypmod attnotnull attisdropped atthasdef atthasmissing attidentity:t attgenerated:t attcollation attndims attstattarget attislocal attinhcount attbyval:t attalign:t attstorage:t',
    described.flatMap((d) => d.columns.map((c, k) => [d.oid, c.name, typeOid(c.type), k + 1, TYPES.find((x) => x.name === c.type)?.len ?? -1, -1, 0, 0, 0, 0, '', '', c.type === 'text' ? 100 : 0, 0, -1, 1, 0, '', 'd', 'p'])));
  add('pg_catalog.pg_type', 'oid typname:t typnamespace typowner typlen typbyval typtype:t typcategory:t typispreferred typisdefined typdelim:t typrelid typelem typarray typinput:t typoutput:t typreceive:t typsend:t typalign:t typstorage:t typnotnull typbasetype typtypmod typndims typcollation typdefault:t typacl:t',
    TYPES.map((t) => [t.oid, t.name, NS.pg_catalog, OWNER, t.len, t.len > 0 && t.len <= 8 ? 1 : 0, t.typtype, t.cat, 0, 1, ',', 0, t.elem, t.arr, t.elem ? 'array_in' : `${t.name}in`, t.elem ? 'array_out' : `${t.name}out`, t.elem ? 'array_recv' : `${t.name}recv`, t.elem ? 'array_send' : `${t.name}send`, 'd', 'p', 0, 0, -1, 0, t.name === 'text' || t.name === 'varchar' ? 100 : 0, '', '']));
  add('pg_catalog.pg_database', 'oid datname:t datdba encoding datlocprovider:t datistemplate datallowconn datconnlimit datcollate:t datctype:t daticulocale:t daticurules:t datcollversion:t datacl:t dattablespace datfrozenxid datminmxid', [[16000, database, OWNER, 6, 'c', 0, 1, -1, 'C', 'C', '', '', '', '', 1663, 0, 0]]);
  add('pg_catalog.pg_settings', 'name:t setting:t unit:t category:t short_desc:t extra_desc:t context:t vartype:t source:t min_val:t max_val:t enumvals:t boot_val:t reset_val:t sourcefile:t sourceline pending_restart',
    Object.entries(settings).map(([k, v]) => [k, v, '', 'Vectro', '', '', 'user', 'string', 'default', '', '', '', v, v, '', NaN, 0]));
  add('pg_catalog.pg_roles', 'oid rolname:t rolsuper rolinherit rolcreaterole rolcreatedb rolcanlogin rolreplication rolconnlimit rolpassword:t rolvaliduntil rolbypassrls rolconfig:t', [[OWNER, user, 0, 1, 0, 0, 1, 0, -1, '********', NaN, 0, '']]);
  add('pg_catalog.pg_user', 'usename:t usesysid usecreatedb usesuper userepl usebypassrls passwd:t valuntil useconfig:t', [[user, OWNER, 0, 0, 0, 0, '********', NaN, '']]);
  add('pg_catalog.pg_authid', 'oid rolname:t rolsuper rolcanlogin', [[OWNER, user, 0, 1]]);
  add('pg_catalog.pg_tables', 'schemaname:t tablename:t tableowner:t tablespace:t hasindexes hasrules hastriggers rowsecurity', described.map((d) => ['public', d.name, user, '', 0, 0, 0, 0]));
  add('pg_catalog.pg_am', 'oid amname:t amhandler:t amtype:t', [[2, 'heap', 'heap_tableam_handler', 't'], [403, 'btree', 'bthandler', 'i']]);
  add('pg_catalog.pg_tablespace', 'oid spcname:t spcowner spcacl:t spcoptions:t', [[1663, 'pg_default', OWNER, '', ''], [1664, 'pg_global', OWNER, '', '']]);
  add('pg_catalog.pg_proc', 'oid proname:t pronamespace proowner prolang prokind:t prorettype proargtypes:t proretset pronargs');
  add('pg_catalog.pg_views', 'schemaname:t viewname:t viewowner:t definition:t');
  add('pg_catalog.pg_matviews', 'schemaname:t matviewname:t matviewowner:t tablespace:t hasindexes ispopulated definition:t');
  add('pg_catalog.pg_description', 'objoid classoid objsubid description:t');
  add('pg_catalog.pg_shdescription', 'objoid classoid description:t');
  add('pg_catalog.pg_attrdef', 'oid adrelid adnum adbin:t');
  add('pg_catalog.pg_index', 'indexrelid indrelid indnatts indnkeyatts indisunique indisprimary indisexclusion indimmediate indisclustered indisvalid indisready indislive indkey:t indexprs:t indpred:t');
  add('pg_catalog.pg_constraint', 'oid conname:t connamespace contype:t condeferrable condeferred convalidated conrelid contypid conindid conparentid confrelid confupdtype:t confdeltype:t confmatchtype:t conislocal coninhcount connoinherit conkey:t confkey:t conbin:t');
  add('pg_catalog.pg_inherits', 'inhrelid inhparent inhseqno');
  add('pg_catalog.pg_enum', 'oid enumtypid enumsortorder enumlabel:t');
  add('pg_catalog.pg_range', 'rngtypid rngsubtype rngmultitypid rngcollation');
  add('pg_catalog.pg_extension', 'oid extname:t extowner extnamespace extrelocatable extversion:t');
  add('pg_catalog.pg_collation', 'oid collname:t collnamespace collowner collprovider:t collencoding collcollate:t collctype:t');
  add('pg_catalog.pg_trigger', 'oid tgrelid tgname:t tgfoid tgtype tgenabled:t tgisinternal');
  add('pg_catalog.pg_rewrite', 'oid rulename:t ev_class ev_type:t ev_enabled:t is_instead');
  add('pg_catalog.pg_sequence', 'seqrelid seqtypid seqstart seqincrement seqmax seqmin seqcache seqcycle');
  add('pg_catalog.pg_foreign_table', 'ftrelid ftserver ftoptions:t');
  add('pg_catalog.pg_partitioned_table', 'partrelid partstrat:t partnatts partdefid');
  for (const k of ['pg_stat_user_tables', 'pg_stat_all_tables']) add(`pg_catalog.${k}`, 'relid schemaname:t relname:t seq_scan seq_tup_read idx_scan idx_tup_fetch n_tup_ins n_tup_upd n_tup_del n_live_tup n_dead_tup n_mod_since_analyze last_vacuum last_autovacuum last_analyze last_autoanalyze',
    described.map((d) => [d.oid, 'public', d.name, 0, 0, 0, 0, 0, 0, 0, d.rows, 0, 0, NaN, NaN, NaN, NaN]));
  add('pg_catalog.pg_stat_activity', 'datid datname:t pid usename:t application_name:t client_addr:t state:t query:t');
  add('pg_catalog.pg_event_trigger', 'oid evtname:t evtevent:t evtenabled:t');
  add('pg_catalog.pg_publication', 'oid pubname:t');
  // information_schema
  const db = database;
  add('information_schema.schemata', 'catalog_name:t schema_name:t schema_owner:t default_character_set_catalog:t default_character_set_schema:t default_character_set_name:t sql_path:t', schemas.map(([n]) => [db, n, user, '', '', '', '']));
  add('information_schema.tables', 'table_catalog:t table_schema:t table_name:t table_type:t self_referencing_column_name:t reference_generation:t user_defined_type_catalog:t user_defined_type_schema:t user_defined_type_name:t is_insertable_into:t is_typed:t commit_action:t',
    described.map((d) => [db, 'public', d.name, 'BASE TABLE', '', '', '', '', '', 'NO', 'NO', '']));
  add('information_schema.columns', 'table_catalog:t table_schema:t table_name:t column_name:t ordinal_position column_default:t is_nullable:t data_type:t character_maximum_length character_octet_length numeric_precision numeric_precision_radix numeric_scale datetime_precision interval_type:t interval_precision character_set_catalog:t character_set_schema:t character_set_name:t collation_catalog:t collation_schema:t collation_name:t domain_catalog:t domain_schema:t domain_name:t udt_catalog:t udt_schema:t udt_name:t scope_catalog:t scope_schema:t scope_name:t maximum_cardinality dtd_identifier:t is_self_referencing:t is_identity:t identity_generation:t identity_start:t identity_increment:t identity_maximum:t identity_minimum:t identity_cycle:t is_generated:t generation_expression:t is_updatable:t',
    described.flatMap((d) => d.columns.map((c, k) => {
      const num = c.type === 'int8' || c.type === 'float8';
      return [db, 'public', d.name, c.name, k + 1, '', 'YES', SQL_NAMES[c.type] ?? c.type, NaN, c.type === 'text' ? 1073741824 : NaN, c.type === 'int8' ? 64 : c.type === 'float8' ? 53 : NaN, num ? 2 : NaN, c.type === 'int8' ? 0 : NaN, c.type === 'timestamp' ? 6 : c.type === 'date' ? 0 : NaN, '', NaN, '', '', '', '', '', '', '', '', '', db, 'pg_catalog', c.type, '', '', '', NaN, String(k + 1), 'NO', 'NO', '', '', '', '', '', 'NO', 'NEVER', '', 'NO'];
    })));
  add('information_schema.views', 'table_catalog:t table_schema:t table_name:t view_definition:t check_option:t is_updatable:t is_insertable_into:t');
  add('information_schema.table_constraints', 'constraint_catalog:t constraint_schema:t constraint_name:t table_catalog:t table_schema:t table_name:t constraint_type:t is_deferrable:t initially_deferred:t enforced:t');
  add('information_schema.key_column_usage', 'constraint_catalog:t constraint_schema:t constraint_name:t table_catalog:t table_schema:t table_name:t column_name:t ordinal_position position_in_unique_constraint');
  add('information_schema.referential_constraints', 'constraint_catalog:t constraint_schema:t constraint_name:t unique_constraint_catalog:t unique_constraint_schema:t unique_constraint_name:t match_option:t update_rule:t delete_rule:t');
  add('information_schema.constraint_column_usage', 'table_catalog:t table_schema:t table_name:t column_name:t constraint_catalog:t constraint_schema:t constraint_name:t');
  add('information_schema.routines', 'specific_catalog:t specific_schema:t specific_name:t routine_catalog:t routine_schema:t routine_name:t routine_type:t data_type:t');
  add('information_schema.parameters', 'specific_catalog:t specific_schema:t specific_name:t ordinal_position parameter_mode:t parameter_name:t data_type:t');
  add('information_schema.table_privileges', 'grantor:t grantee:t table_catalog:t table_schema:t table_name:t privilege_type:t is_grantable:t with_hierarchy:t',
    described.map((d) => [user, user, db, 'public', d.name, 'SELECT', 'NO', 'NO']));
  add('information_schema.column_privileges', 'grantor:t grantee:t table_catalog:t table_schema:t table_name:t column_name:t privilege_type:t is_grantable:t');
  add('information_schema.character_sets', 'character_set_catalog:t character_set_schema:t character_set_name:t character_repertoire:t form_of_use:t default_collate_catalog:t default_collate_schema:t default_collate_name:t', [['', '', 'UTF8', 'UCS', 'UTF8', db, 'pg_catalog', 'C']]);
  add('information_schema.sequences', 'sequence_catalog:t sequence_schema:t sequence_name:t data_type:t');
  add('information_schema.triggers', 'trigger_catalog:t trigger_schema:t trigger_name:t event_object_table:t');
  return T;
}

// ---- driver queries answered as they are -----------------------------------------------
// SQL LIKE pattern (with \ escapes) -> RegExp
function likeRegex(p) {
  let re = '';
  for (let i = 0; i < p.length; i++) {
    const c = p[i];
    if (c === '\\' && i + 1 < p.length) { re += p[++i].replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); continue; }
    re += c === '%' ? '[\\s\\S]*' : c === '_' ? '[\\s\\S]' : c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`^${re}$`);
}
// the pattern a query compares a name with (LIKE 'x' or = 'x'), or null for any
function patternFor(text, field) {
  const f = field.replace('.', '\\.');
  const where = text.slice(Math.max(0, text.search(/\bWHERE\b/i))); // (the filters, not a CASE before them)
  const m = new RegExp(`${f}\\s+LIKE\\s+E?'((?:[^']|'')*)'`, 'i').exec(text) ?? new RegExp(`${f}\\s+=\\s+E?'((?:[^']|'')*)'`, 'i').exec(where);
  if (!m) return null;
  const v = m[1].replaceAll("''", "'");
  return /LIKE/i.test(m[0]) ? likeRegex(v) : new RegExp(`^${v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`);
}
const ok = (re, s) => !re || re.test(s);
const result = (spec, rows) => ({ columns: spec.split(/\s+/).filter(Boolean).map((x) => { const [name, type] = x.split(':'); return { name, type }; }), rows });

// A known driver query -> { columns: [{ name, type }], rows }, or null.
export function driverAnswer(text, described, database = 'vectro') {
  if (/\/\*\*\* Load all supported types \*\*\*\//.test(text)) { // Npgsql (Power BI): every type it may meet
    const rows = [];
    for (const t of TYPES) {
      if (t.typtype === 'p') { if (t.name === 'record' || t.name === 'void') rows.push(['pg_catalog', t.name, t.oid, 0, 0, 'p', 0, 0]); continue; }
      if (t.elem) rows.push(['pg_catalog', t.name, t.oid, 0, 0, 'a', t.elem, 3]);
      else rows.push(['pg_catalog', t.name, t.oid, 0, 0, 'b', 0, 0]);
    }
    rows.sort((a, b) => a[7] - b[7]);
    return result('nspname:name typname:name oid:oid typrelid:oid typbasetype:oid type:char elemoid:oid ord:int4', rows);
  }
  if (/\/\*\*\* Load field definitions for \(free-standing\) composite types \*\*\*\//.test(text)) return result('oid:oid attname:name atttypid:oid', []);
  if (/\/\*\*\* Load enum fields \*\*\*\//.test(text)) return result('oid:oid enumlabel:name', []);
  // pgJDBC (Tableau, DBeaver, Metabase): DatabaseMetaData
  const schemas = /AS "?TABLE_SCHEM"?,\s*(NULL|current_database\(\)) AS "?TABLE_CATALOG"?\s+FROM pg_catalog\.pg_namespace/i.exec(text);
  if (schemas) { // (older pgJDBC: NULL as the catalog; newer: current_database() and quoted upper-case names)
    const re = patternFor(text, 'nspname'), quoted = /AS "TABLE_SCHEM"/.test(text);
    return result(`${quoted ? 'TABLE_SCHEM' : 'table_schem'}:name ${quoted ? 'TABLE_CATALOG' : 'table_catalog'}:${quoted ? 'name' : 'text'}`, ['information_schema', 'pg_catalog', 'public'].filter((x) => ok(re, x)).map((x) => [x, /NULL/i.test(schemas[1]) ? null : database]));
  }
  if (/AS TABLE_TYPE,\s*d\.description AS REMARKS/i.test(text)) {
    const sre = patternFor(text, 'n.nspname'), tre = patternFor(text, 'c.relname');
    const wantTables = /c\.relkind = 'r'/.test(text) || !/c\.relkind/.test(text);
    const rows = wantTables && ok(sre, 'public') ? described.filter((d) => ok(tre, d.name)).map((d) => [null, 'public', d.name, 'TABLE', null, '', '', '', '', '']) : [];
    return result('table_cat:text table_schem:name table_name:name table_type:text remarks:text type_cat:text type_schem:text type_name:text self_referencing_col_name:text ref_generation:text', rows);
  }
  if (/row_number\(\) OVER \(PARTITION BY a\.attrelid ORDER BY a\.attnum\) AS attnum/i.test(text)) {
    const sre = patternFor(text, 'n.nspname'), tre = patternFor(text, 'c.relname'), cre = patternFor(text, 'attname');
    const rows = [];
    if (ok(sre, 'public')) {
      for (const d of described) {
        if (!ok(tre, d.name)) continue;
        d.columns.forEach((c, k) => { if (ok(cre, c.name)) rows.push(['public', d.name, c.name, OID[c.type], false, -1, TYPES.find((x) => x.name === c.type)?.len ?? -1, -1, k + 1, null, null, null, null, 0, 'b']); });
      }
    }
    return result('nspname:name relname:name attname:name atttypid:oid attnotnull:bool atttypmod:int4 attlen:int2 typtypmod:int4 attnum:int8 attidentity:char attgenerated:char adsrc:text description:text typbasetype:oid typtype:char', rows);
  }
  // psql's \d name and \d+ name: the table's columns (its columns differ between psql versions:
  // each item is answered by what it reads; defaults, collations, comments... are empty)
  const att = /^\s*SELECT a\.attname,\s*pg_catalog\.format_type\(a\.atttypid,\s*a\.atttypmod\)[\s\S]*\bFROM pg_catalog\.pg_attribute a\s+WHERE a\.attrelid = '(\d+)'/i.exec(text);
  if (att) {
    const items = selectItems(text), names = selectNames(text), d = described.find((x) => x.oid === Number(att[1]));
    const value = (item, c) => (item === 'a.attname' ? c.name : /format_type/.test(item) ? SQL_NAMES[c.type] ?? c.type
      : item === 'a.attnotnull' ? false : item === 'a.attstorage' ? (c.type === 'text' ? 'x' : 'p') : null);
    return result(items.map((item, k) => `${names[k]}:${item === 'a.attnotnull' ? 'bool' : 'text'}`).join(' '), (d?.columns ?? []).map((c) => items.map((item) => value(item, c))));
  }
  // psql's \d name, \d+ name: about the table (a plain table: no checks, rules, options...)
  const rel = /^\s*SELECT c\.relchecks, c\.relkind, c\.relhasindex[\s\S]*\bWHERE c\.oid = '(\d+)'\s*$/i.exec(text);
  if (rel) {
    const items = selectItems(text), names = selectNames(text);
    const value = (item) => (/relchecks|reltablespace/.test(item) ? 0 : /relhas|rowsecurity|relispartition/.test(item) ? false
      : item === 'c.relkind' ? 'r' : item === 'c.relpersistence' ? 'p' : item === 'c.relreplident' ? 'd' : item === 'am.amname' ? 'heap' : '');
    const type = (v) => (typeof v === 'number' ? 'int4' : typeof v === 'boolean' ? 'bool' : 'text');
    const row = items.map(value);
    return result(names.map((n, k) => `${n}:${type(row[k])}`).join(' '), described.some((d) => d.oid === Number(rel[1])) ? [row] : []);
  }
  if (/SELECT t\.typname,\s*t\.oid FROM pg_catalog\.pg_type t\s+JOIN pg_catalog\.pg_namespace n/i.test(text)) {
    return result('typname:name oid:oid', TYPES.map((t) => [t.name, t.oid]));
  }
  if (/typinput\s*=\s*'pg_catalog\.array_in'::regproc as is_array/i.test(text)) {
    const byName = /typname\s*=\s*'([^']*)'/i.exec(text)?.[1], byOid = /pg_type\.oid\s*=\s*(\d+)/i.exec(text)?.[1];
    const rows = TYPES.filter((t) => (byName === undefined || t.name === byName) && (byOid === undefined || t.oid === Number(byOid))).map((t) => [!!t.elem, t.typtype, t.name, t.oid]);
    return result('is_array:bool typtype:char typname:name oid:oid', rows);
  }
  return null;
}

// Does a query read the catalog (pg_catalog, information_schema, pg_* tables)?
export const readsCatalog = (text) => /\b(pg_catalog|information_schema)\s*\.|\bpg_[a-z_]+\b/i.test(text);

// psql's way of writing operators and collations in catalog queries (\d name):
// OPERATOR(pg_catalog.~) is plain ~, and COLLATE pg_catalog.default changes nothing.
export const plainOperators = (text) => text
  .replace(/\bOPERATOR\s*\(\s*(?:pg_catalog\s*\.\s*)?([~!=<>*]+)\s*\)/gi, ' $1 ')
  .replace(/\s+COLLATE\s+(?:pg_catalog\s*\.\s*)?"?default"?/gi, '');

// The names of a SELECT's columns, read roughly from its text (for an empty answer).
export function selectNames(text) {
  return selectItems(text).map((s) => {
    const as = /\bas\s+("?)([^"\s]+)\1\s*$/i.exec(s) ?? /("?)([\p{L}_][\p{L}\p{M}\p{N}_$]*)\1\s*$/u.exec(s);
    if (!as) return '?column?';
    return as[1] ? as[2] : as[2].toLowerCase();
  });
}

// A SELECT's items, as text.
function selectItems(text) {
  const t = text.replace(/--[^\n]*|\/\*[\s\S]*?\*\//g, ' ');
  const start = /\bselect\b/i.exec(t);
  if (!start) return [];
  let depth = 0, i = start.index + 6, item = '', end = t.length;
  const items = [];
  for (; i < t.length; i++) {
    const c = t[i];
    if (c === "'") { const j = t.indexOf("'", i + 1); item += t.slice(i, j < 0 ? t.length : j + 1); i = j < 0 ? t.length : j; continue; }
    if (c === '"') { const j = t.indexOf('"', i + 1); item += t.slice(i, j < 0 ? t.length : j + 1); i = j < 0 ? t.length : j; continue; }
    if (c === '(') depth++;
    if (c === ')') depth--;
    if (depth === 0 && /\bfrom\b/i.test(t.slice(i, i + 5)) && /\s/.test(t[i - 1] ?? ' ') && /\s/.test(t[i + 4] ?? ' ')) { end = i; break; }
    if (depth === 0 && c === ',') { items.push(item); item = ''; continue; }
    item += c;
  }
  if (end <= t.length) items.push(item);
  return items.map((s) => s.trim()).filter(Boolean);
}
