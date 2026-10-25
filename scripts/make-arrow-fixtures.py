# Writes test/fixtures/arrow/*: small Arrow files made by pyarrow (the
# reference implementation) plus expected.json, the values Vectro should read
# from them. Needs pyarrow and pandas:
#   python3 -m venv /tmp/pyenv && /tmp/pyenv/bin/pip install pyarrow pandas
#   /tmp/pyenv/bin/python scripts/make-arrow-fixtures.py
import datetime as dt, json, math, os, random
import pyarrow as pa, pyarrow.ipc as ipc, pyarrow.feather as feather
import pandas as pd

OUT = os.path.join(os.path.dirname(__file__), '..', 'test', 'fixtures', 'arrow')
os.makedirs(OUT, exist_ok=True)
random.seed(5)
N = 300
MS = {'s': 1000, 'ms': 1, 'us': 1e-3, 'ns': 1e-6}

def maybe(v, p=0.1):
    return None if random.random() < p else v

def ints(lo, hi):
    return [maybe(random.randint(lo, hi)) for _ in range(N)]

labels = ['EU', 'US', 'APAC', 'Zürich', '日本', '', 'say "hi"']
cols = {
    'i8': pa.array(ints(-128, 127), pa.int8()),
    'i16': pa.array(ints(-32768, 32767), pa.int16()),
    'i32': pa.array(ints(-2**31, 2**31 - 1), pa.int32()),
    'i64': pa.array(ints(-2**53, 2**53), pa.int64()),
    'u8': pa.array(ints(0, 255), pa.uint8()),
    'u16': pa.array(ints(0, 65535), pa.uint16()),
    'u32': pa.array(ints(0, 2**32 - 1), pa.uint32()),
    'u64': pa.array(ints(0, 2**53), pa.uint64()),
    'f16': pa.array([maybe(random.choice([0.5, -2.0, 1.5, 65504.0, 0.0001, -0.0])) for _ in range(N)], pa.float16()),
    'f32': pa.array([maybe(random.choice([0.25, -1.5, 3.0, 1e10, float('inf')])) for _ in range(N)], pa.float32()),
    'f64': pa.array([maybe(random.uniform(-1e6, 1e6)) for _ in range(N)] , pa.float64()),
    'flag': pa.array([maybe(random.random() < 0.5) for _ in range(N)], pa.bool_()),
    'day': pa.array([maybe(dt.date(2020, 1, 1) + dt.timedelta(days=random.randint(-5000, 5000))) for _ in range(N)], pa.date32()),
    'day64': pa.array([maybe(random.randint(-10**12, 10**12) // 86400000 * 86400000) for _ in range(N)], pa.date64()),
    't32s': pa.array(ints(0, 86399), pa.time32('s')),
    't32ms': pa.array(ints(0, 86399999), pa.time32('ms')),
    't64us': pa.array(ints(0, 86399999999), pa.time64('us')),
    't64ns': pa.array(ints(0, 86399999999999), pa.time64('ns')),
    'ts_s': pa.array(ints(0, 2**31), pa.timestamp('s')),
    'ts_ms': pa.array(ints(0, 2**41), pa.timestamp('ms', tz='UTC')),
    'ts_us': pa.array(ints(0, 2**50), pa.timestamp('us')),
    'ts_ns': pa.array(ints(0, 2**53), pa.timestamp('ns', tz='Europe/Zurich')),
    'dur': pa.array(ints(-10**9, 10**9), pa.duration('ms')),
    'nothing': pa.nulls(N),
    'text': pa.array([maybe(random.choice(labels)) for _ in range(N)], pa.string()),
    'big_text': pa.array([maybe(random.choice(labels)) for _ in range(N)], pa.large_string()),
    'region': pa.array([maybe(random.choice(labels)) for _ in range(N)], pa.string()).dictionary_encode(),
    'small_dict': pa.DictionaryArray.from_arrays(pa.array([random.randint(0, 2) for _ in range(N)], pa.int8()), pa.array(['a', 'b', 'c'])),
    'num_dict': pa.DictionaryArray.from_arrays(pa.array([maybe(random.randint(0, 3)) for _ in range(N)], pa.int16()), pa.array([10, 20, 30, 40], pa.int64())),
    'tags': pa.array([[1, 2], None, [3]] * (N // 3), pa.list_(pa.int32())),
    'point': pa.array([{'x': 1, 'y': 'a'}] * N, pa.struct([('x', pa.int32()), ('y', pa.string())])),
}
table = pa.table(cols)

def expected_values(arr):
    t = arr.type
    if pa.types.is_dictionary(t):
        return expected_values(arr.cast(t.value_type))
    out = []
    if pa.types.is_null(t):
        return [None] * len(arr)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return ['' if v is None else v for v in arr.to_pylist()]
    if pa.types.is_boolean(t):
        return [None if v is None else int(v) for v in arr.to_pylist()]
    if pa.types.is_date32(t):
        raw = arr.cast(pa.int32()).to_pylist()
        return [None if v is None else v * 86400000 for v in raw]
    if pa.types.is_date64(t):
        return arr.cast(pa.int64()).to_pylist()
    if pa.types.is_time(t) or pa.types.is_timestamp(t) or pa.types.is_duration(t):
        raw = arr.cast(pa.int32() if t in (pa.time32('s'), pa.time32('ms')) else pa.int64()).to_pylist()
        u = t.unit
        return [None if v is None else (v * 1000 if u == 's' else v if u == 'ms' else v / 1000 if u == 'us' else v / 1e6) for v in raw]
    return [None if v is None else v for v in arr.to_pylist()]

def json_safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return 'NaN' if math.isnan(v) else ('Infinity' if v > 0 else '-Infinity')
    return v

readable = [c for c in table.column_names if c not in ('tags', 'point')]
expected = {'columns': {c: [json_safe(v) for v in expected_values(table.column(c).combine_chunks())] for c in readable},
            'kinds': {c: ('category' if c in ('text', 'big_text', 'region', 'small_dict') else 'number') for c in readable}}

# stream, several batches
with ipc.new_stream(os.path.join(OUT, 'types.arrow'), table.schema) as w:
    for b in table.to_batches(max_chunksize=70):
        w.write_batch(b)
feather.write_feather(table, os.path.join(OUT, 'types.feather'), compression='uncompressed', chunksize=128)
feather.write_feather(table, os.path.join(OUT, 'types-lz4.feather'), compression='lz4', chunksize=128)
feather.write_feather(pa.table({'x': pa.array([1.0, 2.0])}), os.path.join(OUT, 'zstd.feather'), compression='zstd')

# dictionaries that grow (delta) and that are replaced, in streams
def dict_batches():
    out, seen = [], []
    for step in range(4):
        new = [f'label{step}_{i}' for i in range(3)]
        vocab = seen + new
        seen = vocab
        idx = [random.randint(0, len(vocab) - 1) for _ in range(20)]
        out.append(pa.record_batch({'k': pa.DictionaryArray.from_arrays(pa.array(idx, pa.int32()), pa.array(vocab)), 'v': pa.array(list(range(step * 20, step * 20 + 20)), pa.float64())}))
    return out
batches = dict_batches()
schema = batches[0].schema
with ipc.new_stream(os.path.join(OUT, 'delta.arrow'), schema, options=ipc.IpcWriteOptions(emit_dictionary_deltas=True)) as w:
    for b in batches: w.write_batch(b)
replaced = [pa.record_batch({'k': pa.DictionaryArray.from_arrays(pa.array([0, 1, 1, 0], pa.int32()), pa.array(['x', 'y'])), 'v': pa.array([1.0, 2, 3, 4])}),
            pa.record_batch({'k': pa.DictionaryArray.from_arrays(pa.array([0, 1, 2], pa.int32()), pa.array(['y', 'z', 'x'])), 'v': pa.array([5.0, 6, 7])})]
with ipc.new_stream(os.path.join(OUT, 'replaced.arrow'), replaced[0].schema) as w:
    for b in replaced: w.write_batch(b)
expected['delta'] = {'k': sum((b.column(0).cast(pa.string()).to_pylist() for b in batches), []), 'v': sum((b.column(1).to_pylist() for b in batches), [])}
expected['replaced'] = {'k': ['x', 'y', 'y', 'x', 'y', 'z', 'x'], 'v': [1, 2, 3, 4, 5, 6, 7]}

# pandas: categorical, object strings, datetimes, with its default (LZ4) compression
df = pd.DataFrame({
    'city': pd.Categorical(['Paris', 'Oslo', 'Paris', None, 'Rome'] * 40),
    'name': ['ann', 'bob', None, 'cy', 'dee'] * 40,
    'when': pd.to_datetime(format='ISO8601', arg=['2024-01-01T00:00:01.5', '2024-06-30', None, '1999-12-31T23:59:59', '2030-01-01'] * 40),
    'amount': [1.5, None, 3.25, -4.0, 1e300] * 40,
    'count': pd.array([1, 2, None, 4, 5] * 40, dtype='Int64'),
})
df.to_feather(os.path.join(OUT, 'pandas.feather'))
pt = pa.Table.from_pandas(df, preserve_index=False)
expected['pandas'] = {c: [json_safe(v) for v in expected_values(pt.column(c).combine_chunks())] for c in df.columns}

with open(os.path.join(OUT, 'expected.json'), 'w') as f:
    json.dump(expected, f, ensure_ascii=False)
print('wrote', sorted(os.listdir(OUT)))
