# Proposal: Evolve `fleet-midi` into a fleet-wide, multi-rate sensor event bus

**Status:** Draft for review
**Branch:** `research-fleet-event-bus-2026-07-10`
**Author:** Research pass — grounded in the actual `production-round3-2026-07-10`
implementation of `fleet-midi` and a survey of the storage candidates named by
the maintainer.
**Date:** 2026-07-10

---

## 0. What `fleet-midi` actually is today (ground truth)

I read the source rather than trusting descriptions. Two states of the repo
exist:

- **`master` / `src/lib.rs`** is a 19-line stub (`pub fn hello()` + one test).
- **`production-round3-2026-07-10` / `src/lib.rs`** (404 lines, 12 passing
  tests) is the *real* implementation. Everything below refers to **this**
  branch, because that is where the design described in the brief actually
  lives.

That real implementation gives us:

- A closed `MidiMessage` enum: `NoteOn`, `NoteOff`, `ControlChange`,
  `PitchBend`, `ProgramChange`, each carrying a `channel: u8` (0–15).
- `parse_midi_byte(&mut ParserState, byte) -> Option<MidiMessage>` — a
  byte-level state machine with **running status** (a status byte sent once is
  reused for subsequent bare data bytes; halves bandwidth for continuous
  streams). Velocity-zero note-ons are normalized to `NoteOff`.
- `parse_midi_slice(bytes) -> Vec<MidiMessage>`.
- `FleetBroadcaster { subscriptions: HashMap<u8, Vec<String>> }` mapping a MIDI
  channel → subscribed agent IDs, with `subscribe(channel, agent_id)` and
  `broadcast(&msg) -> Vec<String>`.

The defining property to preserve: **every message is 1–3 fixed bytes, the
length is fully determined by the status byte, and parse/route is O(1).** That
is the entire reason MIDI is sub-millisecond. Any proposal that erodes this for
the *control* path has to justify the cost.

> Note on the `openconstruct-esp32` reference: the brief speculated that repo
> establishes a `[0xAA][0x55][len][type][payload][crc8]` binary framing as a
> house style. It does **not** — that repo is a text-command-over-MQTT protocol
> (`status`, `ping`, `read <sensor>`, `write <pin> <value>`). So there is no
> pre-existing binary framing to inherit; the framing proposed in §6 is a new
> design, but it is a natural extension of `fleet-midi`'s existing status/data
> byte discipline rather than something borrowed.

---

## 1. Problem statement

The maintainer (a commercial fisherman) wants to move from "route musical
trigger messages between agents on one machine" to "carry **any** timestamped,
location-stamped sensor event from any device on the boat, and eventually sync
and query them **across a fleet of boats**." In their words: depth-sounder
waveforms, ~0.5 Hz radar sweeps, bite-detector triggers, camera footage — plus
fleet-wide synoptic queries ("everyone's depth right now", "catch rate across
the fleet over the last hour").

The hard part is that the event types the system must now carry have violently
different shapes and rates:

| Event type | Size | Rate | Latency need | Shape |
|---|---|---|---|---|
| Bite-detector trigger | ~bytes | sparse, bursty | **sub-ms, fire-and-forget** | scalar flag |
| Simple sensor reading (temp, depth scalar) | small | ~1–10 Hz | low | scalar |
| Depth-sounder waveform | medium–large | ~1–10 Hz | moderate | float array (a *signal*) |
| Radar sweep | large, dense | ~0.5 Hz | moderate | dense 2D polar/range-doppler array |
| Camera footage / clip | **very large blobs** | variable | async | opaque binary, **cannot use 1–3 byte framing at all** |

And a new requirement that MIDI was never asked to meet: a **durable,
indexed, queryable store** keyed by `(timestamp, location, event_type,
source_vessel)` so that synoptic fleet questions can be answered — not just
real-time fan-out.

**This is the central tension:** MIDI's whole design is optimized for tiny,
fixed-size, ultra-low-latency messages. Camera footage and radar sweeps are
neither tiny nor fixed-size. Forcing a 50 MB video clip through a format whose
proudest feature is "every message is 3 bytes" would be a category error.

---

## 2. The architectural question: one system or two?

### The diagnosis

The four event families above are not one workload. They are two workloads
with opposite engineering optima, sharing only an *addressing scheme*
(timestamp + location + type + source):

1. **Real-time control/trigger plane** — tiny, frequent-to-sparse, latency-
   critical, ephemeral (fire-and-forget). This is exactly what `fleet-midi`
   already does well. Bite detectors, simple readings, agent coordination
   pulses belong here.
2. **Durable signal/archive plane** — large, dense, queryable, latency-tolerant,
   must survive across reboots and across boats. Depth waveforms, radar sweeps,
   camera footage, and *all* history belong here.

### Recommendation: **two tiers, one shared envelope** (a "dual-bus" architecture)

Not one unified system, and not two unrelated systems. Concretely:

- **Tier 1 — `fleet-events` (the real-time bus), grown from `fleet-midi`.**
  Keep the byte-level state machine and O(1) parse. Generalize the message
  format: a **variable `event_type` byte + variable-length small payload**
  (capped, e.g. ≤ 255 bytes inline), while preserving the status/data-bit-7
  discipline and running-status compression that make it fast. Timestamp +
  location are carried as a periodic **context frame** (see §5) that downstream
  events implicitly inherit — directly analogous to running status, but for
  spatial-temporal context. Pure pub/sub, in-memory, ephemeral. Anything that
  fits in the cap and needs low latency goes here.
- **Tier 2 — the durable store (DuckDB + Parquet, local-first; see §3).**
  Everything is also written here. Small readings are written as rows; large
  blobs are written to a content-addressed object/file store with only a
  **manifest reference** (offset, length, checksum, codec) stored as a Parquet
  row. Synoptic and historical queries run here.
- **The contract between them: the shared event envelope** (§5). Both tiers
  speak the same `(timestamp, lat, lon, vessel_id, event_type, payload|ref)`
  tuple, so an event can flow on the bus *and* be persisted, and the same
  schema serves a live subscriber and a historical query.

### Why not force everything through one system?

Three honest reasons:

1. **Latency.** A 2-second radar sweep (densely packed MBs) on the same
   queue as a bite detector would head-of-line block the detector unless you
   add priority queues, at which point you've reinvented two tiers anyway —
   just less cleanly.
2. **Wire format.** MIDI's framing has no notion of "here come 40 MB of
   bytes." You'd have to bolt on length-prefixed blob framing, and once you do,
   the "1–3 fixed bytes" invariant that makes the parser trivial is gone for
   the blob path. Better to let blobs be *files* (the thing filesystems and
   object stores are good at) and keep the wire format lean.
3. **Failure isolation.** Disk full / store slow / sync down must not stop a
   bite alarm from firing. Separating the ephemeral in-memory bus from the
   durable store gives this for free.

### Why not fully separate systems either?

Because the **shared envelope** is what makes synoptic queries possible and is
the maintainer's actual goal ("synced using timestamps and location stamps").
If the bus and the store disagree on what "time" or "where" means, fleet
queries are meaningless. So: two *implementations*, one *addressing contract*.

---

## 3. Storage evaluation (researched, not guessed)

The maintainer named TileDB ("tiledb might be a good choice but so might
something else or even custom made"). I researched each candidate against the
three things that actually matter here: **(a) edge/offline operation** (a boat
radio-silent for hours must keep writing locally and reconcile later),
**(b) multi-rate heterogeneous events in one store**, and **(c) fleet-wide
synoptic query performance** once multiple boats' data must be merged.

### 3.1 TileDB — "maybe a good choice"

- **What it really is:** TileDB Embedded is an open-source (**MIT**-licensed)
  embeddable C++ library for **dense and sparse multi-dimensional arrays**,
  with a Python/R/Java/Go/C# API layer. Sparse arrays can model dataframes and
  key-value stores; dimensions can be arbitrary types. It supports
  chunked/tiled layout, multiple compression/encryption/checksum filters,
  multi-threaded + parallel I/O, cloud backends (S3/GCS/Azure), data
  versioning ("time traveling"), array metadata, and array groups.
- **Verdict on the user's hypothesis:** Yes — it *genuinely* supports efficient
  sparse arrays indexed by `(time, lat, lon)`, and a sparse array is a
  legitimate model for "events scattered across time and space." Its real
  superpower, though, is **dense arrays** — which is exactly the shape of
  depth-sounder waveforms and radar range-doppler maps. So TileDB's fit is
  *strongest* for the dense signal payloads, and merely *adequate* for the
  heterogeneous tabular event index.

### 3.2 TimescaleDB / PostGIS

- **What it really is:** TimescaleDB is a PostgreSQL extension (MIT) for
  time-series: hypertables (time-partitioned chunks), a columnstore with ~90%
  compression, continuous aggregates (incrementally-refreshed materialized
  views), `time_bucket()`, skipscan for "last reading per series", retention
  and S3 tiering. PostGIS is the mature geospatial extension for Postgres. The
  two combine into arguably the strongest **server-side** time+geo story that
  exists.
- **The catch:** it needs a **running Postgres server**. That is not
  edge-native. On a boat with intermittent Starlink, a Postgres server is the
  wrong local store — it is a process to keep alive, a connection to maintain,
  and a poor fit for "just append a file and upload it later."

### 3.3 InfluxDB

- **What it really is:** purpose-built TSDB (3.x is **written in Rust**, same
  language as `fleet-midi`); line protocol
  `measurement,tags fields timestamp`; SQL-like + Flux. Points indexed by time
  + tagset.
- **Real problems for this use case:**
  - **Clustering is closed-source.** Fleet-wide sync across boats is *the*
    requirement, and the distributed piece is exactly what InfluxData
    monetizes. That is a structural mismatch.
  - **InfluxDB 3 Core** (the open "edge" product) has a **5-database limit and
    no data compactor** for fast historical querying — explicitly positioned as
    an "edge collector," not a historical store.
  - **Weak native geospatial.** Location would be just another tag; real
    geo-predicates need bolt-ons.

### 3.4 DuckDB + Parquet

- **What it really is:** DuckDB is an embedded (in-process, like "SQLite for
  analytics"), server-free analytical database with excellent columnar scans.
  It reads Parquet natively — `SELECT * FROM 'f.parquet'` is a first-class
  operation. Clients for Python/R/Java/Wasm; trivially scriptable.
- **Spatial reality:** there is a `spatial` extension, but it is explicitly
  **"🚧 WORK IN PROGRESS"**, with **no spherical (lat/lon) geometry and no
  spatial index** yet. So it is *not* relied on here. The robust substitute is
  a **geohash / H3 / S2 cell as an indexed string/integer column** — this gives
  good spatial selectivity in plain columnar scans and works identically in
  Parquet and on the cloud side later.
- **Why it fits the boat:** zero server, just files on disk; writes survive
  reboots; a finished Parquet file is *trivially* rsync'd/uploaded over
  Starlink when connectivity returns; multiple boats' files are merged with a
  single DuckDB query over a directory of Parquet (`read_parquet('fleet/*.parquet')`).

### 3.5 A custom binary format (extending `fleet-midi`'s own framing)

- **What it is:** reuse `fleet-midi`'s status/data bit-7 discipline and
  running-status machinery, generalize the type byte, add a length-prefixed
  payload and a CRC. This is the **transport** for Tier 1 and the **framing**
  of the shared envelope.
- **Best as:** the *wire* and *envelope* layer, not the *storage* layer. You
  still need a durable engine behind it; a custom format alone gives you no
  query, no compression, no sync.

### 3.6 Comparison matrix

| Criterion | TileDB | Timescale/PostGIS | InfluxDB 3 | DuckDB+Parquet | Custom fmt |
|---|---|---|---|---|---|
| Edge/offline writes (no server) | ✅ embedded lib | ❌ needs Postgres | ⚠️ server, edge-limited Core | ✅✅ in-process, files only | ✅ (transport) |
| Reconcile after radio-silence | ⚠️ manual sync | ❌ | ⚠️ clustering paid | ✅✅ ship Parquet files | n/a |
| Heterogeneous multi-rate events | ⚠️ dense-array bias | ✅ rows | ✅ tags/fields | ✅✅ rows + blob refs | n/a |
| Strong time+geo query | ✅ arrays | ✅✅ (PostGIS) | ⚠️ time yes, geo weak | ✅ via geohash/H3 col | ❌ none |
| Dense signal arrays (radar/depth) | ✅✅ purpose-built | ⚠️ blobs | ❌ | ⚠️ arrays as blob+ref | n/a |
| Fleet merge perf | ⚠️ | ✅ (server) | ⚠️ paid clustering | ✅✅ union of files | n/a |
| License/cost friction | ✅ MIT | ✅ MIT | ⚠️ clustering paid | ✅✅ MIT | ✅ own |
| Dependency weight / familiarity | ⚠️ heavy C++ | ⚠️ server ops | ⚠️ server ops | ✅ single binary | ✅ none |

---

## 4. Recommendation

**Primary durable store: DuckDB + Parquet (edge-first).** It is the only
candidate that is simultaneously offline-native (no server, just files),
trivially syncable over Starlink (ship finished Parquet files), strong at the
columnar scans synoptic queries need, and license/cost-free. Spatial needs are
met with a **geohash/H3 column** rather than the not-yet-ready DuckDB spatial
index. **Reject** TimescaleDB/PostGIS and InfluxDB for the *edge* tier (they
are server products; InfluxDB also paywalls clustering, which is the exact
requirement). They remain the right answer for the *eventual cloud* aggregation
layer (§7, Phase 3), if and when always-on connectivity justifies a server.

**On TileDB — honest, not dismissed.** It is not the primary recommendation for
the tabular event index because DuckDB/Parquet is simpler, more familiar, and
better at heterogeneous rows. **But TileDB is the single best fit for the dense
signal payloads** — depth-sounder waveforms and radar range-doppler maps *are*
dense arrays, which is TileDB's reason to exist. The recommended path is
therefore: **store the event index + blob manifest in Parquet, and revisit
TileDB as a dedicated array backend for dense signals in Phase 2** if DuckDB's
handling of large array payloads proves clumsy. This defers the heaviest
dependency until there's evidence it's needed, without foreclosing it.

**Transport/envelope:** a custom binary format extending `fleet-midi` (Tier 1
bus + the shared envelope in §5–6).

---

## 5. Addressing scheme — the shared event envelope

Every event, regardless of type or tier, conforms to one envelope. The two
design moves that matter:

### 5.1 Separate the event timestamp from the GPS fix

The boat is moving; a GPS fix is jittery and arrives on its own cadence
(typically 1–10 Hz), independent of sensor event rates. **Location therefore
needs its own timestamp**, distinct from the event time. Carrying them fused is
a classic source of "where was the boat when this pinged" ambiguity. The
envelope carries `event_time` and, separately, the most recent `gps_fix`
(position + fix-time + accuracy). For high-rate events between fixes, position
is the last known fix; consumers can interpolate using `fix_time` and
`event_time` if they need to.

### 5.2 Treat GPS context like "running status for location"

`fleet-midi`'s running-status trick — send a status byte once, reuse it for
following bare data bytes — maps directly onto spatial-temporal context: a
vessel's position changes *slowly* relative to a burst of bite-detector or
sensor events. So **broadcast a compact `Context` frame (vessel id + current
GPS fix + clock anchor) at a low cadence (~1 Hz or on significant change), and
let subsequent events inherit it implicitly until the next `Context`.** This is
exactly the bandwidth economics running status was invented for, generalized
from "status byte" to "where and when am I."

### 5.3 Concrete envelope (wire + logical schema)

Logical (what a subscriber/querier sees):

```
Event {
  vessel_id   : u32            // 4B  registry-assigned boat id
  event_type  : u16            // 2B  registry of types (see §5.4)
  event_time  : i64            // 8B  ns since GPS epoch (see §5.5)
  source_id   : u16            // 2B  which sensor/agent produced it
  seq         : u32            // 4B  per-source monotonic, gap detection
  // inherited Context (not repeated per event on the wire):
  ctx {
    fix_time   : i64           // ns since GPS epoch of the GPS fix
    lat_e7     : i32           // deg * 1e7  (±1.1e8 range fits i32)
    lon_e7     : i32           // deg * 1e7
    hacc_cm    : u16           // horizontal accuracy, cm
    clock_quality : u8         // 0=dead-reckon .. 3=GPS-PPS locked
  }
  // payload:
  payload_inline : Option<[u8; 0..255]>   // small events ride the bus
  payload_ref    : Option<BlobRef>        // large events point at a file
}

BlobRef {
  content_hash : [u8; 32]      // blake3/sha256 of the blob
  store        : u8            // 0=local path, 1=object key, ...
  offset       : u64
  length       : u64
  codec        : u16           // raw/zstd/ffmpeg-h264/png/...
}
```

Notes on the choices:
- **Fixed-width scalars everywhere** (no varint) so framing/parsing stays O(1)
  and predictable — staying faithful to MIDI's "length is fully determined"
  philosophy. Variable-ness is confined to the optional inline payload.
- **lat/lon as deg×1e7 into `i32`** (~11 mm resolution, ±180° fits easily) —
  cheaper than float64 and exact for indexing.
- **`seq`** lets consumers detect dropped/late events on the lossy tier-1 bus
  and reconcile against the durable tier.

### 5.4 A small, extensible `event_type` registry

Reserved examples (Phase 1 ships the first few):

| event_type | meaning | typical tier |
|---|---|---|
| `0x0000` | reserved / null | — |
| `0x0001` | Context (vessel + GPS + clock) | T1 control |
| `0x0010`–`0x00FF` | generalized MIDI-compatible (note/cc/bend…) | T1 |
| `0x0100` | scalar reading (value in inline) | T1+T2 |
| `0x0101` | bite-detector trigger | T1+T2 |
| `0x0200` | depth-sounder waveform (ref, dense) | T2 |
| `0x0201` | radar sweep (ref, dense) | T2 |
| `0x0300` | camera frame/clip (ref, opaque) | T2 |
| `0xF000`+ | vendor/experimental | — |

### 5.5 Clock model (no shared wall clock)

There is no guaranteed shared wall clock across boats. The resolution:

- **Anchor all timestamps to GPS time.** Every vessel has a GPS receiver; GPS
  time is disciplined by atomic clocks and is the cheapest accurate shared
  reference afloat. `event_time` and `fix_time` are nanoseconds since the GPS
  epoch (1980-01-06). A local monotonic clock is steered to GPS 1-PPS when
  available.
- **Carry `clock_quality`.** When GPS lock is lost, the vessel free-runs its
  local oscillator; events are stamped but flagged lower quality so fleet
  consumers know to tolerate wider windows.
- **Define synoptic queries with tolerance.** "Everyone's depth right now"
  cannot mean "the identical nanosecond" — radar is 0.5 Hz, depthsonder samples
  on its own clock. A synoptic read is defined as **nearest reading within a
  tolerance window** (e.g. ±2 s) keyed by `(event_type, vessel_id)`, materialized
  at query time from the durable store. This is a DuckDB `ASOF JOIN` / window
  query, cheap on columnar Parquet.
- **Reconcile on sync.** When Starlink comes up, each boat uploads Parquet;
  a `clock_offset` correction recorded per vessel/context lets the cloud layer
  align clocks retroactively. Exact-equality joins are avoided by design.

---

## 6. Wire framing (Tier 1), extending `fleet-midi`

Stays close to the existing parser discipline so the running-status and O(1)
work is preserved:

- **Bit-7 rule preserved:** status/context bytes have bit 7 set; payload data
  bytes have bit 7 clear (this is what lets the parser stay in lockstep).
- A new generalized frame:
  `[status|type byte] [seq] [small inline payload (≤ ~127B, bit-7 clean)] [optional CRC8]`
  for low-latency events.
- A **`Context` frame** (`event_type=0x0001`) carries vessel id + GPS fix +
  clock anchor; subsequent bare-data events inherit it (running-status-for-
  location).
- **Large payloads never go on this wire.** They are written to the blob store
  locally, and only a `BlobRef` rides Tier 1 (and is also persisted in Parquet).

This keeps `parse_midi_byte`'s design DNA — a deterministic, fixed-transition
state machine — while generalizing from "5 message types" to "typed events with
small variable payloads."

---

## 7. Phased roadmap

Markers: ✅ realistically buildable/testable now · ⚠️ doable but with real risk
· 🔬 research spike (decide after evidence) · 🔇 deferred / out of scope for now.

### Phase 1 — Generalized envelope + local persistence (✅ genuinely buildable)
1. Generalize `MidiMessage` into a typed `Event` enum with variable `event_type`
   + small inline payload, **without** breaking the existing 12 tests (keep the
   MIDI path as one event-type family). ✅
2. Add the `Context` frame + "inherit last context" rule; add `seq`. Add unit
   tests mirroring the running-status suite. ✅
3. Add an **envelope encoder/decoder** (§5) with CRC8; fuzz-style tests for
   partial/corrupt streams (extend the existing
   `incomplete_sequence_does_not_panic` style). ✅
4. Prototype **writing simple sensor readings + bite triggers to local
   DuckDB/Parquet**, one Parquet file per `(vessel, hour)` partition, plus a
   `vessels`/`sources` registry. ✅
5. Local synoptic query: "latest reading per source within ±2s" via DuckDB
   ASOF/window SQL. ✅

### Phase 2 — Heavy signals + the TileDB decision (⚠️ / 🔬)
6. Blob store: content-addressed local file layout; `BlobRef` rows in Parquet. ⚠️
7. 🔬 **TileDB spike:** store depth-sounder waveforms + radar sweeps as dense
   TileDB arrays; benchmark query vs. "array-as-blob + DuckDB". Decide
   TileDB-vs-Parquet for dense signals *on evidence*, not preference. 🔬
8. Camera footage: write clips to blob store; metadata-only on the bus/store. ⚠️

### Phase 3 — Fleet sync + cloud aggregation (🔇 until Phase 2 is solid)
9. Offline reconcile: when Starlink is up, upload finished Parquet partitions
   (and blobs) to a central store; record `clock_offset` per vessel. 🔇
10. Fleet synoptic layer on cloud: **DuckDB/Parquet first** (union over uploaded
    files). If rich geospatial SQL or concurrency is needed, **migrate the cloud
    read model to TimescaleDB/PostGIS** (the right server-side home once
    always-on is justified). 🔇
11. Cross-vessel "catch-rate over the last hour" dashboards. 🔇

**Deliberately deferred / non-goals** for this proposal: real-time
inter-vessel streaming (boats are often radio-silent — fleet value is
*eventual* synoptic views, not live mesh), and a custom time-series engine
("custom made" is ruled out: DuckDB+Parquet already covers the 90th percentile
and a custom engine would just rebuild it badly).

---

## 8. Summary of the recommendation

Two tiers, one envelope:

- **Tier 1 (real-time bus):** generalize `fleet-midi`'s proven byte-machine —
  typed events + small payloads + a `Context` frame that gives every event a
  timestamp and location by *inheritance* (running-status, generalized to
  space-time). Keep it ephemeral and sub-ms.
- **Tier 2 (durable store):** **DuckDB + Parquet**, edge-first and offline-
  native; blobs are files referenced from Parquet rows; spatial queries use a
  geohash/H3 column. **TileDB kept as a Phase-2 candidate specifically for
  dense signals** (radar/depthsonder), decided by benchmark. **TimescaleDB/
  PostGIS deferred to the eventual cloud layer**, not the boat.

The unifying idea is the **shared `(event_time, fix_time, lat, lon, vessel_id,
event_type, payload|ref)` envelope** plus a **GPS-anchored, tolerance-based
clock model** — that is what turns "a MIDI router" into "a fleet-wide sensor
event bus" without lying about what each tier is good at.
