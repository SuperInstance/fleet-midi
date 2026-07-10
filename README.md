# Fleet MIDI — Musical Instrument Digital Interface for Agent Meshes

**MIDI** (Musical Instrument Digital Interface, pronounced "middy") is a 40-year-old serial protocol that encodes musical events — note-ons, note-offs, control changes, pitch bends — as compact 1–3 byte messages. **Fleet MIDI** is a Rust library that parses these binary messages and broadcasts them across a constellation of agents, treating the MIDI wire format as a real-time control plane for distributed systems.

## Why It Matters

MIDI is the most successful binary protocol in history: every electronic keyboard, every DAW (Digital Audio Workstation), every film scoring tool speaks it. Its design is remarkably efficient — a "Note On, Middle C, velocity 64" takes exactly 3 bytes. This efficiency makes MIDI attractive far beyond music: robotics control, lighting systems, and live coding environments all repurpose MIDI as a low-latency signaling layer.

In a fleet context, MIDI provides something HTTP and gRPC cannot: **sub-millisecond, fire-and-forget event delivery**. A MIDI message from a controller arrives in under 1ms. Translating that into fleet actions — agent wakeups, parameter sweeps, visual cues — requires a parser that understands the wire format and can route decoded events to the right subscriber. That is what this crate provides.

## How It Works

MIDI messages consist of a **status byte** followed by 0–2 **data bytes**. The high nibble of the status byte determines the message type:

```
Status byte:  1sss ssss   (bit 7 always set)
Data byte:    0ddd dddd   (bit 7 always clear)
```

| Status (hex) | Message | Data bytes | Meaning |
|---|---|---|---|
| `8n` | Note Off | note, velocity | Release a note on channel n |
| `9n` | Note On | note, velocity | Strike a note (velocity 0 = note off) |
| `Bn` | Control Change | controller, value | Knob/slider on channel n |
| `En` | Pitch Bend | lsb, msb | 14-bit pitch bend on channel n |
| `Cn` | Program Change | program | Change instrument on channel n |

Channel is encoded in the low nibble (`n = 0–15`), giving 16 independent channels per MIDI bus.

### Parsing State Machine

A MIDI parser is a byte-level state machine. Bytes arrive in a stream with no framing — the parser must track whether it expects a status byte or data bytes:

```
State: WAIT_STATUS
  byte & 0x80 → State = READ_DATA_N (N from message type)

State: READ_DATA_1
  read byte (data) → store

State: READ_DATA_2
  read byte (data) → emit complete message → State = WAIT_STATUS
```

**Running status** optimization: if a data byte arrives while in `WAIT_STATUS`, the parser reuses the previous status byte. This halves the bandwidth for continuous controllers.

### Time Complexity

| Operation | Complexity |
|---|---|
| Parse one message | O(1) — fixed-size state transition |
| Broadcast to K subscribers | O(K) — fan-out per message |
| Route by channel | O(1) — hash table lookup |

MIDI parsing is inherently O(1) per message because every message type has a fixed, protocol-defined length. There are no length-prefixed payloads or variable-width fields.

### Fleet Broadcast Architecture

```
[MIDI Device] → [Parser] → [Event Queue] → [Subscriber Agents]
                     ↑
                   Channel Router (channels 0–15 → agent topics)
```

Each MIDI channel maps to a fleet topic. Channel 10 (standard drum channel) might route to a percussion agent; channel 0 might route to a lead melody agent. The parser extracts the channel and note, the router dispatches to subscribers.

## Quick Start

```rust
use fleet_midi::{parse_midi_slice, FleetBroadcaster, MidiMessage};

fn main() {
    // "Note On, Middle C, velocity 64" followed by a matching Note Off.
    let bytes = [
        0x90, 60, 64, // status, note, velocity
        0x80, 60, 32,
    ];

    let messages = parse_midi_slice(&bytes);
    println!("parsed {} messages", messages.len());

    let mut broadcaster = FleetBroadcaster::new();
    broadcaster.subscribe(0, "lead-agent");
    broadcaster.subscribe(10, "drums-agent");

    for msg in &messages {
        let notified = broadcaster.broadcast(msg);
        println!("{:?} -> {:?}", msg, notified);
    }
}
```

## API

### `MidiMessage`

Parsed channel messages emitted by the parser.

```rust
pub enum MidiMessage {
    NoteOn { channel: u8, note: u8, velocity: u8 },
    NoteOff { channel: u8, note: u8, velocity: u8 },
    ControlChange { channel: u8, controller: u8, value: u8 },
    PitchBend { channel: u8, value: u16 },
    ProgramChange { channel: u8, program: u8 },
}
```

Note-on messages with `velocity == 0` are normalized to `NoteOff`, matching the conventional MIDI interpretation of a zero-velocity note-on as a note-off event.

### `ParserState`

Byte-level parsing state for a MIDI byte stream. Use it directly when data arrives one byte at a time:

```rust
use fleet_midi::{parse_midi_byte, ParserState};

let mut state = ParserState::default();
for byte in [0x90u8, 60, 64] {
    if let Some(msg) = parse_midi_byte(&mut state, byte) {
        println!("{:?}", msg);
    }
}
```

### `parse_midi_byte(state: &mut ParserState, byte: u8) -> Option<MidiMessage>`

Feed a single byte into the state machine. Returns `Some(MidiMessage)` once a complete message has been assembled; otherwise returns `None`. Implements running status and tolerates stray data bytes and incomplete messages without panicking.

### `parse_midi_slice(bytes: &[u8]) -> Vec<MidiMessage>`

Parse a complete byte buffer into decoded messages. Incomplete messages at the end of the buffer are silently dropped.

### `FleetBroadcaster`

A simple channel-router for decoded messages.

```rust
use fleet_midi::{FleetBroadcaster, MidiMessage};

let mut bc = FleetBroadcaster::new();
bc.subscribe(0, "lead-agent");
bc.subscribe(0, "visuals-agent");
bc.subscribe(10, "drums-agent");

let msg = MidiMessage::NoteOn { channel: 0, note: 60, velocity: 64 };
assert_eq!(bc.broadcast(&msg), vec!["lead-agent", "visuals-agent"]);
```

### `stub::hello() -> &'static str`

Legacy scaffold placeholder returning `"hello from fleet-midi"`. Retained for backward compatibility; new code should use the MIDI parsing API.

## Phase 2: Local Parquet Persistence (`fleet_midi::persist`)

Phase 2 adds a **durable Tier-2 store** on top of the Phase 1 real-time bus.
Events decoded through the Phase 1 binary envelope (`encode_event` /
`decode_event`) are persisted as rows in **genuine Parquet files** on disk,
partitioned one file per `(vessel, hour)`, plus a `vessels`/`sources`
registry. The tolerance-window query implements the §5.5 synoptic pattern:
"latest reading per source within ±2 seconds."

### Design decision: pure-Rust Parquet instead of bundled DuckDB

The proposal recommends DuckDB + Parquet as the primary durable store. In this
development sandbox (~3.5 GB RAM), DuckDB's Rust bindings (`duckdb` crate with
the `bundled` feature, which compiles the full C++ library) peaked at 3.3 GB
and was killed before completing. The `duckdb` CLI binary is not available
here either.

We therefore use the **pure-Rust `parquet` + `arrow` crates** to write real,
valid Parquet files — the critical property for the design (a boat rsyncs
finished Parquet files; a future cloud DuckDB reads them with
`SELECT * FROM 'events.parquet'`) — and implement the tolerance-window query
logic in Rust. The on-disk format is genuine Parquet; only the local query
engine differs from the proposal's ideal. If DuckDB becomes practical (more
RAM, or a pre-built binary), the query layer can be swapped without changing
any persisted files.

### Storage layout

```
<base_dir>/
  data/
    vessel=<id>/
      hour=<unix_hour>/
        events.parquet
  registry/
    vessels.parquet
    sources.parquet
```

Each `events.parquet` partition contains rows with this schema (§5.3):

| Column | Type | Source |
|---|---|---|
| `vessel_id` | `UInt32` | Context |
| `event_type` | `UInt16` | Event |
| `event_time` | `Int64` | Event (ns since GPS epoch) |
| `source_id` | `UInt16` | Event |
| `seq` | `UInt32` | Event (per-source monotonic) |
| `fix_time` | `Int64` | Context (ns since GPS epoch) |
| `lat_e7` | `Int32` | Context (deg × 1e7, ~11 mm resolution) |
| `lon_e7` | `Int32` | Context |
| `hacc_cm` | `UInt16` | Context (horizontal accuracy, cm) |
| `clock_quality` | `UInt8` | Context (0=dead-reckon … 3=GPS-PPS locked) |

The `vessels` and `sources` registries are separate Parquet files containing
`(vessel_id, name)` and `(source_id, vessel_id, name)` rows respectively. This
keeps everything as plain files — no server, no database process — consistent
with the edge-native, offline-first design.

### Quick start

```rust
use fleet_midi::persist::{EventStore, DEFAULT_TOLERANCE_NS, persisted_event_types};
use fleet_midi::{Event, Context, encode_event, decode_event};

let mut store = EventStore::open("/data/fleet").unwrap();
store.register_vessel(42, "F/V Aurora");

let ctx = Context::new(42, 1_000_000_000_000_000_000, 600_000_000, -1_200_000_000, 150, 3);

// Persist an event through the real Phase 1 wire path.
let ev = Event::new(persisted_event_types::SCALAR_READING, 1_000_000_000_100_000, 1, 1);
let decoded = decode_event(&encode_event(&ev)).unwrap();
store.persist(&decoded, ctx);
store.flush().unwrap();

// "Latest reading per source within ±2 seconds" (§5.5 tolerance window).
let results = store.query_latest_within_tolerance(
    1_000_000_000_100_000,
    DEFAULT_TOLERANCE_NS,
    None,
);
for event in &results {
    println!("source {} at {}: lat={:.7}, lon={:.7}",
             event.source_id, event.event_time,
             event.lat_e7 as f64 / 1e7, event.lon_e7 as f64 / 1e7);
}
```

### Tolerance-window query semantics

Since sources don't share a wall clock (§5.5), `query_latest_within_tolerance`
finds the **nearest** reading per `(source_id, event_type)` within the window,
not an exact-timestamp match. The boundary is **inclusive**: an event at
exactly ±tolerance is included; an event 1 ns beyond is excluded. Ties (two
events equidistant from the target) are broken by preferring the later
`event_time`.

### What's implemented

- ✅ `EventStore`: buffer → flush → Parquet, partitioned by `(vessel, hour)`
- ✅ Read-merge-write append to existing partition files
- ✅ `vessels`/`sources` registry as separate Parquet files (reloadable)
- ✅ Tolerance-window synoptic query (§5.5) with inclusive boundary
- ✅ Event-type filtering (`SCALAR_READING`, `BITE_TRIGGER`, etc.)
- ✅ Auto-registration of vessels seen via Context frames

### 🔮 Out of scope (per proposal Phase 2–3)

These are explicitly deferred to later phases per the proposal's own roadmap:

- 🔮 **Blob storage** for large payloads (camera footage, dense arrays) — the
  `BlobRef` manifest schema is designed (§5.3) but not implemented
- 🔮 **TileDB spike** for dense signal arrays (radar, depth-sounder waveforms)
  — the proposal recommends deciding this on benchmark evidence, not preference
- 🔮 **Fleet sync** — uploading finished Parquet partitions over Starlink,
  `clock_offset` reconciliation, cross-vessel merge
- 🔮 **Cloud aggregation** — DuckDB/Parquet union queries at fleet scale, or
  migration to TimescaleDB/PostGIS if rich geo-SQL is needed
- 🔮 **Geohash/H3 spatial column** for efficient geo-predicates in Parquet scans

## Architecture Notes

Fleet MIDI provides a **real-time sensory channel** for the SuperInstance constellation. In the conservation law **γ + η = C**, MIDI events are a form of η (η脉冲, pulse energy) — short, sharp impulses that perturb agent state without sustained computation. A drummer's kick drum becomes a fleet-wide synchronization pulse; a knob turn becomes a parameter sweep across all agents.

The crate is designed to integrate with the PLATO room architecture, where MIDI events from a physical controller in one room can trigger agent responses in another. See the [SuperInstance Architecture](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md) for how real-time input streams flow through the fleet.

## References

1. The MIDI 1.0 Specification — [https://midi.org/spec-details](https://midi.org/spec-details)
2. Messick, S. "The MIDI Specification" — practical parsing guide, [https://www.cs.cmu.edu/~music/cmsip/readings/MIDI%20tutorial%20for%20programmers.html](https://www.cs.cmu.edu/~music/cmsip/readings/MIDI%20tutorial%20for%20programmers.html)
3. MIDI 2.0 Working Draft — [https://midi.org/midi-2-0-specifications](https://midi.org/midi-2-0-specifications)

## License

MIT
