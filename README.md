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
use fleet_midi::stub;

fn main() {
    println!("{}", stub::hello());
    // "hello from fleet-midi"
}
```

The crate is currently scaffolded — the parsing state machine, channel router, and broadcast layer are planned. The public API will expose:

```rust
// Planned API
pub fn parse_midi_byte(state: &mut ParserState, byte: u8) -> Option<MidiMessage>;
pub fn parse_midi_slice(bytes: &[u8]) -> Vec<MidiMessage>;
pub struct FleetBroadcaster { /* ... */ }
impl FleetBroadcaster {
    pub fn subscribe(&mut self, channel: u8, agent_id: &str);
    pub fn broadcast(&self, msg: &MidiMessage);
}
```

## API

### `stub::hello() -> &'static str`

Placeholder returning `"hello from fleet-midi"`. The full MIDI parsing API will replace this once the state machine and router are implemented.

### Planned: `MidiMessage`

```rust
pub enum MidiMessage {
    NoteOn { channel: u8, note: u8, velocity: u8 },
    NoteOff { channel: u8, note: u8, velocity: u8 },
    ControlChange { channel: u8, controller: u8, value: u8 },
    PitchBend { channel: u8, value: u16 },
    ProgramChange { channel: u8, program: u8 },
}
```

## Architecture Notes

Fleet MIDI provides a **real-time sensory channel** for the SuperInstance constellation. In the conservation law **γ + η = C**, MIDI events are a form of η (η脉冲, pulse energy) — short, sharp impulses that perturb agent state without sustained computation. A drummer's kick drum becomes a fleet-wide synchronization pulse; a knob turn becomes a parameter sweep across all agents.

The crate is designed to integrate with the PLATO room architecture, where MIDI events from a physical controller in one room can trigger agent responses in another. See the [SuperInstance Architecture](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md) for how real-time input streams flow through the fleet.

## References

1. The MIDI 1.0 Specification — [https://midi.org/spec-details](https://midi.org/spec-details)
2. Messick, S. "The MIDI Specification" — practical parsing guide, [https://www.cs.cmu.edu/~music/cmsip/readings/MIDI%20tutorial%20for%20programmers.html](https://www.cs.cmu.edu/~music/cmsip/readings/MIDI%20tutorial%20for%20programmers.html)
3. MIDI 2.0 Working Draft — [https://midi.org/midi-2-0-specifications](https://midi.org/midi-2-0-specifications)

## License

MIT
