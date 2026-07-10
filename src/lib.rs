//! fleet-midi - MIDI message parsing and fleet broadcast for constellation agents

use std::collections::HashMap;

/// A parsed MIDI channel message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MidiMessage {
    NoteOn { channel: u8, note: u8, velocity: u8 },
    NoteOff { channel: u8, note: u8, velocity: u8 },
    ControlChange { channel: u8, controller: u8, value: u8 },
    PitchBend { channel: u8, value: u16 },
    ProgramChange { channel: u8, program: u8 },
}

impl MidiMessage {
    /// Returns the MIDI channel (0–15) associated with the message.
    pub fn channel(&self) -> u8 {
        match self {
            MidiMessage::NoteOn { channel, .. } => *channel,
            MidiMessage::NoteOff { channel, .. } => *channel,
            MidiMessage::ControlChange { channel, .. } => *channel,
            MidiMessage::PitchBend { channel, .. } => *channel,
            MidiMessage::ProgramChange { channel, .. } => *channel,
        }
    }
}

/// Byte-level parsing state for a MIDI byte stream.
#[derive(Debug, Default, Clone)]
pub struct ParserState {
    last_status: Option<u8>,
    pending: Vec<u8>,
}

/// Returns the number of data bytes expected for a supported channel status byte.
fn data_byte_count(status: u8) -> Option<usize> {
    match status & 0xF0 {
        0x80 | 0x90 | 0xB0 | 0xE0 => Some(2),
        0xC0 => Some(1),
        _ => None,
    }
}

/// Feed a single MIDI byte into the parser.
///
/// Returns a fully parsed `MidiMessage` once enough bytes have arrived,
/// or `None` while the message is still incomplete.
///
/// Implements running status: data bytes received while no new status
/// byte is pending reuse the most recent supported channel status byte.
///
/// Note-on messages with velocity `0` are normalized to `MidiMessage::NoteOff`,
/// matching the conventional MIDI interpretation of "velocity zero note-on"
/// as a note-off event.
pub fn parse_midi_byte(state: &mut ParserState, byte: u8) -> Option<MidiMessage> {
    if byte & 0x80 != 0 {
        // Status byte.
        match byte {
            // System real-time messages should not disrupt running status.
            0xF8..=0xFF => return None,
            // System common messages cancel any running status.
            0xF0..=0xF7 => {
                state.last_status = None;
                state.pending.clear();
                return None;
            }
            _ => {}
        }

        if let Some(_count) = data_byte_count(byte) {
            state.last_status = Some(byte);
            state.pending.clear();
        } else {
            // Unsupported status byte — reset to a safe state.
            state.last_status = None;
            state.pending.clear();
        }
        None
    } else {
        // Data byte.
        if state.pending.is_empty() {
            // We need a status byte. Use running status if available.
            if let Some(status) = state.last_status {
                let count = data_byte_count(status)?;
                state.pending.reserve(count);
            } else {
                // No running status — stray data byte, ignore.
                return None;
            }
        }

        state.pending.push(byte);
        let status = state.last_status?;
        let count = data_byte_count(status)?;
        if state.pending.len() < count {
            return None;
        }

        let channel = status & 0x0F;
        let bytes = &state.pending;
        let msg = match status & 0xF0 {
            0x80 => MidiMessage::NoteOff {
                channel,
                note: bytes[0],
                velocity: bytes[1],
            },
            0x90 => {
                if bytes[1] == 0 {
                    MidiMessage::NoteOff {
                        channel,
                        note: bytes[0],
                        velocity: 0,
                    }
                } else {
                    MidiMessage::NoteOn {
                        channel,
                        note: bytes[0],
                        velocity: bytes[1],
                    }
                }
            }
            0xB0 => MidiMessage::ControlChange {
                channel,
                controller: bytes[0],
                value: bytes[1],
            },
            0xE0 => MidiMessage::PitchBend {
                channel,
                value: ((bytes[1] as u16) << 7) | (bytes[0] as u16),
            },
            0xC0 => MidiMessage::ProgramChange {
                channel,
                program: bytes[0],
            },
            _ => return None,
        };

        state.pending.clear();
        Some(msg)
    }
}

/// Parse a slice of MIDI bytes into complete messages.
///
/// Bytes that do not form a complete message by the end of the slice are
/// silently dropped.
pub fn parse_midi_slice(bytes: &[u8]) -> Vec<MidiMessage> {
    let mut state = ParserState::default();
    bytes
        .iter()
        .filter_map(|&b| parse_midi_byte(&mut state, b))
        .collect()
}

/// Channel-based broadcaster for decoded MIDI messages.
#[derive(Debug, Default)]
pub struct FleetBroadcaster {
    subscriptions: HashMap<u8, Vec<String>>,
}

impl FleetBroadcaster {
    /// Create an empty broadcaster.
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe an agent to all messages on a MIDI channel.
    pub fn subscribe(&mut self, channel: u8, agent_id: &str) {
        if channel > 15 {
            return;
        }
        self.subscriptions
            .entry(channel)
            .or_default()
            .push(agent_id.to_string());
    }

    /// Broadcast a message to all agents subscribed to its channel.
    ///
    /// Returns the IDs of agents that were notified.
    pub fn broadcast(&self, msg: &MidiMessage) -> Vec<String> {
        match self.subscriptions.get(&msg.channel()) {
            Some(subs) => subs.iter().cloned().collect(),
            None => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 1, Task 1: generalized event envelope
// ---------------------------------------------------------------------------

/// Maximum bytes carried inline in an [`Event`] payload.
///
/// Phase 1 caps the inline payload at 32 bytes, which is enough for the
/// serialized [`Context`] frame (23 bytes) plus a small margin.  The
/// proposal's full envelope allows up to 255 bytes; that can be widened
/// later without changing the wire discipline.
pub const MAX_INLINE_PAYLOAD: usize = 32;

/// Reserved `event_type` values (§5.4).
pub mod event_types {
    /// Context frame: vessel + GPS fix + clock anchor.
    pub const CONTEXT: u16 = 0x0001;

    // 0x0010..=0x00FF are reserved for generalized MIDI-compatible events.
    /// MIDI Note On.
    pub const MIDI_NOTE_ON: u16 = 0x0010;
    /// MIDI Note Off.
    pub const MIDI_NOTE_OFF: u16 = 0x0011;
    /// MIDI Control Change.
    pub const MIDI_CONTROL_CHANGE: u16 = 0x0012;
    /// MIDI Pitch Bend.
    pub const MIDI_PITCH_BEND: u16 = 0x0013;
    /// MIDI Program Change.
    pub const MIDI_PROGRAM_CHANGE: u16 = 0x0014;
}

/// A generalized, typed event on the fleet bus.
///
/// This is the shared envelope described in the proposal: every event,
/// regardless of type, carries an `event_type`, a timestamp placeholder,
/// a source id, a sequence number, and a small bounded inline payload.
///
/// `event_time` is a placeholder in Phase 1 (Task 2 attaches real GPS-time
/// semantics through the inherited [`Context`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub event_type: u16,
    pub event_time: i64,
    pub source_id: u16,
    pub seq: u32,
    pub payload_len: u8,
    pub payload: [u8; MAX_INLINE_PAYLOAD],
}

impl Event {
    /// Create an event with no payload.
    pub fn new(event_type: u16, event_time: i64, source_id: u16, seq: u32) -> Self {
        Self {
            event_type,
            event_time,
            source_id,
            seq,
            payload_len: 0,
            payload: [0; MAX_INLINE_PAYLOAD],
        }
    }

    /// Return the active slice of the inline payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }

    /// Set the inline payload, truncating to [`MAX_INLINE_PAYLOAD`].
    pub fn set_payload(&mut self, bytes: &[u8]) {
        let len = bytes.len().min(MAX_INLINE_PAYLOAD);
        self.payload_len = len as u8;
        self.payload[..len].copy_from_slice(&bytes[..len]);
        self.payload[len..].fill(0);
    }
}

impl From<MidiMessage> for Event {
    fn from(msg: MidiMessage) -> Self {
        let mut ev = Event::new(0, 0, 0, 0);
        match msg {
            MidiMessage::NoteOn {
                channel,
                note,
                velocity,
            } => {
                ev.event_type = event_types::MIDI_NOTE_ON;
                ev.set_payload(&[channel, note, velocity]);
            }
            MidiMessage::NoteOff {
                channel,
                note,
                velocity,
            } => {
                ev.event_type = event_types::MIDI_NOTE_OFF;
                ev.set_payload(&[channel, note, velocity]);
            }
            MidiMessage::ControlChange {
                channel,
                controller,
                value,
            } => {
                ev.event_type = event_types::MIDI_CONTROL_CHANGE;
                ev.set_payload(&[channel, controller, value]);
            }
            MidiMessage::PitchBend { channel, value } => {
                ev.event_type = event_types::MIDI_PITCH_BEND;
                let lsb = (value & 0x7F) as u8;
                let msb = ((value >> 7) & 0x7F) as u8;
                ev.set_payload(&[channel, lsb, msb]);
            }
            MidiMessage::ProgramChange { channel, program } => {
                ev.event_type = event_types::MIDI_PROGRAM_CHANGE;
                ev.set_payload(&[channel, program]);
            }
        }
        ev
    }
}

/// Errors when converting an [`Event`] back into a [`MidiMessage`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MidiConversionError {
    NotMidiEventType(u16),
    PayloadTooShort { expected: u8, got: u8 },
}

impl TryFrom<Event> for MidiMessage {
    type Error = MidiConversionError;

    fn try_from(ev: Event) -> Result<Self, Self::Error> {
        let need = |n| {
            if ev.payload_len >= n {
                Ok(())
            } else {
                Err(MidiConversionError::PayloadTooShort {
                    expected: n,
                    got: ev.payload_len,
                })
            }
        };
        let b = &ev.payload;
        match ev.event_type {
            event_types::MIDI_NOTE_ON => {
                need(3)?;
                Ok(MidiMessage::NoteOn {
                    channel: b[0],
                    note: b[1],
                    velocity: b[2],
                })
            }
            event_types::MIDI_NOTE_OFF => {
                need(3)?;
                Ok(MidiMessage::NoteOff {
                    channel: b[0],
                    note: b[1],
                    velocity: b[2],
                })
            }
            event_types::MIDI_CONTROL_CHANGE => {
                need(3)?;
                Ok(MidiMessage::ControlChange {
                    channel: b[0],
                    controller: b[1],
                    value: b[2],
                })
            }
            event_types::MIDI_PITCH_BEND => {
                need(3)?;
                let value = ((b[2] as u16) << 7) | (b[1] as u16);
                Ok(MidiMessage::PitchBend {
                    channel: b[0],
                    value,
                })
            }
            event_types::MIDI_PROGRAM_CHANGE => {
                need(2)?;
                Ok(MidiMessage::ProgramChange {
                    channel: b[0],
                    program: b[1],
                })
            }
            other => Err(MidiConversionError::NotMidiEventType(other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 1, Task 2: Context frame + "inherit last context" rule
// ---------------------------------------------------------------------------

/// Spatial-temporal context that events inherit (§5.2/§5.3).
///
/// A `Context` frame is broadcast at a low cadence; subsequent events
/// implicitly reuse it until the next `Context` arrives, just as running
/// status lets bare MIDI data bytes reuse the last status byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context {
    pub vessel_id: u32,
    pub fix_time: i64,
    pub lat_e7: i32,
    pub lon_e7: i32,
    pub hacc_cm: u16,
    pub clock_quality: u8,
}

impl Context {
    /// Create a new context frame.
    pub fn new(
        vessel_id: u32,
        fix_time: i64,
        lat_e7: i32,
        lon_e7: i32,
        hacc_cm: u16,
        clock_quality: u8,
    ) -> Self {
        Self {
            vessel_id,
            fix_time,
            lat_e7,
            lon_e7,
            hacc_cm,
            clock_quality,
        }
    }
}

/// Byte length of a serialized [`Context`] frame in an event payload.
const CONTEXT_PAYLOAD_LEN: usize = 23;

impl Event {
    /// If this event is a Context frame (`event_type == event_types::CONTEXT`),
    /// return the embedded [`Context`].
    pub fn as_context(&self) -> Option<Context> {
        if self.event_type != event_types::CONTEXT {
            return None;
        }
        if self.payload_len as usize != CONTEXT_PAYLOAD_LEN {
            return None;
        }
        let b = &self.payload;
        let vessel_id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        let fix_time = i64::from_be_bytes([
            b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
        ]);
        let lat_e7 = i32::from_be_bytes([b[12], b[13], b[14], b[15]]);
        let lon_e7 = i32::from_be_bytes([b[16], b[17], b[18], b[19]]);
        let hacc_cm = u16::from_be_bytes([b[20], b[21]]);
        let clock_quality = b[22];
        Some(Context {
            vessel_id,
            fix_time,
            lat_e7,
            lon_e7,
            hacc_cm,
            clock_quality,
        })
    }

    /// Build a Context event from a [`Context`].
    pub fn from_context(ctx: Context, event_time: i64, source_id: u16, seq: u32) -> Self {
        let mut ev = Event::new(event_types::CONTEXT, event_time, source_id, seq);
        let v = ctx.vessel_id.to_be_bytes();
        let f = ctx.fix_time.to_be_bytes();
        let lat = ctx.lat_e7.to_be_bytes();
        let lon = ctx.lon_e7.to_be_bytes();
        let h = ctx.hacc_cm.to_be_bytes();
        ev.set_payload(&[
            v[0], v[1], v[2], v[3], f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], lat[0],
            lat[1], lat[2], lat[3], lon[0], lon[1], lon[2], lon[3], h[0], h[1],
            ctx.clock_quality,
        ]);
        ev
    }
}

/// Router state that tracks the last broadcast [`Context`] for inherited
/// event attribution.
#[derive(Debug, Default, Clone)]
pub struct EventRouter {
    last_context: Option<Context>,
}

impl EventRouter {
    /// Create a router with no inherited context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a Context frame, updating the inherited context.
    pub fn broadcast_context(&mut self, ctx: Context) {
        self.last_context = Some(ctx);
    }

    /// Process an event, updating inherited context if the event itself is a
    /// Context frame, and returning the context that applies to the event.
    ///
    /// Events received before any Context has been seen return `None`;
    /// callers should treat this as "no position/clock attribution yet"
    /// rather than an error.
    pub fn handle_event(&mut self, event: &Event) -> Option<Context> {
        if let Some(ctx) = event.as_context() {
            self.last_context = Some(ctx);
        }
        self.last_context
    }

    /// Return the currently inherited context without processing an event.
    pub fn current_context(&self) -> Option<Context> {
        self.last_context
    }
}

/// Stub module retained for backward compatibility with the original scaffold.
pub mod stub {
    /// Placeholder function returning a greeting.
    pub fn hello() -> &'static str {
        "hello from fleet-midi"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_on_and_off() {
        let bytes = [
            0x90, 60, 64, // Note On, channel 0, note 60, velocity 64
            0x80, 60, 32, // Note Off, channel 0, note 60, velocity 32
        ];
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![
                MidiMessage::NoteOn {
                    channel: 0,
                    note: 60,
                    velocity: 64,
                },
                MidiMessage::NoteOff {
                    channel: 0,
                    note: 60,
                    velocity: 32,
                },
            ]
        );
    }

    #[test]
    fn note_on_velocity_zero_is_note_off() {
        let bytes = [0x93, 48, 0]; // Note On, channel 3, velocity 0
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![MidiMessage::NoteOff {
                channel: 3,
                note: 48,
                velocity: 0,
            }]
        );
    }

    #[test]
    fn running_status_note_on() {
        let bytes = [
            0x91, 60, 64, // status + first message on channel 1
            64, 32,       // running status: note 64 velocity 32
            67, 16,       // running status: note 67 velocity 16
        ];
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![
                MidiMessage::NoteOn {
                    channel: 1,
                    note: 60,
                    velocity: 64,
                },
                MidiMessage::NoteOn {
                    channel: 1,
                    note: 64,
                    velocity: 32,
                },
                MidiMessage::NoteOn {
                    channel: 1,
                    note: 67,
                    velocity: 16,
                },
            ]
        );
    }

    #[test]
    fn running_status_note_off_after_status_change() {
        // A fresh status byte should replace the running status.
        let bytes = [
            0x90, 60, 64, // Note On channel 0
            0x80, 60, 0,  // Note Off channel 0
            48, 0,        // Running status: Note Off channel 0, note 48
        ];
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![
                MidiMessage::NoteOn {
                    channel: 0,
                    note: 60,
                    velocity: 64,
                },
                MidiMessage::NoteOff {
                    channel: 0,
                    note: 60,
                    velocity: 0,
                },
                MidiMessage::NoteOff {
                    channel: 0,
                    note: 48,
                    velocity: 0,
                },
            ]
        );
    }

    #[test]
    fn control_change() {
        let bytes = [0xB2, 0x01, 0x7F]; // CC #1, value 127, channel 2
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![MidiMessage::ControlChange {
                channel: 2,
                controller: 1,
                value: 127,
            }]
        );
    }

    #[test]
    fn program_change_single_data_byte() {
        let bytes = [0xC5, 42]; // Program change, channel 5, program 42
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![MidiMessage::ProgramChange {
                channel: 5,
                program: 42,
            }]
        );
    }

    #[test]
    fn pitch_bend_14bit_lsb_first() {
        // value = (msb << 7) | lsb
        let bytes = [0xE4, 0x34, 0x12]; // lsb=0x34, msb=0x12
        let messages = parse_midi_slice(&bytes);
        assert_eq!(
            messages,
            vec![MidiMessage::PitchBend {
                channel: 4,
                value: 0x0934,
            }]
        );
    }

    #[test]
    fn incomplete_sequence_does_not_panic() {
        let bytes = [0x90, 60]; // missing velocity
        let messages = parse_midi_slice(&bytes);
        assert!(messages.is_empty());
    }

    #[test]
    fn stray_data_byte_does_not_panic() {
        let bytes = [60, 64]; // no status byte at all
        let messages = parse_midi_slice(&bytes);
        assert!(messages.is_empty());
    }

    #[test]
    fn state_accumulates_across_calls() {
        let mut state = ParserState::default();
        assert_eq!(parse_midi_byte(&mut state, 0x90), None);
        assert_eq!(parse_midi_byte(&mut state, 60), None);
        assert_eq!(
            parse_midi_byte(&mut state, 64),
            Some(MidiMessage::NoteOn {
                channel: 0,
                note: 60,
                velocity: 64,
            })
        );
    }

    #[test]
    fn broadcaster_routes_by_channel() {
        let mut bc = FleetBroadcaster::new();
        bc.subscribe(0, "lead");
        bc.subscribe(10, "drums");
        bc.subscribe(0, "visuals");

        let msg = MidiMessage::NoteOn {
            channel: 0,
            note: 60,
            velocity: 64,
        };
        let notified = bc.broadcast(&msg);
        assert_eq!(notified, vec!["lead", "visuals"]);

        let drums = MidiMessage::NoteOff {
            channel: 10,
            note: 36,
            velocity: 0,
        };
        assert_eq!(bc.broadcast(&drums), vec!["drums"]);

        let no_subs = MidiMessage::ControlChange {
            channel: 7,
            controller: 1,
            value: 0,
        };
        assert!(bc.broadcast(&no_subs).is_empty());
    }

    #[test]
    fn stub_hello_still_works() {
        assert_eq!(stub::hello(), "hello from fleet-midi");
    }

    // --- Task 1: Event envelope + MIDI compatibility ---

    #[test]
    fn midi_note_on_round_trips_through_event() {
        let msg = MidiMessage::NoteOn {
            channel: 3,
            note: 60,
            velocity: 64,
        };
        let ev: Event = msg.clone().into();
        assert_eq!(ev.event_type, event_types::MIDI_NOTE_ON);
        assert_eq!(ev.event_time, 0);
        assert_eq!(ev.source_id, 0);
        assert_eq!(ev.payload_len, 3);
        assert_eq!(ev.payload()[..], [3, 60, 64]);
        let back: MidiMessage = ev.try_into().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn midi_note_off_round_trips_through_event() {
        let msg = MidiMessage::NoteOff {
            channel: 5,
            note: 48,
            velocity: 32,
        };
        let ev: Event = msg.clone().into();
        assert_eq!(ev.event_type, event_types::MIDI_NOTE_OFF);
        assert_eq!(ev.payload()[..], [5, 48, 32]);
        let back: MidiMessage = ev.try_into().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn midi_control_change_round_trips_through_event() {
        let msg = MidiMessage::ControlChange {
            channel: 2,
            controller: 1,
            value: 127,
        };
        let ev: Event = msg.clone().into();
        assert_eq!(ev.event_type, event_types::MIDI_CONTROL_CHANGE);
        let back: MidiMessage = ev.try_into().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn midi_pitch_bend_round_trips_through_event() {
        let msg = MidiMessage::PitchBend {
            channel: 4,
            value: 0x0934,
        };
        let ev: Event = msg.clone().into();
        assert_eq!(ev.event_type, event_types::MIDI_PITCH_BEND);
        assert_eq!(ev.payload()[..], [4, 0x34, 0x12]);
        let back: MidiMessage = ev.try_into().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn midi_program_change_round_trips_through_event() {
        let msg = MidiMessage::ProgramChange {
            channel: 7,
            program: 42,
        };
        let ev: Event = msg.clone().into();
        assert_eq!(ev.event_type, event_types::MIDI_PROGRAM_CHANGE);
        let back: MidiMessage = ev.try_into().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn non_midi_event_does_not_convert_to_midi() {
        let ev = Event::new(event_types::CONTEXT, 0, 0, 0);
        assert_eq!(
            MidiMessage::try_from(ev).unwrap_err(),
            MidiConversionError::NotMidiEventType(event_types::CONTEXT)
        );
    }

    // --- Task 2: Context frame + inherited context ---

    #[test]
    fn context_event_serializes_and_deserializes() {
        let ctx = Context::new(
            42,
            1_000_000_000_000,
            600_000_000_i32,
            -1_200_000_000_i32,
            150,
            3,
        );
        let ev = Event::from_context(ctx, 1_000_000_000_100, 7, 1);
        assert_eq!(ev.event_type, event_types::CONTEXT);
        assert_eq!(ev.event_time, 1_000_000_000_100);
        assert_eq!(ev.source_id, 7);
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.payload_len, CONTEXT_PAYLOAD_LEN as u8);
        assert_eq!(ev.as_context(), Some(ctx));
    }

    #[test]
    fn event_before_any_context_has_no_context() {
        let mut router = EventRouter::new();
        let ev = Event::new(event_types::MIDI_NOTE_ON, 100, 5, 1);
        assert_eq!(router.current_context(), None);
        assert_eq!(router.handle_event(&ev), None);
    }

    #[test]
    fn event_inherits_last_context() {
        let mut router = EventRouter::new();

        let ctx = Context::new(
            42,
            1_000_000_000_000,
            600_000_000_i32,
            -1_200_000_000_i32,
            150,
            3,
        );
        router.broadcast_context(ctx);

        let ev = Event::new(event_types::MIDI_NOTE_ON, 1_000_000_000_100, 5, 10);
        let resolved = router.handle_event(&ev);
        assert_eq!(resolved, Some(ctx));
        assert_eq!(resolved.unwrap().vessel_id, 42);
    }

    #[test]
    fn context_updates_are_inherited_by_later_events() {
        let mut router = EventRouter::new();

        let ctx1 = Context::new(1, 100, 100_000_000, 200_000_000, 50, 1);
        router.broadcast_context(ctx1);

        let ev = Event::new(event_types::MIDI_CONTROL_CHANGE, 150, 2, 3);
        assert_eq!(router.handle_event(&ev), Some(ctx1));

        let ctx2 = Context::new(2, 200, -300_000_000, 400_000_000, 75, 2);
        router.broadcast_context(ctx2);

        let ev2 = Event::new(event_types::MIDI_NOTE_OFF, 250, 2, 4);
        assert_eq!(router.handle_event(&ev2), Some(ctx2));
        assert_ne!(ctx1, ctx2);
    }

    #[test]
    fn context_event_also_updates_inherited_context() {
        let mut router = EventRouter::new();

        let ctx = Context::new(99, 500, 100, 200, 10, 0);
        let ctx_event = Event::from_context(ctx, 500, 0, 0);
        assert_eq!(router.handle_event(&ctx_event), Some(ctx));

        let ev = Event::new(event_types::MIDI_PITCH_BEND, 600, 1, 5);
        assert_eq!(router.handle_event(&ev), Some(ctx));
    }
}
