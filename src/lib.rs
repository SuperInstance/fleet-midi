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
}
