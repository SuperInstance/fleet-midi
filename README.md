# fleet-midi

MIDI message parsing and fleet broadcast for constellation agents.

**Status:** Early stage — scaffolded, building, tests passing.

## What it does

Parses incoming MIDI messages and routes them across a fleet of agents.
Designed as a thin layer that turns MIDI events into structured messages
suitable for broadcast over the fleet mesh.

## Building

```sh
cargo build
cargo test
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
