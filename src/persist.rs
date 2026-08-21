//! Phase 2: Local Parquet persistence layer for the fleet event bus.
//!
//! Implements the durable Tier-2 store described in the proposal (§3.4, §5,
//! §7 Phase 1 items 4–5). Events decoded through the Phase 1 binary envelope
//! are persisted as rows in Parquet files, partitioned one file per
//! `(vessel, hour)`. A vessels/sources registry tracks known entities. The
//! tolerance-window query ("latest reading per source within ±2 seconds")
//! implements the synoptic query pattern from §5.5.
//!
//! # Storage layout
//! ```text
//! <base_dir>/
//!   data/
//!     vessel=<id>/
//!       hour=<unix_hour>/
//!         events.parquet
//!   registry/
//!     vessels.parquet
//!     sources.parquet
//! ```
//!
//! # Design note: DuckDB vs. pure-Rust (fallback taken)
//! The proposal recommends DuckDB+Parquet for the primary durable store.
//! DuckDB's Rust bindings (`duckdb` crate with `bundled` feature) compile a
//! large C++ library that exceeded the memory budget of this development
//! sandbox (~3.5 GB RAM; the build peaked at 3.3 GB and was killed before
//! completing). We therefore use the pure-Rust `parquet` + `arrow` crates
//! to write **genuine, valid Parquet files** on disk — the critical property
//! for the design — and implement the tolerance-window query logic in Rust.
//! Any future cloud DuckDB layer can read these Parquet files directly with
//! `SELECT * FROM 'events.parquet'`, no conversion needed.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use arrow::array::{
    Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

use crate::{Context, Event};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Nanoseconds in one hour. Used for `(vessel, hour)` partitioning.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Default tolerance for synoptic queries: ±2 seconds (§5.5).
pub const DEFAULT_TOLERANCE_NS: i64 = 2_000_000_000;

/// Additional event-type constants for persisted sensor events (§5.4).
///
/// Phase 1 defined the MIDI-compatible family and `CONTEXT`; these extend the
/// registry with the Tier-2-relevant types for scalar readings and bite
/// triggers.
pub mod persisted_event_types {
    /// Scalar sensor reading (value carried inline in the event payload).
    pub const SCALAR_READING: u16 = 0x0100;
    /// Bite-detector trigger.
    pub const BITE_TRIGGER: u16 = 0x0101;
}

// ---------------------------------------------------------------------------
// PersistedEvent — mirrors the Phase 1 Event + resolved Context envelope
// ---------------------------------------------------------------------------

/// A single persisted event row, combining the Phase 1 `Event` fields with the
/// inherited `Context` fields into one flat record (§5.3).
///
/// This is the logical schema both for the Parquet row layout and for the
/// in-memory representation returned by queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedEvent {
    pub vessel_id: u32,
    pub event_type: u16,
    pub event_time: i64,
    pub source_id: u16,
    pub seq: u32,
    pub fix_time: i64,
    pub lat_e7: i32,
    pub lon_e7: i32,
    pub hacc_cm: u16,
    pub clock_quality: u8,
}

impl PersistedEvent {
    /// Flatten a decoded `Event` together with its resolved `Context` into a
    /// persisted row.
    pub fn from_event(event: &Event, ctx: Context) -> Self {
        Self {
            vessel_id: ctx.vessel_id,
            event_type: event.event_type,
            event_time: event.event_time,
            source_id: event.source_id,
            seq: event.seq,
            fix_time: ctx.fix_time,
            lat_e7: ctx.lat_e7,
            lon_e7: ctx.lon_e7,
            hacc_cm: ctx.hacc_cm,
            clock_quality: ctx.clock_quality,
        }
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by persistence operations.
#[derive(Debug)]
pub enum PersistError {
    Io(io::Error),
    Arrow(arrow::error::ArrowError),
    Parquet(parquet::errors::ParquetError),
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Arrow(e) => write!(f, "Arrow error: {}", e),
            Self::Parquet(e) => write!(f, "Parquet error: {}", e),
        }
    }
}

impl std::error::Error for PersistError {}

impl From<io::Error> for PersistError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<arrow::error::ArrowError> for PersistError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e)
    }
}
impl From<parquet::errors::ParquetError> for PersistError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Parquet(e)
    }
}

// ---------------------------------------------------------------------------
// Schema definitions
// ---------------------------------------------------------------------------

/// Arrow schema for event Parquet partitions (§5.3 field layout).
fn event_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("vessel_id", DataType::UInt32, false),
        Field::new("event_type", DataType::UInt16, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("source_id", DataType::UInt16, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("fix_time", DataType::Int64, false),
        Field::new("lat_e7", DataType::Int32, false),
        Field::new("lon_e7", DataType::Int32, false),
        Field::new("hacc_cm", DataType::UInt16, false),
        Field::new("clock_quality", DataType::UInt8, false),
    ]))
}

/// Arrow schema for the `vessels` registry.
fn vessels_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("vessel_id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Arrow schema for the `sources` registry.
fn sources_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("source_id", DataType::UInt16, false),
        Field::new("vessel_id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

// ---------------------------------------------------------------------------
// Partition helpers
// ---------------------------------------------------------------------------

/// Compute the hour-bucket for a nanosecond timestamp (Euclidean division so
/// it is correct for all `i64` values).
fn hour_bucket(event_time: i64) -> i64 {
    event_time.div_euclid(NS_PER_HOUR)
}

/// Relative path from `base_dir` to a given partition's Parquet file.
fn partition_path(base: &Path, vessel_id: u32, bucket: i64) -> PathBuf {
    base.join("data")
        .join(format!("vessel={}", vessel_id))
        .join(format!("hour={}", bucket))
        .join("events.parquet")
}

// ---------------------------------------------------------------------------
// Low-level Parquet read/write
// ---------------------------------------------------------------------------

/// Build a single-row-per-event Arrow [`RecordBatch`] from a slice of
/// [`PersistedEvent`]s.
fn build_event_batch(events: &[PersistedEvent]) -> RecordBatch {
    let vessel_id: Vec<u32> = events.iter().map(|e| e.vessel_id).collect();
    let event_type: Vec<u16> = events.iter().map(|e| e.event_type).collect();
    let event_time: Vec<i64> = events.iter().map(|e| e.event_time).collect();
    let source_id: Vec<u16> = events.iter().map(|e| e.source_id).collect();
    let seq: Vec<u32> = events.iter().map(|e| e.seq).collect();
    let fix_time: Vec<i64> = events.iter().map(|e| e.fix_time).collect();
    let lat_e7: Vec<i32> = events.iter().map(|e| e.lat_e7).collect();
    let lon_e7: Vec<i32> = events.iter().map(|e| e.lon_e7).collect();
    let hacc_cm: Vec<u16> = events.iter().map(|e| e.hacc_cm).collect();
    let clock_quality: Vec<u8> = events.iter().map(|e| e.clock_quality).collect();

    RecordBatch::try_new(
        event_schema(),
        vec![
            std::sync::Arc::new(UInt32Array::from(vessel_id)),
            std::sync::Arc::new(UInt16Array::from(event_type)),
            std::sync::Arc::new(Int64Array::from(event_time)),
            std::sync::Arc::new(UInt16Array::from(source_id)),
            std::sync::Arc::new(UInt32Array::from(seq)),
            std::sync::Arc::new(Int64Array::from(fix_time)),
            std::sync::Arc::new(Int32Array::from(lat_e7)),
            std::sync::Arc::new(Int32Array::from(lon_e7)),
            std::sync::Arc::new(UInt16Array::from(hacc_cm)),
            std::sync::Arc::new(UInt8Array::from(clock_quality)),
        ],
    )
    .expect("schema mismatch in build_event_batch — this is a bug")
}

/// Write (or overwrite) a Parquet partition file containing `events`.
///
/// If the target file already exists, its rows are read back, merged with the
/// new events, sorted by `event_time`, and rewritten as a single file. This
/// read-merge-write cycle is acceptable for the edge prototype (small volumes
/// per hour per vessel); a production system would append row groups instead.
fn write_partition(path: &Path, events: &[PersistedEvent]) -> Result<(), PersistError> {
    if events.is_empty() {
        return Ok(());
    }

    // Merge with existing rows.
    let mut all = if path.exists() {
        read_partition(path).unwrap_or_default()
    } else {
        Vec::new()
    };
    all.extend_from_slice(events);
    all.sort_by_key(|e| e.event_time);
    all.dedup_by(|a, b| {
        a.vessel_id == b.vessel_id
            && a.source_id == b.source_id
            && a.seq == b.seq
            && a.event_type == b.event_type
    });

    // Ensure parent directory exists.
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    let batch = build_event_batch(&all);
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Read all [`PersistedEvent`] rows from a single Parquet partition file.
fn read_partition(path: &Path) -> Result<Vec<PersistedEvent>, PersistError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut events = Vec::new();
    for maybe_batch in reader {
        let batch = maybe_batch?;
        let n = batch.num_rows();

        let vessel_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("vessel_id column type");
        let event_type = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("event_type column type");
        let event_time = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("event_time column type");
        let source_id = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("source_id column type");
        let seq = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("seq column type");
        let fix_time = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("fix_time column type");
        let lat_e7 = batch
            .column(6)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("lat_e7 column type");
        let lon_e7 = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("lon_e7 column type");
        let hacc_cm = batch
            .column(8)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("hacc_cm column type");
        let clock_quality = batch
            .column(9)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("clock_quality column type");

        for i in 0..n {
            events.push(PersistedEvent {
                vessel_id: vessel_id.value(i),
                event_type: event_type.value(i),
                event_time: event_time.value(i),
                source_id: source_id.value(i),
                seq: seq.value(i),
                fix_time: fix_time.value(i),
                lat_e7: lat_e7.value(i),
                lon_e7: lon_e7.value(i),
                hacc_cm: hacc_cm.value(i),
                clock_quality: clock_quality.value(i),
            });
        }
    }
    Ok(events)
}

/// Read all events from partitions whose hour-bucket overlaps the
/// `[target_time - tolerance, target_time + tolerance]` window.
///
/// This avoids scanning every historical partition when only a narrow
/// tolerance window is queried.
fn read_events_in_window(
    base_dir: &Path,
    target_time: i64,
    tolerance_ns: i64,
) -> Vec<PersistedEvent> {
    let lo_hour = hour_bucket(target_time - tolerance_ns);
    let hi_hour = hour_bucket(target_time + tolerance_ns);

    let data_dir = base_dir.join("data");
    let mut all = Vec::new();

    if let Ok(vessel_entries) = fs::read_dir(&data_dir) {
        for vessel_entry in vessel_entries.flatten() {
            let vessel_path = vessel_entry.path();
            if !vessel_path.is_dir() {
                continue;
            }
            for bucket in lo_hour..=hi_hour {
                let path = vessel_path
                    .join(format!("hour={}", bucket))
                    .join("events.parquet");
                if path.exists() {
                    if let Ok(events) = read_partition(&path) {
                        all.extend(events);
                    }
                }
            }
        }
    }

    all
}

/// Read *all* events across every partition (used for full scans / tests).
fn read_all_events(base_dir: &Path) -> Vec<PersistedEvent> {
    let data_dir = base_dir.join("data");
    let mut all = Vec::new();

    fn walk(dir: &Path, out: &mut Vec<PersistedEvent>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                if let Ok(events) = read_partition(&path) {
                    out.extend(events);
                }
            }
        }
    }

    walk(&data_dir, &mut all);
    all
}

// ---------------------------------------------------------------------------
// Registry read/write
// ---------------------------------------------------------------------------

/// Persist the vessels registry to `<base>/registry/vessels.parquet`.
fn write_vessels(base: &Path, vessels: &BTreeMap<u32, String>) -> Result<(), PersistError> {
    let dir = base.join("registry");
    fs::create_dir_all(&dir)?;
    let path = dir.join("vessels.parquet");

    let ids: Vec<u32> = vessels.keys().copied().collect();
    let names: Vec<&str> = vessels.values().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        vessels_schema(),
        vec![
            std::sync::Arc::new(UInt32Array::from(ids)),
            std::sync::Arc::new(StringArray::from(names)),
        ],
    )?;

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Persist the sources registry to `<base>/registry/sources.parquet`.
fn write_sources(
    base: &Path,
    sources: &BTreeMap<(u16, u32), String>,
) -> Result<(), PersistError> {
    let dir = base.join("registry");
    fs::create_dir_all(&dir)?;
    let path = dir.join("sources.parquet");

    let sids: Vec<u16> = sources.keys().map(|(s, _)| *s).collect();
    let vids: Vec<u32> = sources.keys().map(|(_, v)| *v).collect();
    let names: Vec<&str> = sources.values().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        sources_schema(),
        vec![
            std::sync::Arc::new(UInt16Array::from(sids)),
            std::sync::Arc::new(UInt32Array::from(vids)),
            std::sync::Arc::new(StringArray::from(names)),
        ],
    )?;

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Load the vessels registry from disk (returns an empty map if absent).
fn load_vessels(base: &Path) -> BTreeMap<u32, String> {
    let path = base.join("registry").join("vessels.parquet");
    if !path.exists() {
        return BTreeMap::new();
    }
    let mut map = BTreeMap::new();
    if let Ok(file) = File::open(&path) {
        if let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) {
            if let Ok(reader) = builder.build() {
                for batch in reader.flatten() {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .expect("vessel_id column");
                    let names = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("name column");
                    for i in 0..batch.num_rows() {
                        map.insert(ids.value(i), names.value(i).to_string());
                    }
                }
            }
        }
    }
    map
}

/// Load the sources registry from disk (returns an empty map if absent).
fn load_sources(base: &Path) -> BTreeMap<(u16, u32), String> {
    let path = base.join("registry").join("sources.parquet");
    if !path.exists() {
        return BTreeMap::new();
    }
    let mut map = BTreeMap::new();
    if let Ok(file) = File::open(&path) {
        if let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) {
            if let Ok(reader) = builder.build() {
                for batch in reader.flatten() {
                    let sids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt16Array>()
                        .expect("source_id column");
                    let vids = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .expect("vessel_id column");
                    let names = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("name column");
                    for i in 0..batch.num_rows() {
                        map.insert(
                            (sids.value(i), vids.value(i)),
                            names.value(i).to_string(),
                        );
                    }
                }
            }
        }
    }
    map
}

// ---------------------------------------------------------------------------
// EventStore — the main persistence facade
// ---------------------------------------------------------------------------

/// Durable event store backed by Parquet files on disk.
///
/// Events are accumulated in an in-memory buffer partitioned by
/// `(vessel_id, hour_bucket)`. [`EventStore::flush`] writes each partition to
/// its Parquet file (merging with any existing rows) and persists the
/// vessels/sources registry.
///
/// Typical usage:
/// ```ignore
/// let mut store = EventStore::open("/data/fleet")?;
/// store.register_vessel(42, "F/V Aurora");
/// for event in event_stream {
///     let ctx = router.handle_event(&event).unwrap();
///     store.persist(&event, ctx);
/// }
/// store.flush()?;
/// let latest = store.query_latest_within_tolerance(now, DEFAULT_TOLERANCE_NS, None);
/// ```
pub struct EventStore {
    base_dir: PathBuf,
    /// Buffered events keyed by `(vessel_id, hour_bucket)`.
    buffer: HashMap<(u32, i64), Vec<PersistedEvent>>,
    /// Vessel registry: `vessel_id → name`.
    vessels: BTreeMap<u32, String>,
    /// Source registry: `(source_id, vessel_id) → name`.
    sources: BTreeMap<(u16, u32), String>,
}

impl EventStore {
    /// Open or create an event store rooted at `base_dir`.
    ///
    /// Existing registry files are loaded; existing event partitions are left
    /// untouched (they will be merged with buffered events on flush).
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self, PersistError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)?;
        let vessels = load_vessels(&base_dir);
        let sources = load_sources(&base_dir);
        Ok(Self {
            base_dir,
            buffer: HashMap::new(),
            vessels,
            sources,
        })
    }

    /// Register a vessel name.
    pub fn register_vessel(&mut self, vessel_id: u32, name: &str) {
        self.vessels.insert(vessel_id, name.to_string());
    }

    /// Register a source name.
    pub fn register_source(&mut self, source_id: u16, vessel_id: u32, name: &str) {
        self.sources
            .insert((source_id, vessel_id), name.to_string());
    }

    /// Buffer an event for later persistence.
    ///
    /// The event's fields are combined with the resolved `Context` (typically
    /// obtained from [`crate::EventRouter::handle_event`]) to form a complete
    /// persisted row.
    pub fn persist(&mut self, event: &Event, ctx: Context) {
        let row = PersistedEvent::from_event(event, ctx);
        let key = (row.vessel_id, hour_bucket(row.event_time));
        self.buffer.entry(key).or_default().push(row);
        // Auto-register the vessel if not already known.
        self.vessels
            .entry(ctx.vessel_id)
            .or_insert_with(|| format!("vessel_{}", ctx.vessel_id));
    }

    /// Write all buffered events to Parquet files and persist the registry.
    ///
    /// After flushing, the in-memory buffer is cleared.
    pub fn flush(&mut self) -> Result<(), PersistError> {
        let partitions: Vec<_> = self.buffer.drain().collect();
        for ((vessel_id, bucket), events) in &partitions {
            let path = partition_path(&self.base_dir, *vessel_id, *bucket);
            write_partition(&path, events)?;
        }
        write_vessels(&self.base_dir, &self.vessels)?;
        write_sources(&self.base_dir, &self.sources)?;
        Ok(())
    }

    /// Return the number of buffered (not yet flushed) events.
    pub fn pending_count(&self) -> usize {
        self.buffer.values().map(|v| v.len()).sum()
    }

    /// Return the base directory of this store.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Query the latest reading per `(source_id, event_type)` within
    /// `±tolerance_ns` of `target_time`.
    ///
    /// This implements the §5.5 tolerance-window synoptic pattern: since
    /// sources don't share a wall clock, we find the nearest reading within
    /// the window rather than exact-timestamp equality.
    ///
    /// If `event_type_filter` is `Some`, only events of that type are
    /// considered.
    ///
    /// Reads from Parquet on disk; call [`flush`] first to include recently
    /// buffered events.
    ///
    /// [`flush`]: EventStore::flush
    pub fn query_latest_within_tolerance(
        &self,
        target_time: i64,
        tolerance_ns: i64,
        event_type_filter: Option<u16>,
    ) -> Vec<PersistedEvent> {
        let candidates = read_events_in_window(&self.base_dir, target_time, tolerance_ns);
        select_latest_within_tolerance(candidates, target_time, tolerance_ns, event_type_filter)
    }

    /// Read all persisted events (full scan, mainly for tests).
    pub fn read_all(&self) -> Vec<PersistedEvent> {
        read_all_events(&self.base_dir)
    }

    /// Return a reference to the vessels registry.
    pub fn vessels(&self) -> &BTreeMap<u32, String> {
        &self.vessels
    }

    /// Return a reference to the sources registry.
    pub fn sources(&self) -> &BTreeMap<(u16, u32), String> {
        &self.sources
    }
}

// ---------------------------------------------------------------------------
// Tolerance-window selection logic (pure function, separately testable)
// ---------------------------------------------------------------------------

/// From a set of candidate events, select the one closest to `target_time`
/// per `(source_id, event_type)` group, keeping only those within tolerance.
///
/// Ties (two events equidistant from target) are broken by preferring the
/// event with the **later** `event_time`.
fn select_latest_within_tolerance(
    candidates: Vec<PersistedEvent>,
    target_time: i64,
    tolerance_ns: i64,
    event_type_filter: Option<u16>,
) -> Vec<PersistedEvent> {
    let mut best: HashMap<(u16, u16), PersistedEvent> = HashMap::new();

    for ev in candidates {
        if let Some(ft) = event_type_filter {
            if ev.event_type != ft {
                continue;
            }
        }
        let delta = (ev.event_time - target_time).abs();
        // Boundary is inclusive: delta == tolerance_ns is included.
        if delta > tolerance_ns {
            continue;
        }
        let key = (ev.source_id, ev.event_type);
        match best.get(&key) {
            Some(cur) => {
                let cur_delta = (cur.event_time - target_time).abs();
                if delta < cur_delta || (delta == cur_delta && ev.event_time > cur.event_time) {
                    best.insert(key, ev);
                }
            }
            None => {
                best.insert(key, ev);
            }
        }
    }

    let mut results: Vec<_> = best.into_values().collect();
    results.sort_by_key(|e| (e.source_id, e.event_type));
    results
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Unique temp directory that cleans up on drop (avoids adding `tempfile`
/// as a dev-dependency).
#[cfg(test)]
pub(crate) struct TestDir(PathBuf);

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
impl TestDir {
    pub fn new() -> Self {
        let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir()
            .join(format!("fleet-midi-test-{}-{}", std::process::id(), n));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create test dir");
        TestDir(path)
    }
}

#[cfg(test)]
impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[cfg(test)]
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode_event, decode_event, Event, Context};
    use persisted_event_types::*;

    /// Convenience: encode → decode → persist a single event, verifying the
    /// Phase 1 binary round-trip along the way.
    fn encode_decode_persist(
        store: &mut EventStore,
        event: &Event,
        ctx: Context,
    ) -> PersistedEvent {
        let bytes = encode_event(event);
        let decoded = decode_event(&bytes).expect("decode should succeed");
        assert_eq!(decoded, *event, "Phase 1 encode/decode round-trip");
        store.persist(&decoded, ctx);
        PersistedEvent::from_event(&decoded, ctx)
    }

    // -----------------------------------------------------------------------
    // Round-trip: write through Phase 1 decode path, read back, verify fields
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_scalar_reading_all_fields_preserved() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let base_time: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(42, base_time, 600_000_000, -1_200_000_000, 150, 3);

        let mut ev = Event::new(SCALAR_READING, base_time + 100_000, 7, 42);
        ev.set_payload(&[0xDE, 0xAD, 0xBE, 0xEF]); // temperature bytes, etc.

        let expected = encode_decode_persist(&mut store, &ev, ctx);
        store.flush().unwrap();
        assert_eq!(store.pending_count(), 0);

        let all = store.read_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0], expected);
        // Verify every field individually for clarity.
        assert_eq!(all[0].vessel_id, 42);
        assert_eq!(all[0].event_type, SCALAR_READING);
        assert_eq!(all[0].event_time, base_time + 100_000);
        assert_eq!(all[0].source_id, 7);
        assert_eq!(all[0].seq, 42);
        assert_eq!(all[0].fix_time, ctx.fix_time);
        assert_eq!(all[0].lat_e7, ctx.lat_e7);
        assert_eq!(all[0].lon_e7, ctx.lon_e7);
        assert_eq!(all[0].hacc_cm, ctx.hacc_cm);
        assert_eq!(all[0].clock_quality, ctx.clock_quality);
    }

    #[test]
    fn round_trip_bite_trigger() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let base_time: i64 = 2_000_000_000_000_000_000;
        let ctx = Context::new(7, base_time, -350_000_000, 1_800_000_000, 300, 2);

        let ev = Event::new(BITE_TRIGGER, base_time + 50_000, 3, 1);
        let expected = encode_decode_persist(&mut store, &ev, ctx);
        store.flush().unwrap();

        let all = store.read_all();
        assert_eq!(all, vec![expected]);
    }

    #[test]
    fn round_trip_multiple_events_multiple_sources() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let base_time: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(1, base_time, 500_000_000, -1_000_000_000, 100, 3);

        let mut events = Vec::new();
        for i in 0..10u32 {
            let mut ev = Event::new(SCALAR_READING, base_time + i as i64 * 1_000_000, (i % 3) as u16, i);
            ev.set_payload(&[(i % 256) as u8]);
            events.push(ev);
        }

        let mut expected_rows = Vec::new();
        for ev in &events {
            let bytes = encode_event(ev);
            let decoded = decode_event(&bytes).unwrap();
            assert_eq!(&decoded, ev);
            store.persist(&decoded, ctx);
            expected_rows.push(PersistedEvent::from_event(&decoded, ctx));
        }
        store.flush().unwrap();

        let mut all = store.read_all();
        all.sort_by_key(|e| e.seq);
        assert_eq!(all.len(), expected_rows.len());
        for (a, b) in all.iter().zip(&expected_rows) {
            assert_eq!(a, b);
        }
    }

    // -----------------------------------------------------------------------
    // Tolerance-window query — the critical §5.5 test
    // -----------------------------------------------------------------------

    #[test]
    fn tolerance_window_query_finds_closest_per_source() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        // Use a realistic GPS-era timestamp.
        let t: i64 = 1_000_000_000_000_000_000;
        let tol = DEFAULT_TOLERANCE_NS; // ±2s

        let ctx = Context::new(1, t, 600_000_000, -1_200_000_000, 150, 3);

        // Source 1: three readings — T+0.5s (closest), T-1s (farther),
        // T-3s (outside window).
        let s1_closest = Event::new(SCALAR_READING, t + 500_000_000, 1, 10);
        let s1_far = Event::new(SCALAR_READING, t - 1_000_000_000, 1, 20);
        let s1_outside = Event::new(SCALAR_READING, t - 3_000_000_000, 1, 30);

        // Source 2: bite triggers — T-2s (at boundary), T+2s (at boundary,
        // equidistant but later), T+2s+1ns (just outside).
        let s2_lo = Event::new(BITE_TRIGGER, t - 2_000_000_000, 2, 100);
        let s2_hi = Event::new(BITE_TRIGGER, t + 2_000_000_000, 2, 200);
        let s2_outside = Event::new(BITE_TRIGGER, t + 2_000_000_001, 2, 300);

        // Source 3: exactly at negative boundary (T-2s), and 1ns beyond it.
        let s3_boundary = Event::new(SCALAR_READING, t - 2_000_000_000, 3, 1);
        let s3_outside = Event::new(SCALAR_READING, t - 2_000_000_001, 3, 2);

        for ev in &[
            &s1_closest, &s1_far, &s1_outside,
            &s2_lo, &s2_hi, &s2_outside,
            &s3_boundary, &s3_outside,
        ] {
            // Encode/decode through Phase 1 to verify real wire path.
            let bytes = encode_event(ev);
            let decoded = decode_event(&bytes).unwrap();
            assert_eq!(decoded, **ev);
            store.persist(&decoded, ctx);
        }
        store.flush().unwrap();

        let results = store.query_latest_within_tolerance(t, tol, None);

        // Exactly 3 (source_id, event_type) groups should be represented.
        assert_eq!(results.len(), 3, "expected one result per source/type group");

        // Source 1 SCALAR_READING → T+0.5s (closest within window).
        let r1 = results.iter().find(|e| e.source_id == 1).unwrap();
        assert_eq!(r1.event_time, t + 500_000_000);
        assert_eq!(r1.seq, 10);
        assert_eq!(r1.event_type, SCALAR_READING);

        // Source 2 BITE_TRIGGER → T+2s wins tie over T-2s (later event_time).
        let r2 = results.iter().find(|e| e.source_id == 2).unwrap();
        assert_eq!(r2.event_time, t + 2_000_000_000);
        assert_eq!(r2.seq, 200);
        assert_eq!(r2.event_type, BITE_TRIGGER);

        // Source 3 SCALAR_READING → T-2s (at boundary, included).
        let r3 = results.iter().find(|e| e.source_id == 3).unwrap();
        assert_eq!(r3.event_time, t - 2_000_000_000);
        assert_eq!(r3.seq, 1);
    }

    #[test]
    fn tolerance_boundary_is_inclusive() {
        // Isolated boundary check: the exact ±tolerance timestamps must be
        // included, and 1ns beyond must be excluded.
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 5_000_000_000_000_000_000;
        let tol: i64 = 2_000_000_000;
        let ctx = Context::new(1, t, 0, 0, 0, 0);

        // Exactly at +tolerance — should be included.
        let at_pos_boundary = Event::new(SCALAR_READING, t + tol, 1, 1);
        // Exactly at -tolerance — should be included.
        let at_neg_boundary = Event::new(SCALAR_READING, t - tol, 2, 1);
        // 1ns beyond +tolerance — should be excluded.
        let past_pos = Event::new(SCALAR_READING, t + tol + 1, 3, 1);
        // 1ns beyond -tolerance — should be excluded.
        let past_neg = Event::new(SCALAR_READING, t - tol - 1, 4, 1);

        for ev in &[&at_pos_boundary, &at_neg_boundary, &past_pos, &past_neg] {
            let bytes = encode_event(ev);
            let decoded = decode_event(&bytes).unwrap();
            store.persist(&decoded, ctx);
        }
        store.flush().unwrap();

        let results = store.query_latest_within_tolerance(t, tol, None);

        // Sources 1 and 2 (at boundary) are in; sources 3 and 4 (past) are out.
        let returned_sources: Vec<u16> = results.iter().map(|e| e.source_id).collect();
        assert!(returned_sources.contains(&1), "source at +boundary must be included");
        assert!(returned_sources.contains(&2), "source at -boundary must be included");
        assert!(!returned_sources.contains(&3), "source past +boundary must be excluded");
        assert!(!returned_sources.contains(&4), "source past -boundary must be excluded");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn tolerance_query_with_event_type_filter() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(1, t, 0, 0, 0, 0);

        // Same source, different event types at the same time.
        let scalar = Event::new(SCALAR_READING, t, 5, 1);
        let bite = Event::new(BITE_TRIGGER, t, 5, 2);

        for ev in &[&scalar, &bite] {
            let bytes = encode_event(ev);
            let decoded = decode_event(&bytes).unwrap();
            store.persist(&decoded, ctx);
        }
        store.flush().unwrap();

        let all = store.query_latest_within_tolerance(t, 1_000_000, None);
        assert_eq!(all.len(), 2, "both event types without filter");

        let only_scalar = store.query_latest_within_tolerance(t, 1_000_000, Some(SCALAR_READING));
        assert_eq!(only_scalar.len(), 1);
        assert_eq!(only_scalar[0].event_type, SCALAR_READING);

        let only_bite = store.query_latest_within_tolerance(t, 1_000_000, Some(BITE_TRIGGER));
        assert_eq!(only_bite.len(), 1);
        assert_eq!(only_bite[0].event_type, BITE_TRIGGER);
    }

    #[test]
    fn empty_query_returns_empty_vec() {
        let dir = TestDir::new();
        let store = EventStore::open(&dir).unwrap();
        let results = store.query_latest_within_tolerance(0, DEFAULT_TOLERANCE_NS, None);
        assert!(results.is_empty());
    }

    // -----------------------------------------------------------------------
    // Partitioning: events go to the right (vessel, hour) files
    // -----------------------------------------------------------------------

    #[test]
    fn events_partitioned_by_vessel_and_hour() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx_a = Context::new(1, t, 100, 200, 10, 1);
        let ctx_b = Context::new(2, t, 300, 400, 20, 2);

        // Same hour, two vessels → two partition files.
        store.persist(&Event::new(SCALAR_READING, t, 1, 1), ctx_a);
        store.persist(&Event::new(SCALAR_READING, t + 10, 2, 1), ctx_b);

        // Different hour (next hour boundary).
        let t2 = t + NS_PER_HOUR + 1;
        let ctx_a2 = Context::new(1, t2, 100, 200, 10, 1);
        store.persist(&Event::new(SCALAR_READING, t2, 1, 2), ctx_a2);

        store.flush().unwrap();

        // Verify file paths.
        let bucket1 = hour_bucket(t);
        let bucket2 = hour_bucket(t2);

        let path_a1 = partition_path(dir.as_ref(), 1, bucket1);
        let path_b1 = partition_path(dir.as_ref(), 2, bucket1);
        let path_a2 = partition_path(dir.as_ref(), 1, bucket2);

        assert!(path_a1.exists(), "vessel=1, hour={} should exist", bucket1);
        assert!(path_b1.exists(), "vessel=2, hour={} should exist", bucket1);
        assert!(path_a2.exists(), "vessel=1, hour={} should exist", bucket2);
        assert!(path_b1 != path_a1, "different vessels get different files");
    }

    #[test]
    fn flush_merges_into_existing_partition() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(1, t, 0, 0, 0, 0);

        // First batch.
        store.persist(&Event::new(SCALAR_READING, t, 1, 1), ctx);
        store.persist(&Event::new(SCALAR_READING, t + 1, 1, 2), ctx);
        store.flush().unwrap();

        // Second batch, same partition.
        store.persist(&Event::new(SCALAR_READING, t + 2, 1, 3), ctx);
        store.flush().unwrap();

        let all = store.read_all();
        assert_eq!(all.len(), 3, "all three events should be present after merge");
        let seqs: Vec<u32> = all.iter().map(|e| e.seq).collect();
        assert!(seqs.contains(&1) && seqs.contains(&2) && seqs.contains(&3));
    }

    // -----------------------------------------------------------------------
    // Registry
    // -----------------------------------------------------------------------

    #[test]
    fn registry_persists_and_reloads() {
        let dir = TestDir::new();

        {
            let mut store = EventStore::open(&dir).unwrap();
            store.register_vessel(42, "F/V Aurora");
            store.register_vessel(7, "F/V Northstar");
            store.register_source(1, 42, "depth-sounder");
            store.register_source(2, 42, "temp-sensor");
            store.register_source(3, 7, "bite-detector");
            store.flush().unwrap();
        }

        // Re-open and verify registry loaded from disk.
        let store = EventStore::open(&dir).unwrap();
        assert_eq!(store.vessels().get(&42), Some(&"F/V Aurora".to_string()));
        assert_eq!(store.vessels().get(&7), Some(&"F/V Northstar".to_string()));
        assert_eq!(
            store.sources().get(&(1, 42)),
            Some(&"depth-sounder".to_string())
        );
        assert_eq!(
            store.sources().get(&(3, 7)),
            Some(&"bite-detector".to_string())
        );
    }

    #[test]
    fn vessel_auto_registered_on_persist() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(99, t, 0, 0, 0, 0);
        store.persist(&Event::new(SCALAR_READING, t, 1, 1), ctx);
        store.flush().unwrap();

        let store2 = EventStore::open(&dir).unwrap();
        assert!(store2.vessels().contains_key(&99));
    }

    // -----------------------------------------------------------------------
    // Cross-vessel query
    // -----------------------------------------------------------------------

    #[test]
    fn query_spans_multiple_vessels() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx_a = Context::new(1, t, 100, 200, 10, 3);
        let ctx_b = Context::new(2, t + 500_000_000, 300, 400, 20, 3);

        // Both vessels have depth readings near time T.
        store.persist(&Event::new(SCALAR_READING, t + 100_000, 1, 1), ctx_a);
        store.persist(&Event::new(SCALAR_READING, t + 200_000, 2, 1), ctx_b);
        store.flush().unwrap();

        let results = store.query_latest_within_tolerance(t, DEFAULT_TOLERANCE_NS, Some(SCALAR_READING));
        // Two different sources (source_id 1 on vessel 1, source_id 2 on vessel 2).
        assert_eq!(results.len(), 2);
        let vids: Vec<u32> = results.iter().map(|e| e.vessel_id).collect();
        assert!(vids.contains(&1) && vids.contains(&2));
    }

    // -----------------------------------------------------------------------
    // Real Parquet file verification (not just "tests pass")
    // -----------------------------------------------------------------------

    #[test]
    fn parquet_files_exist_and_have_content() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(1, t, 600_000_000, -1_200_000_000, 150, 3);

        for i in 0..5u32 {
            store.persist(
                &Event::new(SCALAR_READING, t + i as i64 * 1_000_000_000, 1, i),
                ctx,
            );
        }
        store.flush().unwrap();

        // Verify the Parquet file exists and is non-empty.
        let bucket = hour_bucket(t);
        let path = partition_path(dir.as_ref(), 1, bucket);
        assert!(path.exists(), "Parquet file should exist at {:?}", path);

        let metadata = fs::metadata(&path).unwrap();
        assert!(
            metadata.len() > 0,
            "Parquet file should be non-empty (got {} bytes)",
            metadata.len()
        );
        // A Parquet file with 5 rows and 10 columns should be at least a few hundred bytes
        // (footer + row group metadata + data).
        assert!(
            metadata.len() > 100,
            "Parquet file seems suspiciously small: {} bytes",
            metadata.len()
        );

        // Also verify registry files exist.
        let vessels_path = dir.as_ref().join("registry").join("vessels.parquet");
        assert!(vessels_path.exists(), "vessels registry should exist");
        let sources_path = dir.as_ref().join("registry").join("sources.parquet");
        assert!(sources_path.exists(), "sources registry should exist");

        eprintln!(
            "\n  [persist test] Parquet file: {:?} ({} bytes)\n",
            path,
            metadata.len()
        );
    }

    /// Verify that the written Parquet files are readable by a fresh,
    /// independent reader (simulating a cloud DuckDB reading the same file).
    #[test]
    fn parquet_file_readable_by_independent_reader() {
        let dir = TestDir::new();
        let mut store = EventStore::open(&dir).unwrap();

        let t: i64 = 1_000_000_000_000_000_000;
        let ctx = Context::new(5, t, 123_456_789, -987_654_321, 250, 2);
        store.persist(&Event::new(SCALAR_READING, t, 3, 7), ctx);
        store.flush().unwrap();

        // Open a completely new store at the same path — it should see the
        // persisted data without any in-memory state.
        let store2 = EventStore::open(&dir).unwrap();
        let all = store2.read_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].vessel_id, 5);
        assert_eq!(all[0].lat_e7, 123_456_789);
        assert_eq!(all[0].lon_e7, -987_654_321);
    }
}
