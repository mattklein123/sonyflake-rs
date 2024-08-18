use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

use crate::{builder::Builder, error::*};

/// bit length of time
pub(crate) const BIT_LEN_TIME: u64 = 39;
/// bit length of sequence number
pub(crate) const BIT_LEN_SEQUENCE: u64 = 9;
/// bit length of machine id
pub(crate) const BIT_LEN_MACHINE_ID: u64 = 64 - BIT_LEN_TIME - BIT_LEN_SEQUENCE;

const GENERATE_MASK_SEQUENCE: u16 = (1 << BIT_LEN_SEQUENCE) - 1;

#[derive(Debug)]
pub(crate) struct Internals {
    pub(crate) elapsed_time: i64,
    pub(crate) sequence: u16,
}

pub(crate) struct SharedSonyflake {
    pub(crate) start_time: i64,
    pub(crate) machine_id: u16,
    pub(crate) internals: Mutex<Internals>,
}

/// Sonyflake is a distributed unique ID generator.
pub struct Sonyflake(pub(crate) Arc<SharedSonyflake>);

impl Sonyflake {
    /// Create a new Sonyflake with the default configuration.
    /// For custom configuration see [`builder`].
    ///
    /// [`builder`]: struct.Sonyflake.html#method.builder
    pub fn new() -> Result<Self, Error> {
        Builder::new().finalize()
    }

    /// Create a new [`Builder`] to construct a Sonyflake.
    ///
    /// [`Builder`]: struct.Builder.html
    pub fn builder<'a>() -> Builder<'a> {
        Builder::new()
    }

    pub(crate) fn new_inner(shared: Arc<SharedSonyflake>) -> Self {
        Self(shared)
    }

    pub fn min_sonyflake_for_time(&self, time: OffsetDateTime) -> u64 {
        ((to_sonyflake_time(time) - self.0.start_time) as u64)
            << (BIT_LEN_SEQUENCE + BIT_LEN_MACHINE_ID)
    }

    /// Generate the next unique id.
    /// After the Sonyflake time overflows, next_id returns an error.
    pub fn next_id(&self, now: OffsetDateTime) -> Result<u64, Error> {
        let mut internals = self.0.internals.lock().unwrap();

        let current = current_elapsed_time(now, self.0.start_time);
        if internals.elapsed_time < current {
            internals.elapsed_time = current;
            internals.sequence = 0;
        } else {
            // self.elapsed_time >= current
            let next_sequence = (internals.sequence + 1) & GENERATE_MASK_SEQUENCE;
            if next_sequence == 0 {
                // Overflowed. Caller will need to sleep or handle.
                return Err(Error::OverSequenceLimit);
            } else {
                internals.sequence = next_sequence;
            }
        }

        if internals.elapsed_time >= 1 << BIT_LEN_TIME {
            return Err(Error::OverTimeLimit);
        }

        Ok(
            (internals.elapsed_time as u64) << (BIT_LEN_SEQUENCE + BIT_LEN_MACHINE_ID)
                | (internals.sequence as u64) << BIT_LEN_MACHINE_ID
                | (self.0.machine_id as u64),
        )
    }
}

/// Returns a new `Sonyflake` referencing the same state as `self`.
impl Clone for Sonyflake {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

const SONYFLAKE_TIME_UNIT: i64 = 10_000_000; // nanoseconds, i.e. 10msec

pub(crate) fn to_sonyflake_time(time: OffsetDateTime) -> i64 {
    time.unix_timestamp_nanos() as i64 / SONYFLAKE_TIME_UNIT
}

fn current_elapsed_time(now: OffsetDateTime, start_time: i64) -> i64 {
    to_sonyflake_time(now) - start_time
}

pub struct DecomposedSonyflake {
    pub id: u64,
    pub time: u64,
    pub sequence: u64,
    pub machine_id: u64,
}

impl DecomposedSonyflake {
    /// Returns the timestamp in nanoseconds without epoch.
    pub fn nanos_time(&self) -> i64 {
        (self.time as i64) * SONYFLAKE_TIME_UNIT
    }
}

const DECOMPOSE_MASK_SEQUENCE: u64 = ((1 << BIT_LEN_SEQUENCE) - 1) << BIT_LEN_MACHINE_ID;

const MASK_MACHINE_ID: u64 = (1 << BIT_LEN_MACHINE_ID) - 1;

/// Break a Sonyflake ID up into its parts.
pub fn decompose(id: u64) -> DecomposedSonyflake {
    DecomposedSonyflake {
        id,
        time: id >> (BIT_LEN_SEQUENCE + BIT_LEN_MACHINE_ID),
        sequence: (id & DECOMPOSE_MASK_SEQUENCE) >> BIT_LEN_MACHINE_ID,
        machine_id: id & MASK_MACHINE_ID,
    }
}
