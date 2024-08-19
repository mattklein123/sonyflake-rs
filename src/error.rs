use std::error::Error as StdError;
use thiserror::Error;
use time::OffsetDateTime;

/// Convenience type alias for usage within sonyflake.
pub(crate) type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// The error type for this crate.
#[derive(Error, Debug)]
pub enum Error {
    #[error("start_time `{0}` is ahead of current time")]
    StartTimeAheadOfCurrentTime(OffsetDateTime),
    #[error("machine_id returned an error: {0}")]
    MachineIdFailed(#[source] BoxDynError),
    #[error("check_machine_id returned false")]
    CheckMachineIdFailed,
    #[error("over the time limit")]
    OverTimeLimit,
    #[error("over the sequence limit")]
    OverSequenceLimit,
    #[error("could not find any private ipv4 address")]
    NoPrivateIPv4,
}
