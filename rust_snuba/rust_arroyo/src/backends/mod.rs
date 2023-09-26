use super::types::{BrokerMessage, Partition, Topic, TopicOrPartition};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;

pub mod kafka;
pub mod local;
pub mod storages;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("End of partition reached")]
    EndOfPartition,

    #[error("Not subscribed to a topic")]
    NotSubscribed,

    #[error("The consumer errored")]
    ConsumerErrored,

    #[error("The consumer is closed")]
    ConsumerClosed,

    #[error("Partition not assigned to consumer")]
    UnassignedPartition,

    #[error("Offset out of range")]
    OffsetOutOfRange { source: Box<dyn std::error::Error> },

    #[error(transparent)]
    BrokerError(#[from] Box<dyn std::error::Error>),
}

/// This is basically an observer pattern to receive the callbacks from
/// the consumer when partitions are assigned/revoked.
pub trait AssignmentCallbacks: Send + Sync {
    fn on_assign(&mut self, partitions: HashMap<Partition, u64>);
    fn on_revoke(&mut self, partitions: Vec<Partition>);
}

/// This abstract class provides an interface for consuming messages from a
/// multiplexed collection of partitioned topic streams.
///
/// Partitions support sequential access, as well as random access by
/// offsets. There are three types of offsets that a consumer interacts with:
/// working offsets, staged offsets, and committed offsets. Offsets always
/// represent the starting offset of the *next* message to be read. (For
/// example, committing an offset of X means the next message fetched via
/// poll will have a least an offset of X, and the last message read had an
/// offset less than X.)
///
/// The working offsets are used track the current read position within a
/// partition. This can be also be considered as a cursor, or as high
/// watermark. Working offsets are local to the consumer process. They are
/// not shared with other consumer instances in the same consumer group and
/// do not persist beyond the lifecycle of the consumer instance, unless they
/// are committed.
///
/// Committed offsets are managed by an external arbiter/service, and are
/// used as the starting point for a consumer when it is assigned a partition
/// during the subscription process. To ensure that a consumer roughly "picks
/// up where it left off" after restarting, or that another consumer in the
/// same group doesn't read messages that have been processed by another
/// consumer within the same group during a rebalance operation, positions must
/// be regularly committed by calling ``commit_offsets`` after they have been
/// staged with ``stage_offsets``. Offsets are not staged or committed
/// automatically!
///
/// During rebalance operations, working offsets are rolled back to the
/// latest committed offset for a partition, and staged offsets are cleared
/// after the revocation callback provided to ``subscribe`` is called. (This
/// occurs even if the consumer retains ownership of the partition across
/// assignments.) For this reason, it is generally good practice to ensure
/// offsets are committed as part of the revocation callback.
pub trait Consumer<'a, TPayload: Clone> {
    fn subscribe(
        &mut self,
        topic: &[Topic],
        callbacks: Box<dyn AssignmentCallbacks>,
    ) -> Result<(), ConsumerError>;

    fn unsubscribe(&mut self) -> Result<(), ConsumerError>;

    /// Fetch a message from the consumer. If no message is available before
    /// the timeout, ``None`` is returned.
    ///
    /// This method may raise an ``OffsetOutOfRange`` exception if the
    /// consumer attempts to read from an invalid location in one of it's
    /// assigned partitions. (Additional details can be found in the
    /// docstring for ``Consumer.seek``.)
    fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<BrokerMessage<TPayload>>, ConsumerError>;

    /// Pause consuming from the provided partitions.
    ///
    /// A partition that is paused will be automatically resumed during
    /// reassignment. This ensures that the behavior is consistent during
    /// rebalances, regardless of whether or not this consumer retains
    /// ownership of the partition. (If this partition was assigned to a
    /// different consumer in the consumer group during a rebalance, that
    /// consumer would not have knowledge of whether or not the partition was
    /// previously paused and would start consuming from the partition.) If
    /// partitions should remain paused across rebalances, this should be
    /// implemented in the assignment callback.
    ///
    /// If any of the provided partitions are not in the assignment set, an
    /// exception will be raised and no partitions will be paused.
    fn pause(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError>;

    /// Resume consuming from the provided partitions.
    ///
    /// If any of the provided partitions are not in the assignment set, an
    /// exception will be raised and no partitions will be resumed.
    fn resume(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError>;

    /// Return the currently paused partitions.
    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError>;

    /// Return the working offsets for all currently assigned positions.
    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError>;

    /// Update the working offsets for the provided partitions.
    ///
    /// When using this method, it is possible to set a partition to an
    /// invalid offset without an immediate error. (Examples of invalid
    /// offsets include an offset that is too low and has already been
    /// dropped by the broker due to data retention policies, or an offset
    /// that is too high which is not yet associated with a message.) Since
    /// this method only updates the local working offset (and does not
    /// communicate with the broker), setting an invalid offset will cause a
    /// subsequent ``poll`` call to raise ``OffsetOutOfRange`` exception,
    /// even though the call to ``seek`` succeeded.
    ///
    /// If any provided partitions are not in the assignment set, an
    /// exception will be raised and no offsets will be modified.
    fn seek(&self, offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError>;

    /// Stage offsets to be committed. If an offset has already been staged
    /// for a given partition, that offset is overwritten (even if the offset
    /// moves in reverse.)
    fn stage_offsets(&mut self, positions: HashMap<Partition, u64>) -> Result<(), ConsumerError>;

    /// Commit staged offsets. The return value of this method is a mapping
    /// of streams with their committed offsets as values.
    fn commit_offsets(&mut self) -> Result<HashMap<Partition, u64>, ConsumerError>;

    fn close(&mut self);

    fn closed(&self) -> bool;
}

pub trait Producer<TPayload>: Send + Sync {
    /// Produce to a topic or partition.
    fn produce(&self, destination: &TopicOrPartition, payload: &TPayload);

    fn close(&mut self);
}
