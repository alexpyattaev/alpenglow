// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Main voting logic for the consensus protocol.
//!
//! Besides [`super::Pool`], [`Votor`] is the other main internal component Alpenglow.
//! It handles the main voting decisions for the consensus protocol. As input it
//! receives events of type [`VotorEvent`] over a channel, depending on the event
//! type these were emitted by  [`super::Pool`], [`super::Blockstore`] and itself.
//! Votor keeps its own internal state for each slot based on previous events and votes.
//!
//! Votor has access to an instance of [`All2All`] for broadcasting votes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use color_eyre::Result;
use log::{debug, trace, warn};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::crypto::Hash;
use crate::crypto::aggsig::SecretKey;
use crate::{All2All, Slot, ValidatorId};

use super::blockstore::BlockInfo;
use super::{Cert, DELTA_BLOCK, DELTA_EARLY_TIMEOUT, SLOTS_PER_WINDOW, Vote};

/// Events that Votor is interested in.
/// These are emitted by [`super::Pool`], [`super::Blockstore`] and [`Votor`] itself.
#[derive(Clone, Debug)]
pub enum VotorEvent {
    /// The pool has newly marked the given block as a ready parent for `slot`.
    ///
    /// This event is only emitted per window, `slot` is always the first slot.
    /// The parent block is identified by `parent_slot` and `parent_hash`.
    ParentReady {
        slot: Slot,
        parent_slot: Slot,
        parent_hash: Hash,
    },
    /// The given block has reached the safe-to-notar status.
    SafeToNotar(Slot, Hash),
    /// The given slot has reached the safe-to-skip status.
    SafeToSkip(Slot),
    /// New certificated created in pool (should then be broadcast by Votor).
    CertCreated(Box<Cert>),

    /// First valid shred of the leader's block was received for the block.
    FirstShred(Slot),
    /// New (complete) block was received in blockstore.
    Block { slot: Slot, block_info: BlockInfo },

    /// Regular timeout for the given slot has fired.
    Timeout(Slot),
    /// Early timeout for a crashed leader (nothing was received) has fired.
    TimeoutCrashedLeader(Slot),
}

/// Votor implements the decision process of which votes to cast.
///
/// It keeps some state for each slot and checks the conditions for voting.
/// On [`Votor::event_receiver`], it receives events from [`super::Pool`],
/// [`super::Blockstore`] and itself.
/// Informed by these events Votor updates its state and generates votes.
/// Votes are signed with [`Votor::voting_key`] and broadcast using [`Votor::all2all`].
pub struct Votor<A: All2All + Sync + Send + 'static> {
    // TODO: merge all of these into `SlotState` struct?
    /// Indicates for which slots we already voted notar or skip.
    voted: BTreeSet<Slot>,
    /// Indicates for which slots we already voted notar and for what hash.
    voted_notar: BTreeMap<Slot, Hash>,
    /// Indicates for which slots we set the 'bad window' flag.
    bad_window: BTreeSet<Slot>,
    /// Blocks that have a notarization certificate (not notar-fallback).
    block_notarized: BTreeMap<Slot, Hash>,
    /// Indicates for which slots the given (slot, hash) pair is a valid parent.
    parents_ready: BTreeSet<(Slot, Slot, Hash)>,
    /// Indicates for which slots we received at least one shred.
    received_shred: BTreeSet<Slot>,
    /// Blocks that are waiting for previous slots to be notarized.
    pending_blocks: BTreeMap<Slot, BlockInfo>,
    /// Slots that Votor is done with.
    retired_slots: BTreeSet<Slot>,

    /// Own validator ID.
    validator_id: ValidatorId,
    /// Secret key used to sign votes.
    voting_key: SecretKey,
    /// Channel for receiving events from pool, blockstore and Votor itself.
    event_receiver: Receiver<VotorEvent>,
    /// Sender side of event channel. Used for sending events to self.
    event_sender: Sender<VotorEvent>,
    /// [`All2All`] instance used to broadcast votes.
    all2all: Arc<A>,
}

impl<A: All2All + Sync + Send + 'static> Votor<A> {
    /// Creates a new Votor instance with empty state.
    pub fn new(
        validator_id: ValidatorId,
        voting_key: SecretKey,
        event_sender: Sender<VotorEvent>,
        event_receiver: Receiver<VotorEvent>,
        all2all: Arc<A>,
    ) -> Self {
        let mut parents_ready = BTreeSet::new();
        parents_ready.insert((0, 0, Hash::default()));
        Self {
            voted: BTreeSet::new(),
            voted_notar: BTreeMap::new(),
            bad_window: BTreeSet::new(),
            block_notarized: BTreeMap::new(),
            parents_ready,
            received_shred: BTreeSet::new(),
            pending_blocks: BTreeMap::new(),
            retired_slots: BTreeSet::new(),
            validator_id,
            voting_key,
            event_receiver,
            event_sender,
            all2all,
        }
    }

    /// Handles the voting (leader and non-leader) side of consensus protocol.
    ///
    /// Checks consensus conditions and broadcasts new votes.
    #[fastrace::trace]
    pub async fn voting_loop(&mut self) -> Result<()> {
        while let Some(event) = self.event_receiver.recv().await {
            if self.retired_slots.contains(&event.slot()) {
                trace!("ignoring event for retired slot {}", event.slot());
                continue;
            }
            trace!("votor event: {:?}", event);
            match event {
                // events from Pool
                VotorEvent::ParentReady {
                    slot,
                    parent_slot,
                    parent_hash,
                } => {
                    let h = &hex::encode(parent_hash)[..8];
                    trace!("slot {slot} has new valid parent {h} in slot {parent_slot}");
                    self.parents_ready.insert((slot, parent_slot, parent_hash));
                    self.check_pending_blocks().await;
                    // TODO: set timeouts only once?
                    trace!(
                        "setting timeouts for slots {}..{}",
                        slot,
                        slot + SLOTS_PER_WINDOW
                    );
                    self.set_timeouts(slot);
                }
                VotorEvent::SafeToNotar(slot, hash) => {
                    trace!("safe to notar slot {}", slot);
                    let vote =
                        Vote::new_notar_fallback(slot, hash, &self.voting_key, self.validator_id);
                    self.all2all.broadcast(&vote.into()).await.unwrap();
                    self.try_skip_window(slot).await;
                    self.bad_window.insert(slot);
                }
                VotorEvent::SafeToSkip(slot) => {
                    trace!("safe to skip slot {}", slot);
                    let vote = Vote::new_skip_fallback(slot, &self.voting_key, self.validator_id);
                    self.all2all.broadcast(&vote.into()).await.unwrap();
                    self.try_skip_window(slot).await;
                    self.bad_window.insert(slot);
                }
                VotorEvent::CertCreated(cert) => {
                    match cert.as_ref() {
                        Cert::NotarFallback(_) => {
                            // TODO: start repair
                        }
                        Cert::Notar(_) => {
                            self.block_notarized
                                .insert(cert.slot(), cert.block_hash().unwrap());
                            self.try_final(cert.slot(), cert.block_hash().unwrap())
                                .await;
                        }
                        _ => {}
                    }
                    self.all2all.broadcast(&(*cert).into()).await.unwrap();
                }

                // events from Blockstore
                VotorEvent::FirstShred(slot) => {
                    self.received_shred.insert(slot);
                }
                VotorEvent::Block { slot, block_info } => {
                    if self.voted.contains(&slot) {
                        let h = &hex::encode(block_info.hash)[..8];
                        warn!("not voting for block {h} in slot {slot}, already voted");
                        continue;
                    }
                    if self.try_notar(slot, block_info).await {
                        self.check_pending_blocks().await;
                    } else {
                        self.pending_blocks.insert(slot, block_info);
                    }
                }

                // events from Votor itself
                VotorEvent::Timeout(slot) => {
                    trace!("timeout for slot {}", slot);
                    if !self.voted.contains(&slot) {
                        self.try_skip_window(slot).await;
                    }
                }
                VotorEvent::TimeoutCrashedLeader(slot) => {
                    trace!("timeout (crashed leader) for slot {}", slot);
                    if !self.received_shred.contains(&slot) && !self.voted.contains(&slot) {
                        self.try_skip_window(slot).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Sets timeouts for the leader window starting at the given `slot`.
    ///
    /// # Panics
    ///
    /// Panics if `slot` is not the first slot of a window.
    fn set_timeouts(&self, slot: Slot) {
        assert_eq!(slot % SLOTS_PER_WINDOW, 0);
        let sender = self.event_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(DELTA_EARLY_TIMEOUT).await;
            for offset in 0..SLOTS_PER_WINDOW {
                let event = VotorEvent::TimeoutCrashedLeader(slot + offset);
                // HACK: ignoring errors to prevent panic when shutting down votor
                let _ = sender.send(event).await;
                tokio::time::sleep(DELTA_BLOCK).await;
                let event = VotorEvent::Timeout(slot + offset);
                let _ = sender.send(event).await;
            }
        });
    }

    /// Sends a notarization vote for the given block if the conditions are met.
    ///
    /// Returns `true` iff we decided to send a notarization vote for the block.
    async fn try_notar(&mut self, slot: Slot, block_info: BlockInfo) -> bool {
        let BlockInfo {
            hash,
            parent_slot,
            parent_hash,
        } = block_info;
        let first_slot = slot / SLOTS_PER_WINDOW * SLOTS_PER_WINDOW;
        if slot == first_slot {
            let valid_parent = self
                .parents_ready
                .contains(&(slot, parent_slot, parent_hash));
            let h = &hex::encode(parent_hash)[..8];
            trace!(
                "try notar slot {} with parent {} in slot {} (valid {})",
                slot, h, parent_slot, valid_parent
            );
            if !valid_parent {
                return false;
            }
        } else if parent_slot != slot - 1
            || self.voted_notar.get(&parent_slot) != Some(&parent_hash)
        {
            return false;
        }
        debug!(
            "validator {} voted notar for slot {}",
            self.validator_id, slot
        );
        let vote = Vote::new_notar(slot, hash, &self.voting_key, self.validator_id);
        self.all2all.broadcast(&vote.into()).await.unwrap();
        self.voted.insert(slot);
        self.voted_notar.insert(slot, hash);
        self.pending_blocks.remove(&slot);
        self.try_final(slot, hash).await;
        true
    }

    /// Sends a finalization vote for the given block if the conditions are met.
    async fn try_final(&mut self, slot: Slot, hash: Hash) {
        let notarized = self.block_notarized.get(&slot) == Some(&hash);
        let voted_notar = self.voted_notar.get(&slot) == Some(&hash);
        let not_bad = !self.bad_window.contains(&slot);
        if notarized && voted_notar && not_bad {
            let vote = Vote::new_final(slot, &self.voting_key, self.validator_id);
            self.all2all.broadcast(&vote.into()).await.unwrap();
            self.retired_slots.insert(slot);
        }
    }

    /// Sends skip votes for all unvoted slots in the window that `slot` belongs to.
    async fn try_skip_window(&mut self, slot: Slot) {
        trace!("try skip window of slot {}", slot);
        let first_slot = slot / SLOTS_PER_WINDOW * SLOTS_PER_WINDOW;
        for s in first_slot..first_slot + SLOTS_PER_WINDOW {
            if self.voted.insert(s) {
                let vote = Vote::new_skip(s, &self.voting_key, self.validator_id);
                self.all2all.broadcast(&vote.into()).await.unwrap();
                self.bad_window.insert(s);
                debug!("validator {} voted skip for slot {}", self.validator_id, s);
            }
        }
    }

    /// Checks if we can vote on any of the pending blocks by now.
    async fn check_pending_blocks(&mut self) {
        let slots: Vec<_> = self.pending_blocks.keys().copied().collect();
        for slot in &slots {
            if let Some(block_info) = self.pending_blocks.get(slot) {
                self.try_notar(*slot, *block_info).await;
            }
        }
    }
}

impl VotorEvent {
    const fn slot(&self) -> Slot {
        match self {
            Self::ParentReady { slot, .. }
            | Self::SafeToNotar(slot, _)
            | Self::SafeToSkip(slot)
            | Self::FirstShred(slot)
            | Self::Block { slot, .. }
            | Self::Timeout(slot)
            | Self::TimeoutCrashedLeader(slot) => *slot,
            Self::CertCreated(cert) => cert.slot(),
        }
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    //
    // use crate::all2all::TrivialAll2All;
    // use crate::network::{NetworkMessage, SimulatedNetwork};
    // use crate::tests::{generate_all2all_instances, generate_validators};
    //
    // use tokio::sync::mpsc;
    //
    // type A2A = TrivialAll2All<SimulatedNetwork>;
    //
    // async fn start_votor() -> (A2A, mpsc::Sender<VotorEvent>) {
    //     let (sks, epoch_info) = generate_validators(2);
    //     let mut a2a = generate_all2all_instances(epoch_info.validators.clone()).await;
    //     let (tx, rx) = mpsc::channel(100);
    //     let other_a2a = a2a.pop().unwrap();
    //     let votor_a2a = a2a.pop().unwrap();
    //     let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
    //     tokio::spawn(async move {
    //         votor.voting_loop().await.unwrap();
    //     });
    //     (other_a2a, tx)
    // }
    //
    // // FIXME: sometimes waits forever
    // #[tokio::test]
    // async fn skips() {
    //     let (other_a2a, tx) = start_votor().await;
    //     for i in 0..SLOTS_PER_WINDOW {
    //         tx.send(VotorEvent::SkipCertified(i)).await.unwrap();
    //     }
    //
    //     // only receive first shred of first block, no full block for the next window
    //     let event = VotorEvent::FirstShred(SLOTS_PER_WINDOW);
    //     tx.send(event).await.unwrap();
    //
    //     // should vote skip for all slots
    //     for i in 0..SLOTS_PER_WINDOW {
    //         if let Ok(msg) = other_a2a.receive().await {
    //             match msg {
    //                 NetworkMessage::Vote(v) => {
    //                     assert!(v.is_skip());
    //                     assert_eq!(v.slot(), SLOTS_PER_WINDOW + i);
    //                 }
    //                 _ => unreachable!(),
    //             }
    //         }
    //     }
    // }
    //
    // #[tokio::test]
    // async fn notar_and_final() {
    //     let (other_a2a, tx) = start_votor().await;
    //
    //     // vote notar after seeing block
    //     let event = VotorEvent::FirstShred(0);
    //     tx.send(event).await.unwrap();
    //     let event = VotorEvent::Block {
    //         slot: 0,
    //         hash: [1u8; 32],
    //         parent_slot: 0,
    //         parent_hash: Hash::default(),
    //     };
    //     tx.send(event).await.unwrap();
    //     if let Ok(msg) = other_a2a.receive().await {
    //         match msg {
    //             NetworkMessage::Vote(v) => {
    //                 assert!(v.is_notar());
    //                 assert_eq!(v.slot(), 0);
    //             }
    //             _ => unreachable!(),
    //         }
    //     }
    //
    //     // vote finalize after seeing branch-certified
    //     let event = VotorEvent::BranchCertified(0, [1u8; 32]);
    //     tx.send(event).await.unwrap();
    //     if let Ok(msg) = other_a2a.receive().await {
    //         match msg {
    //             NetworkMessage::Vote(v) => {
    //                 assert!(v.is_final());
    //                 assert_eq!(v.slot(), 0);
    //             }
    //             _ => unreachable!(),
    //         }
    //     }
    // }
}
