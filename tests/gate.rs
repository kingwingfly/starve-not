//! The gate on its own: how its limit changes while permits are out, how weighted items wait
//! their turn, and how closing and draining behave. A pacer with a [`Fixed`] policy sets the
//! limit, since building one resizes the gate.

use std::time::{Duration, Instant};

use starve_not::{Closed, Fixed, Gate, Pacer, Ticket};
use tokio::task::JoinHandle;

/// Set the gate's limit, as a pacer does.
fn set_limit(gate: &Gate, limit: usize) {
    let _ = Pacer::builder(gate, Fixed::new(limit)).build();
}

/// Ask for a ticket of `weight` permits in the background.
fn spawn_weighted(gate: &Gate, weight: usize) -> JoinHandle<Result<Ticket, Closed>> {
    let gate = gate.clone();
    tokio::spawn(async move { gate.acquire_weighted(weight).await })
}

/// Let spawned tasks run until they are all waiting.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

/// Lowering the limit takes back no permit that is out. Instead, permits that come back retire
/// until what's inside is within the new limit.
#[test]
fn lowering_the_limit_retires_returned_permits() {
    let gate = Gate::new(4);
    let mut ticket = gate.try_acquire_up_to(4).unwrap();
    set_limit(&gate, 2);
    assert_eq!((gate.limit(), gate.in_flight()), (2, 4));

    ticket.release_n(2);
    assert!(gate.try_acquire_up_to(1).is_none(), "both should retire");
    ticket.release_n(1);
    assert_eq!(gate.try_acquire_up_to(2).map(|t| t.len()), Some(1));
}

/// Raising the limit while permits are still to retire cancels that retirement first, instead
/// of adding permits on top of it.
#[test]
fn raising_the_limit_pays_off_retirements_first() {
    let gate = Gate::new(4);
    let mut ticket = gate.try_acquire_up_to(4).unwrap();
    set_limit(&gate, 1);
    set_limit(&gate, 3);

    ticket.release_n(1);
    assert!(
        gate.try_acquire_up_to(1).is_none(),
        "3 inside at a limit of 3"
    );
    ticket.release_n(1);
    assert_eq!(gate.try_acquire_up_to(2).map(|t| t.len()), Some(1));
}

/// A weighted item at the front sets aside permits as they come free, so lighter items can't
/// keep overtaking it. What it set aside counts as in flight.
#[tokio::test]
async fn weighted_item_is_not_overtaken() {
    let gate = Gate::new(4);
    let mut light = gate.try_acquire_up_to(3).unwrap();
    let heavy = spawn_weighted(&gate, 2);
    settle().await;
    assert!(!heavy.is_finished());
    assert_eq!(gate.in_flight(), 4);

    light.release_n(1);
    assert!(
        gate.try_acquire_up_to(1).is_none(),
        "the permit is the heavy item's"
    );
    settle().await;
    assert!(heavy.is_finished());
    let heavy = heavy.await.unwrap().unwrap();
    assert_eq!((heavy.len(), gate.in_flight()), (2, 4));
}

/// An item heavier than the whole limit would never fit, so it gets the whole limit instead,
/// once nothing else is inside.
#[tokio::test]
async fn weighted_item_heavier_than_the_limit_goes_alone() {
    let gate = Gate::new(2);
    let other = gate.try_acquire_up_to(1).unwrap();
    let heavy = spawn_weighted(&gate, 5);
    settle().await;
    assert!(!heavy.is_finished());

    other.release();
    settle().await;
    assert!(heavy.is_finished());
    assert_eq!(heavy.await.unwrap().unwrap().len(), 2);
}

/// When the limit drops while a weighted item waits, it gives back what it set aside and starts
/// over, so it never gets in on permits the new limit doesn't allow.
#[tokio::test]
async fn weighted_item_starts_over_when_the_limit_drops() {
    let gate = Gate::new(4);
    let others = gate.try_acquire_up_to(2).unwrap();
    let heavy = spawn_weighted(&gate, 4);
    settle().await;
    assert_eq!(gate.in_flight(), 4, "2 inside and 2 set aside");

    set_limit(&gate, 2);
    settle().await;
    assert!(!heavy.is_finished());
    assert_eq!(gate.in_flight(), 2, "what was set aside retired");

    others.release();
    settle().await;
    assert!(heavy.is_finished());
    let heavy = heavy.await.unwrap().unwrap();
    assert_eq!((heavy.len(), gate.in_flight()), (2, 2));
}

/// Closing fails everyone still waiting: for a permit, for a weighted item's permits, and for a
/// weighted item's turn.
#[tokio::test]
async fn close_fails_waiting_acquires() {
    let gate = Gate::new(1);
    let inside = gate.try_acquire_up_to(1).unwrap();
    let plain = tokio::spawn({
        let gate = gate.clone();
        async move { gate.acquire().await }
    });
    // the first waits for a permit, the second for its turn
    let first = spawn_weighted(&gate, 1);
    let second = spawn_weighted(&gate, 1);
    settle().await;
    assert!(!plain.is_finished() && !first.is_finished() && !second.is_finished());

    gate.close();
    settle().await;
    for waiting in [plain, first, second] {
        assert!(waiting.is_finished());
        assert_eq!(waiting.await.unwrap().unwrap_err(), Closed);
    }
    assert!(gate.try_acquire_up_to(1).is_none());
    inside.complete();
}

/// `drained` waits until the last ticket is back, even with the gate already closed.
#[tokio::test]
async fn drained_waits_for_the_last_ticket() {
    let gate = Gate::new(2);
    let ticket = gate.try_acquire_up_to(2).unwrap();
    gate.close();
    let drained = tokio::spawn({
        let gate = gate.clone();
        async move { gate.drained().await }
    });
    settle().await;
    assert!(!drained.is_finished());

    ticket.complete();
    settle().await;
    assert!(drained.is_finished());
}

/// Completed and released permits are counted apart: only completed ones count as progress.
#[test]
fn complete_and_release_are_counted_apart() {
    let gate = Gate::new(4);
    let mut pacer = Pacer::builder(&gate, Fixed::new(4)).build();
    let start = Instant::now();
    assert!(pacer.step(start).is_none());

    let mut ticket = gate.try_acquire_up_to(3).unwrap();
    ticket.complete_n(1);
    ticket.release_n(1);
    // dropping counts as a release too
    drop(ticket);
    let sample = pacer.step(start + Duration::from_secs(1)).unwrap().sample;
    assert_eq!(
        (sample.completed, sample.released, sample.in_flight),
        (1, 2, 0)
    );
}
