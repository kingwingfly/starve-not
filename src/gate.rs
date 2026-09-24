//! The gate at the pipeline's entry, and the tickets that travel with admitted work.

use std::{
    fmt,
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{
            AtomicU64, AtomicUsize,
            Ordering::{Relaxed, SeqCst},
        },
    },
    task::Poll,
};

use tokio::sync::{Notify, Semaphore, TryAcquireError};

/// Returned when the gate has been closed with [`Gate::close`], so it lets nothing new in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("gate closed")
    }
}

impl std::error::Error for Closed {}

/// Limits how much work is inside the pipeline at once.
///
/// Before an item enters the pipeline, ask the gate for a [`Ticket`]. The ticket holds one
/// *permit* per item, and the gate hands out at most [`limit`](Self::limit) permits at a time.
/// When the item is done, however many stages later, give the ticket back with
/// [`complete`](Ticket::complete) (or [`release`](Ticket::release) if it failed). Then the gate
/// can let the next item in.
///
/// A [`Pacer`](crate::Pacer) changes the limit while the pipeline runs. Lowering it never takes
/// back a permit that is already out. Instead the gate keeps the next permits returned to it, until
/// the number handed out is back under the new limit.
///
/// Clones are cheap and all refer to the same gate, so pass one to every task that needs it.
///
/// # Weighted items
///
/// A permit doesn't have to mean one item. If items differ a lot in cost (small and large
/// images, short and long documents), give each a *weight* in whatever unit fits, say
/// megabytes, and admit it with [`acquire_weighted`](Self::acquire_weighted). An item of weight
/// 5 then takes 5 permits, and the limit becomes a limit on total weight.
///
/// Everything else counts in the same unit: [`in_flight`](Self::in_flight) is the weight inside,
/// and completing a ticket counts its weight as done. The policies work unchanged, since they
/// only compare these numbers with each other. Their settings that are counts, like
/// [`DrainBounded`](crate::DrainBounded)'s `floor`, are then in your unit too.
///
/// An item heavier than the whole limit would never fit. Instead it waits until nothing else
/// is inside and then takes the whole limit, so it goes through alone.
#[derive(Clone)]
pub struct Gate {
    inner: Arc<Inner>,
}

struct Inner {
    sem: Semaphore,
    /// the eventual limit; `sem` holds `limit + retiring - held` permits once retirements settle
    limit: AtomicUsize,
    /// permits still to retire for a shrink that idle permits couldn't cover
    retiring: AtomicUsize,
    /// permits held by tickets
    held: AtomicUsize,
    /// acquires in progress, which may hold permits that no ticket counts yet
    admitting: AtomicUsize,
    /// permits returned by `complete`, i.e. useful throughput
    completed: AtomicU64,
    /// permits returned without completing
    released: AtomicU64,
    /// woken whenever `held` or `admitting` drops to zero
    drained: Notify,
    /// woken whenever the limit shrinks, so weighted acquires can stop waiting for more
    /// permits than the new limit has
    shrunk: Notify,
    /// twice the shrinks so far, plus one while a shrink is underway, so a weighted acquire can
    /// tell its gathered permits predate one
    shrinks: AtomicU64,
    /// woken when the gate closes, for weighted acquires waiting for their turn
    closed: Notify,
    /// the weighted acquire whose turn it is to gather permits
    weighted: tokio::sync::Mutex<()>,
}

impl Gate {
    /// Create a gate that lets in up to `initial` items at once.
    ///
    /// If you drive the gate with a [`Pacer`](crate::Pacer), the pacer sets the limit to its
    /// policy's starting value as soon as it's built, so `initial` doesn't matter much. The limit
    /// can't go above [`Semaphore::MAX_PERMITS`]; larger values are lowered to it.
    pub fn new(initial: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                sem: Semaphore::new(initial.min(Semaphore::MAX_PERMITS)),
                limit: AtomicUsize::new(initial.min(Semaphore::MAX_PERMITS)),
                retiring: AtomicUsize::new(0),
                held: AtomicUsize::new(0),
                admitting: AtomicUsize::new(0),
                completed: AtomicU64::new(0),
                released: AtomicU64::new(0),
                drained: Notify::new(),
                shrunk: Notify::new(),
                shrinks: AtomicU64::new(0),
                closed: Notify::new(),
                weighted: tokio::sync::Mutex::const_new(()),
            }),
        }
    }

    /// Wait until the gate lets one more item in, and get a ticket for it.
    ///
    /// Fails with [`Closed`] if the gate is closed, including while waiting.
    pub async fn acquire(&self) -> Result<Ticket, Closed> {
        self.acquire_up_to(1).await
    }

    /// Get a ticket for a batch of up to `max` items.
    ///
    /// This waits for the first permit only, then adds as many more as are free right away, up
    /// to `max` in total. So it never sits waiting for a full batch. Take a batch the size of
    /// the ticket, and if you end up with fewer items, give back the rest with
    /// [`release_n`](Ticket::release_n).
    ///
    /// A `max` of 0 counts as 1. Fails with [`Closed`] if the gate is closed.
    pub async fn acquire_up_to(&self, max: usize) -> Result<Ticket, Closed> {
        let _admitting = Admitting::new(&self.inner);
        self.inner.sem.acquire().await.map_err(|_| Closed)?.forget();
        let permits = 1 + self.take_free(max.max(1) - 1);
        Ok(self.ticket(permits))
    }

    /// Wait until an item of the given `weight` fits, and get a ticket holding `weight`
    /// permits. See [Weighted items](Self#weighted-items).
    ///
    /// Weighted items are let in one at a time, in the order they asked. While it waits, the
    /// item at the front sets aside permits as they come free, so light items can't keep
    /// overtaking it. Permits set aside count as [`in_flight`](Self::in_flight), since nobody
    /// else can use them. If the limit is lowered meanwhile, the item gives back everything it
    /// set aside and starts over, so it never gets in on permits the new limit doesn't allow.
    ///
    /// An item heavier than the limit gets a ticket for the whole limit instead, once nothing
    /// else is inside. A `weight` of 0 counts as 1.
    ///
    /// Fails with [`Closed`] if the gate is closed, including while waiting.
    pub async fn acquire_weighted(&self, weight: usize) -> Result<Ticket, Closed> {
        let _admitting = Admitting::new(&self.inner);
        let weight = weight.max(1);
        // weighted acquires gather permits one at a time: two gathering at once could each set
        // aside part of what the other needs, and both wait forever. An earlier one that stopped
        // being polled keeps its turn, so waiting for ours must still notice the gate closing
        let _turn = {
            let mut closed = pin!(self.inner.closed.notified());
            closed.as_mut().enable();
            if self.is_closed() {
                return Err(Closed);
            }
            first(pin!(self.inner.weighted.lock()), closed)
                .await
                .ok_or(Closed)?
        };
        let mut reserved = Reservation::new(self);
        let mut seen = self.inner.shrinks.load(SeqCst);
        loop {
            // the limit before the shrink count: a shrink makes the count odd before it lowers
            // the limit, so seeing a lowered limit means seeing its shrink
            let limit = self.inner.limit.load(SeqCst);
            let shrinks = self.inner.shrinks.load(SeqCst);
            if shrinks != seen {
                // what we gathered fit the old limit, not necessarily the new one: with other
                // items inside it may be the gate's whole excess. Hand it all back, where it pays
                // off the shrink first, and gather again from permits free under the new limit
                seen = shrinks;
                reserved.refund(reserved.permits);
            }
            if shrinks % 2 == 1 {
                // a shrink is underway and hasn't recorded its debt yet: permits free now may be
                // ones it is about to retire, so gather nothing until it is done
                let mut shrunk = pin!(self.inner.shrunk.notified());
                shrunk.as_mut().enable();
                if self.inner.shrinks.load(SeqCst) == seen {
                    shrunk.await;
                }
                continue;
            }
            // at a limit of 0 this waits for a permit that only comes once the limit grows
            let target = weight.min(limit).max(1);
            if reserved.permits >= target {
                // only a growth changed `target` since we gathered, so we hold exactly it
                return Ok(reserved.into_ticket());
            }
            let free = self.take_free(target - reserved.permits);
            if free > 0 {
                reserved.add(free);
                continue;
            }
            let mut shrunk = pin!(self.inner.shrunk.notified());
            shrunk.as_mut().enable();
            // checked after `enable`, so a shrink in between still wakes us: after one we may
            // hold permits that only our refund would free
            if self.inner.shrinks.load(SeqCst) != seen {
                continue;
            }
            // a single permit is never split, so cancelling this wait on a shrink hands at
            // most one permit that should retire to another waiter, as cancelling any
            // `acquire` does; returns later pay the retirement back
            match first(pin!(self.inner.sem.acquire()), shrunk).await {
                Some(Ok(permit)) => {
                    permit.forget();
                    reserved.add(1);
                }
                Some(Err(_)) => return Err(Closed),
                None => {}
            }
        }
    }

    /// Like [`acquire_weighted`](Self::acquire_weighted), but never waits.
    ///
    /// Returns `None` if the item doesn't fit right now, another weighted item is waiting
    /// ahead of it, or the gate is closed.
    pub fn try_acquire_weighted(&self, weight: usize) -> Option<Ticket> {
        let _admitting = Admitting::new(&self.inner);
        let _turn = self.inner.weighted.try_lock().ok()?;
        let shrinks = self.inner.shrinks.load(SeqCst);
        // mid-shrink, free permits may be ones the shrink is about to retire
        if shrinks % 2 == 1 {
            return None;
        }
        let target = weight.max(1).min(self.limit()).max(1);
        let mut reserved = Reservation::new(self);
        reserved.add(self.take_free(target));
        // not all of it free, or a shrink started meanwhile: dropping `reserved` hands back
        // what it took
        (reserved.permits == target && self.inner.shrinks.load(SeqCst) == shrinks)
            .then(|| reserved.into_ticket())
    }

    /// Like [`acquire_up_to`](Self::acquire_up_to), but never waits.
    ///
    /// Returns `None` if no permit is free right now, the gate is closed, or `max` is 0.
    pub fn try_acquire_up_to(&self, max: usize) -> Option<Ticket> {
        let _admitting = Admitting::new(&self.inner);
        match self.take_free(max) {
            0 => None,
            permits => Some(self.ticket(permits)),
        }
    }

    /// Forget up to `want` free permits and return how many.
    fn take_free(&self, want: usize) -> usize {
        let mut taken = 0;
        loop {
            // the semaphore hands out at most `u32::MAX` at once: take bigger amounts in chunks
            let n = self
                .inner
                .sem
                .available_permits()
                .min(want - taken)
                .min(u32::MAX as usize);
            if n == 0 {
                return taken;
            }
            match self.inner.sem.try_acquire_many(n as u32) {
                Ok(permit) => {
                    permit.forget();
                    taken += n;
                }
                Err(TryAcquireError::Closed) => return taken,
                // another acquirer took some in between; retry with what's left
                Err(TryAcquireError::NoPermits) => {}
            }
        }
    }

    fn ticket(&self, permits: usize) -> Ticket {
        self.inner.held.fetch_add(permits, SeqCst);
        Ticket {
            gate: self.clone(),
            permits,
        }
    }

    /// The most items the gate lets in at once.
    ///
    /// Right after the limit is lowered, more items than this can still be inside, since the
    /// gate waits for them to finish rather than taking their permits back.
    pub fn limit(&self) -> usize {
        self.inner.limit.load(Relaxed)
    }

    /// How many items are inside right now: permits handed out and not yet given back.
    ///
    /// This includes permits set aside by a [weighted](Self::acquire_weighted) item still
    /// waiting to get in, since nobody else can use them either.
    pub fn in_flight(&self) -> usize {
        self.inner.held.load(Relaxed)
    }

    /// Stop letting items in.
    ///
    /// Anyone waiting in [`acquire`](Self::acquire), and anyone calling it later, gets
    /// [`Closed`]. Items already inside keep their tickets and finish normally. Closing can't be
    /// undone.
    pub fn close(&self) {
        self.inner.sem.close();
        self.inner.closed.notify_waiters();
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.inner.sem.is_closed()
    }

    /// Wait until every ticket has been given back.
    ///
    /// Use it for a clean shutdown: [`close`](Self::close) the gate, then wait here for the
    /// items already inside to finish.
    ///
    /// Calls to [`acquire`](Self::acquire) that are still waiting count as not finished. So
    /// if the gate is still open, this also waits for them to get their permit and give it
    /// back, or to be cancelled.
    pub async fn drained(&self) {
        loop {
            let notified = self.inner.drained.notified();
            // an acquire counts its permits as held before it stops counting as admitting, so
            // reading `admitting` first can't miss one that is between the two
            if self.inner.admitting.load(SeqCst) == 0 && self.inner.held.load(SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Resize the limit to `target`, capped at [`Semaphore::MAX_PERMITS`]. Held permits beyond it
    /// retire as they return.
    pub(crate) fn resize(&self, target: usize) {
        let target = target.min(Semaphore::MAX_PERMITS);
        // the shrink count is odd while a shrink is underway and even once its limit and debt
        // are recorded, so a weighted acquire never gathers permits a shrink is about to
        // retire. It reads the limit, then the count: making the count odd before lowering the
        // limit means one that sees the lower limit also sees the shrink. A pacer is the only
        // caller, so the limit read here is normally the one the swap replaces; if not, the
        // check after the swap still counts the shrink, and a needless count only costs a
        // weighted acquire a refund
        let started = target < self.inner.limit.load(SeqCst);
        if started {
            self.inner.shrinks.fetch_add(1, SeqCst);
        }
        let current = self.inner.limit.swap(target, SeqCst);
        let shrinking = target < current;
        if shrinking && !started {
            self.inner.shrinks.fetch_add(1, SeqCst);
        }
        if target > current {
            // pays off pending retirements before adding permits
            self.circulate(target - current);
        } else if target < current {
            // record the whole debt first, so permits returned meanwhile retire instead of
            // circulating; then pay it with free permits (a waiting acquirer can take returned
            // permits before they ever become free). Circulating the forgotten ones pays what's
            // still owed and gives back any that returns meanwhile already paid for
            let shrink = current - target;
            self.inner.retiring.fetch_add(shrink, Relaxed);
            let forgotten = self.inner.sem.forget_permits(shrink);
            self.circulate(forgotten);
        }
        if started || shrinking {
            // done: even again
            self.inner.shrinks.fetch_add(1, SeqCst);
            self.inner.shrunk.notify_waiters();
        }
    }

    /// Put `n` permits (back) into circulation, first retiring any a shrink still owes.
    fn circulate(&self, n: usize) {
        let retired = self
            .inner
            .retiring
            .fetch_update(Relaxed, Relaxed, |r| (r > 0).then(|| r - r.min(n)))
            .map_or(0, |r| r.min(n));
        self.inner.sem.add_permits(n - retired);
    }

    /// Return `n` held permits, counting them as completed or released.
    fn give_back(&self, n: usize, completed: bool) {
        if n == 0 {
            return;
        }
        let counter = match completed {
            true => &self.inner.completed,
            false => &self.inner.released,
        };
        counter.fetch_add(n as u64, Relaxed);
        self.put_back(n);
    }

    /// Return `n` held permits without counting them as having left the pipeline.
    fn put_back(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.circulate(n);
        // `SeqCst` pairs with `Admitting::drop`: of the two last decrements, at least one sees
        // the other counter at 0, and only that one pays for waking `drained`
        if self.inner.held.fetch_sub(n, SeqCst) == n && self.inner.admitting.load(SeqCst) == 0 {
            self.inner.drained.notify_waiters();
        }
    }

    /// Total permits returned by `complete` so far.
    pub(crate) fn completed(&self) -> u64 {
        self.inner.completed.load(Relaxed)
    }

    /// Total permits returned without completing so far.
    pub(crate) fn released(&self) -> u64 {
        self.inner.released.load(Relaxed)
    }
}

impl fmt::Debug for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gate")
            .field("limit", &self.limit())
            .field("in_flight", &self.in_flight())
            .field("retiring", &self.inner.retiring.load(Relaxed))
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Permits a weighted acquire has gathered so far. They count as held, so the policy sees them
/// and a shrink retires them when they come back; dropping it gives them back without counting
/// them as having left the pipeline.
struct Reservation<'a> {
    gate: &'a Gate,
    permits: usize,
}

impl<'a> Reservation<'a> {
    fn new(gate: &'a Gate) -> Self {
        Self { gate, permits: 0 }
    }

    fn add(&mut self, n: usize) {
        self.gate.inner.held.fetch_add(n, SeqCst);
        self.permits += n;
    }

    fn refund(&mut self, n: usize) {
        self.permits -= n;
        self.gate.put_back(n);
    }

    fn into_ticket(mut self) -> Ticket {
        Ticket {
            gate: self.gate.clone(),
            permits: std::mem::take(&mut self.permits),
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.gate.put_back(self.permits);
    }
}

/// Wait for `a`, or return `None` as soon as `b` completes first.
async fn first<A: Future, B: Future>(mut a: Pin<&mut A>, mut b: Pin<&mut B>) -> Option<A::Output> {
    poll_fn(|cx| {
        if let Poll::Ready(out) = a.as_mut().poll(cx) {
            return Poll::Ready(Some(out));
        }
        b.as_mut().poll(cx).map(|_| None)
    })
    .await
}

/// Counts an acquire as in progress until dropped, including when its future is cancelled.
struct Admitting<'a>(&'a Inner);

impl<'a> Admitting<'a> {
    fn new(inner: &'a Inner) -> Self {
        inner.admitting.fetch_add(1, SeqCst);
        Self(inner)
    }
}

impl Drop for Admitting<'_> {
    fn drop(&mut self) {
        if self.0.admitting.fetch_sub(1, SeqCst) == 1 && self.0.held.load(SeqCst) == 0 {
            self.0.drained.notify_waiters();
        }
    }
}

/// Permission for some items to be inside the pipeline, from a [`Gate`].
///
/// Send the ticket along with its items through the pipeline. When they are done, hand it back
/// with [`complete`](Self::complete). If they failed or were never used, use
/// [`release`](Self::release) instead. Only completed items count as progress when a policy
/// judges how fast the pipeline is going.
///
/// A ticket is just a count of permits. It doesn't know which items it belongs to. When a stage
/// splits a batch in two or joins batches together, do the same with the tickets using
/// [`split`](Self::split) and [`merge`](Self::merge).
///
/// Dropping a ticket counts as [`release`](Self::release). A panic or early return therefore
/// never loses permits, but it doesn't count as progress either.
#[must_use = "dropping a ticket gives its permits back without counting its items as done"]
pub struct Ticket {
    gate: Gate,
    permits: usize,
}

impl Ticket {
    /// How many permits this ticket holds.
    pub fn len(&self) -> usize {
        self.permits
    }

    /// Whether this ticket holds no permits.
    pub fn is_empty(&self) -> bool {
        self.permits == 0
    }

    /// Move `n` permits into a new ticket, for example to give each item in a batch its own.
    ///
    /// # Panics
    ///
    /// If `n` is more than [`len`](Self::len).
    pub fn split(&mut self, n: usize) -> Ticket {
        assert!(
            n <= self.permits,
            "split {n} permits off a ticket holding {}",
            self.permits
        );
        self.permits -= n;
        Ticket {
            gate: self.gate.clone(),
            permits: n,
        }
    }

    /// Move all of `other`'s permits into this ticket, for example when batches are joined.
    ///
    /// # Panics
    ///
    /// If `other` was admitted by a different gate.
    pub fn merge(&mut self, mut other: Ticket) {
        assert!(
            Arc::ptr_eq(&self.gate.inner, &other.gate.inner),
            "merged tickets from different gates"
        );
        self.permits += std::mem::take(&mut other.permits);
    }

    /// Hand back every permit, counting its items as done.
    pub fn complete(mut self) {
        self.complete_n(self.permits);
    }

    /// Hand back `n` permits, counting their items as done, and keep the rest.
    ///
    /// # Panics
    ///
    /// If `n` is more than [`len`](Self::len).
    pub fn complete_n(&mut self, n: usize) {
        self.give_back(n, true);
    }

    /// Hand back every permit without counting its items as done, for items that failed or
    /// were never used. Same as dropping the ticket.
    pub fn release(self) {}

    /// Hand back `n` permits without counting their items as done, and keep the rest.
    ///
    /// # Panics
    ///
    /// If `n` is more than [`len`](Self::len).
    pub fn release_n(&mut self, n: usize) {
        self.give_back(n, false);
    }

    fn give_back(&mut self, n: usize, completed: bool) {
        assert!(
            n <= self.permits,
            "returned {n} permits from a ticket holding {}",
            self.permits
        );
        self.permits -= n;
        self.gate.give_back(n, completed);
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.gate.give_back(self.permits, false);
    }
}

impl fmt::Debug for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ticket")
            .field("permits", &self.permits)
            .finish()
    }
}
