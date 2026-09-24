# starve-not

Keep the slowest stage of a pipeline busy, without letting work pile up.

Say your program downloads items and then runs them through a GPU. The GPU is the expensive
part, the *bottleneck*, and the whole pipeline only goes as fast as it does. So how many items
should be on their way at once?

- **Too few**, and the GPU sits idle waiting for downloads.
- **Too many**, and memory fills up. On shutdown, you also wait a long time for everything
  already started to finish.

The right number depends on download speed, GPU speed and batch size, and those change while
the program runs. So no fixed number works for long. `starve-not` keeps adjusting it for you.

## How it works

- A **`Gate`** sits at the start of the pipeline and lets items in, up to a *limit*. Each item
  gets a **`Ticket`** that travels with it. When the item is done, the ticket goes back and the
  next item can come in. If a ticket is dropped by accident (a panic, an early return), it
  still goes back, so the gate never loses track.
- An **`IdleProbe`** on the bottleneck measures how long it waits for input.
- Every couple of seconds, a **`Pacer`** looks at both, asks a **`Policy`** what the limit
  should be, and updates the gate. Lowering the limit never interrupts work already inside.
  The gate just lets fewer new items in until it is back under the limit.

Three policies are included:

- **`DrainBounded`** is the one to start with. It doubles the limit while the bottleneck waits
  for input and the gate is full. It lowers the limit when the work inside would take too long
  to finish, 10 seconds by default.
- **`Aimd`** works like TCP: it raises the limit a little each tick and cuts it when items fail
  or get slow. It needs no probe, so it suits pipelines without one clear bottleneck.
- **`Fixed`** never changes the limit. Use it as a baseline to measure the others against.

You can also write your own by implementing `Policy`.

Two more things you can do:

- **Weigh items by cost.** If some items are much bigger than others, admit each with
  `gate.acquire_weighted(size)`. The limit then caps total size (megabytes, say) instead of the
  number of items.
- **Use several gates.** For example, one gate caps downloads in progress, and another caps
  everything in the pipeline. Each gets its own pacer and policy, and an item holds a ticket
  from each.

## Example

```rust
let gate = Gate::new(1);
let device = IdleProbe::new();
let policy = DrainBounded::builder().floor(2 * batch).build();
let _pacer = Pacer::builder(&gate, policy).probe(&device).build().spawn();

// at the start of the pipeline: wait for room, then take up to a batch
let ticket = gate.acquire_up_to(batch).await?;

// in the bottleneck: count the time spent waiting for the next batch
let batch = {
    let _idle = device.idle();
    rx.recv().await
};

// when the work is done (or `ticket.release()` if it failed)
ticket.complete();

// on shutdown: let nothing new in, then wait for what's inside to finish
gate.close();
gate.drained().await;
```

`cargo run --example pipeline` runs a simulated pipeline where downloads and then the device
get slower, and prints each decision so you can watch the limit adjust.

## Why not use …

| | Why it doesn't fit |
|---|---|
| `tokio::sync::Semaphore`, bounded `mpsc`, `buffer_unordered`, `tower::limit` | The limit is fixed. Nothing adjusts it. |
| `congestion-limiter`, ports of Netflix's `concurrency-limits`, Vector's ARC | They adjust based on how long requests take and how often they fail. They can't see that the GPU is idle while the pipeline is full, which is the sign that the limit should grow. |
| `governor`, `leaky-bucket` | They limit how many items start per second, not how many are in progress. |

## Features

- `rt` (on by default): adds `Pacer::spawn`, which runs the pacer as a tokio task. Without it,
  call `Pacer::step` yourself on a timer.
- `tracing`: logs each of the pacer's decisions at the `debug` level.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.
