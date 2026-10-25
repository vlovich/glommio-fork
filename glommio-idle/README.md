# Idle callbacks

The production idle callback controller used by Glommio runs synchronous
callbacks in complete batches and defers eager task polling until a batch ends.
The executor rechecks runnable work before spinning or parking. Each idle
episode runs one batch before the configured spin phase, even when an event
arrives before the spin budget expires. Callback-created runnable work skips
both spinning and parking. The reactor handles I/O and external wakeups.
Callbacks run in the default queue regardless of where they were registered;
`spawn_local` inside a callback inherits that queue. The entire batch,
including deferred callback destruction, shares one stall-detection budget
when a detector is configured. Batch time is included in executor runtime,
total runtime, and default-queue runtime; spinning and parking remain excluded.
Registrations use `u8` slot IDs and are limited to 256 callbacks, including
removals awaiting a batch boundary. Dropping a registration unregisters its callback. `register`
reports capacity exhaustion without panicking.

The controller implementation and Kani harnesses live together in `src/lib.rs`.
Glommio's executor runs callback batches and checks runnable queues directly
before spinning or parking. Its integration tests remain inline in
`glommio/src/idle.rs`.

## Verification

Install the verifier version pinned by CI, then run from the workspace root:

```sh
cargo install --locked kani-verifier --version 0.68.0
cargo kani setup
cargo kani -p glommio-idle --output-format terse --jobs 2
```

Run the ordinary panic-recovery test and executor integration tests with:

```sh
cargo test -p glommio-idle
cargo test -p glommio --lib idle::tests
```

The CI workflow runs when this crate, its dense-registry dependency, the
workflow itself, or workspace build configuration changes.
No proof functions are stubbed, and default safety and unwinding checks remain
enabled. Each harness declares its loop-unwinding bound; insufficient bounds
fail verification.

| Harness | Scope |
| --- | --- |
| `batches_defer_all_future_polls_and_restore_eager_spawning` | Three callback visits, all six visitation orders, and every combination of spawn decisions. Futures are queued during the batch without being polled. After the batch, dispatch resumes eager polling; each polled future checks that all three visits finished. Shuffling runs before batch state is reset. |
| `deferred_destructors_and_shuffle_remain_inside_the_batch` | Three callback visits followed by destruction of a removed payload. A destructor's spawn is deferred, shuffling follows destruction, and both occur while batch state is active. |
| `shutdown_is_an_absorbing_state` | Every length-three sequence of shutdown, run, and registration operations starting from a closed controller. None invokes callbacks or reopens registration. |
| `shutdown_clears_and_releases_the_registry_before_dropping_payloads` | The same generic close operation used by shutdown clears an open registry and releases its borrow before dropping the removed payload. A payload destructor observes the cleared registry and can reenter closure; repeated closure drops the payload only once. |
| `registration_after_shutdown_drops_payload_without_resurrection` | Registration in a closed controller immediately drops the callback, exactly once. Its destructor can reenter shutdown and registration without a registry borrow conflict or resurrecting callbacks. The returned handle is inert. |

These are bounded proofs of the production controller's decisions. Batch
harnesses supply registry visitation and scheduler endpoints as closures, so
they do not execute the dense registry's boxed callbacks or the executor's task
queues. The dense registry has its own inline proofs. Glommio integration tests
cover actual spawning, callback registration/removal, the 256-callback boundary,
task wakeups, callback batches before spinning across timer intervals,
default-queue spawning, batch runtime accounting and stall detection,
executor cleanup context, and panic unwinding. These harnesses do not prove
arbitrary callback bodies, unbounded operation histories, or kernel wakeup races.
