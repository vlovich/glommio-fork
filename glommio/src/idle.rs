//! Executor-local registration handles and idle callback integration.

use std::{
    fmt,
    rc::{Rc, Weak},
};

pub(crate) use glommio_idle::IdleWorkers;

use crate::task::registry::TaskRegistry;

/// Keeps a callback registered with [`crate::ExecutorProxy::register_before_park`].
///
/// Dropping the handle unregisters the callback. If a batch is running, the
/// callback remains in that batch and is removed afterward. The handle is
/// local to its executor's thread and may safely outlive the executor.
/// Captured values are destroyed in the owning executor's cleanup context,
/// including when this handle is dropped between runs or in another executor.
#[must_use = "dropping the handle unregisters the callback"]
pub struct BeforeParkHandle {
    registration: Option<glommio_idle::BeforeParkHandle>,
    owner: Weak<TaskRegistry>,
}

impl BeforeParkHandle {
    pub(crate) fn new(
        registration: glommio_idle::BeforeParkHandle,
        owner: &Rc<TaskRegistry>,
    ) -> Self {
        Self {
            registration: Some(registration),
            owner: Rc::downgrade(owner),
        }
    }
}

impl Drop for BeforeParkHandle {
    fn drop(&mut self) {
        let registration = self.registration.take();
        if let Some(context) = self.owner.upgrade().and_then(|owner| owner.context()) {
            context.with_cleanup(None, || drop(registration));
        } else {
            drop(registration);
        }
    }
}

impl fmt::Debug for BeforeParkHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeforeParkHandle").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    //! Idle-batch integration with spawning, wakeups, and executor lifetime.

    use std::{
        cell::{Cell, RefCell},
        future::{pending, poll_fn},
        panic::{catch_unwind, AssertUnwindSafe},
        rc::Rc,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::{Poll, Waker},
        thread,
        time::{Duration, Instant},
    };

    use glommio_idle::BeforeParkCapacityError;

    use crate::{
        channels::local_channel,
        executor,
        executor::stall::{StallDetection, StallDetectionHandler},
        spawn_local, spawn_local_into,
        timer::Timer,
        BeforeParkHandle, Latency, LocalExecutor, LocalExecutorBuilder, Placement, PoolPlacement,
        Shares, TaskQueueHandle,
    };

    fn local_executor() -> LocalExecutor {
        LocalExecutorBuilder::default()
            .io_memory(0)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(1))
            .make()
            .expect("failed to create idle-worker test executor")
    }

    #[test]
    fn callbacks_and_their_spawns_use_the_default_queue() {
        local_executor().run(async {
            let registration_queue = executor().create_task_queue(
                Shares::Static(1),
                Latency::NotImportant,
                "registration",
            );
            let (sender, receiver) = local_channel::new_bounded(1);
            let sender = Rc::new(sender);
            let worker = spawn_local_into(
                async move {
                    assert_eq!(executor().current_task_queue(), registration_queue);
                    executor().register_before_park(move || {
                        assert_eq!(executor().current_task_queue(), TaskQueueHandle::default());
                        let sender = sender.clone();
                        spawn_local(async move {
                            sender.try_send(executor().current_task_queue()).unwrap();
                        })
                        .detach();
                    })
                },
                registration_queue,
            )
            .unwrap()
            .await
            .unwrap();
            assert_eq!(receiver.recv().await, Some(TaskQueueHandle::default()));
            drop(worker);
        });
    }

    #[test]
    fn idle_callback_runtime_is_charged_to_the_executor_and_default_queue() {
        local_executor().run(async {
            let elapsed = Rc::new(Cell::new(Duration::ZERO));
            let (sender, receiver) = local_channel::new_bounded(1);
            let worker = executor()
                .register_before_park({
                    let elapsed = elapsed.clone();
                    move || {
                        let start = Instant::now();
                        thread::sleep(Duration::from_millis(20));
                        elapsed.set(start.elapsed());
                        sender.try_send(()).unwrap();
                    }
                })
                .unwrap();
            executor()
                .task_queue_stats(TaskQueueHandle::default())
                .unwrap();
            let before = executor().executor_stats();
            receiver.recv().await.unwrap();
            let after = executor().executor_stats();
            assert!(after.total_runtime() - before.total_runtime() >= elapsed.get());
            assert!(after.executor_runtime() - before.executor_runtime() >= elapsed.get());
            assert!(
                executor()
                    .task_queue_stats(TaskQueueHandle::default())
                    .unwrap()
                    .runtime()
                    >= elapsed.get()
            );
            drop(worker);
        });
    }

    #[derive(Clone, Debug, Default)]
    struct IdleStallHandler {
        threshold_calls: Arc<AtomicUsize>,
        detections: Arc<AtomicUsize>,
    }

    impl StallDetectionHandler for IdleStallHandler {
        fn threshold(&self, _: TaskQueueHandle, _: Duration) -> Option<Duration> {
            self.threshold_calls.fetch_add(1, Ordering::Relaxed);
            Some(Duration::from_millis(10))
        }

        fn stall(&self, detection: StallDetection<'_>) {
            assert_eq!(detection.queue_handle, TaskQueueHandle::default());
            self.detections.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct IdleCleanup {
        delay: Duration,
        threshold_calls: Arc<AtomicUsize>,
        batch_threshold: Rc<Cell<Option<usize>>>,
    }

    impl Drop for IdleCleanup {
        fn drop(&mut self) {
            assert_eq!(executor().current_task_queue(), TaskQueueHandle::default());
            assert_eq!(
                Some(self.threshold_calls.load(Ordering::Relaxed)),
                self.batch_threshold.get(),
                "the stall budget was restarted during the batch",
            );
            thread::sleep(self.delay);
        }
    }

    fn assert_whole_batch_is_monitored(deferred_cleanup: bool) {
        let handler = IdleStallHandler::default();
        LocalExecutorBuilder::default()
            .io_memory(0)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(1))
            .preempt_timer(Duration::from_millis(30))
            .detect_stalls(Some(Box::new(handler.clone())))
            .make()
            .unwrap()
            .run(async {
                let (sender, receiver) = local_channel::new_bounded(1);
                let sender = Rc::new(sender);
                let handles: Rc<RefCell<Vec<BeforeParkHandle>>> = Rc::default();
                let visited = Rc::new(Cell::new(0));
                let batch_threshold = Rc::new(Cell::new(None));
                for _ in 0..4 {
                    let handles_for_callback = handles.clone();
                    let visited = visited.clone();
                    let batch_threshold = batch_threshold.clone();
                    let threshold_calls = handler.threshold_calls.clone();
                    let sender = sender.clone();
                    let cleanup = IdleCleanup {
                        delay: if deferred_cleanup {
                            Duration::from_millis(20)
                        } else {
                            Duration::ZERO
                        },
                        threshold_calls: threshold_calls.clone(),
                        batch_threshold: batch_threshold.clone(),
                    };
                    let worker = executor()
                        .register_before_park(move || {
                            let _ = &cleanup;
                            let threshold = threshold_calls.load(Ordering::Relaxed);
                            if let Some(previous) = batch_threshold.get() {
                                assert_eq!(
                                    threshold, previous,
                                    "each callback received a fresh stall budget"
                                );
                            } else {
                                batch_threshold.set(Some(threshold));
                            }
                            if !deferred_cleanup {
                                thread::sleep(Duration::from_millis(20));
                            }
                            visited.set(visited.get() + 1);
                            if visited.get() == 4 {
                                handles_for_callback.borrow_mut().clear();
                                sender.try_send(()).unwrap();
                            }
                        })
                        .unwrap();
                    handles.borrow_mut().push(worker);
                }
                receiver.recv().await.unwrap();
                assert_eq!(visited.get(), 4);
                assert!(
                    handler.detections.load(Ordering::Relaxed) > 0,
                    "the complete idle batch exceeded its budget without a stall report"
                );
            });
    }

    #[test]
    fn stall_detector_monitors_the_combined_idle_callback_runtime() {
        assert_whole_batch_is_monitored(false);
    }

    #[test]
    fn stall_detector_includes_deferred_callback_cleanup() {
        assert_whole_batch_is_monitored(true);
    }

    #[test]
    fn stall_detector_excludes_parking_after_idle_callbacks() {
        let handler = IdleStallHandler::default();
        LocalExecutorBuilder::default()
            .io_memory(0)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(1))
            .preempt_timer(Duration::from_millis(30))
            .detect_stalls(Some(Box::new(handler.clone())))
            .make()
            .unwrap()
            .run(async {
                let calls = Rc::new(Cell::new(0));
                let worker = executor()
                    .register_before_park({
                        let calls = calls.clone();
                        move || calls.set(calls.get() + 1)
                    })
                    .unwrap();
                Timer::new(Duration::from_millis(100)).await;
                assert!(calls.get() > 0);
                assert_eq!(handler.detections.load(Ordering::Relaxed), 0);
                drop(worker);
            });
    }

    #[test]
    fn panicking_idle_callback_is_accounted_and_disarms_stall_detection() {
        let handler = IdleStallHandler::default();
        let ex = LocalExecutorBuilder::default()
            .io_memory(0)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(1))
            .preempt_timer(Duration::from_millis(30))
            .detect_stalls(Some(Box::new(handler.clone())))
            .make()
            .unwrap();
        let elapsed = Rc::new(Cell::new(Duration::ZERO));
        let worker = ex.run(async {
            let elapsed = elapsed.clone();
            executor()
                .register_before_park(move || {
                    let start = Instant::now();
                    thread::sleep(Duration::from_millis(80));
                    elapsed.set(start.elapsed());
                    panic!("idle callback stalled and panicked");
                })
                .unwrap()
        });
        assert!(catch_unwind(AssertUnwindSafe(|| ex.run(pending::<()>()))).is_err());
        drop(worker);
        let detections = handler.detections.load(Ordering::Relaxed);
        assert!(detections > 0);
        ex.run(async {
            let stats = executor().executor_stats();
            assert!(stats.total_runtime() >= elapsed.get());
            assert!(stats.executor_runtime() >= elapsed.get());
            assert!(
                executor()
                    .task_queue_stats(TaskQueueHandle::default())
                    .unwrap()
                    .runtime()
                    >= elapsed.get()
            );
            Timer::new(Duration::from_millis(100)).await;
            assert_eq!(handler.detections.load(Ordering::Relaxed), detections);
        });
    }

    #[test]
    fn idle_workers_run_once_per_timer_interval_before_spinning() {
        let affinity = nix::sched::sched_getaffinity(nix::unistd::Pid::from_raw(0)).unwrap();
        let cpu = (0..nix::sched::CpuSet::count())
            .find(|&cpu| affinity.is_set(cpu).unwrap())
            .expect("test thread has no available CPU");
        LocalExecutorBuilder::new(Placement::Fixed(cpu))
            .io_memory(0)
            .blocking_thread_pool_placement(PoolPlacement::Unbound(1))
            .spin_before_park(Duration::from_millis(1))
            .spawn(|| async {
                let calls = Rc::new(Cell::new(0));
                let worker = executor()
                    .register_before_park({
                        let calls = calls.clone();
                        move || calls.set(calls.get() + 1)
                    })
                    .unwrap();
                let mut interval = Timer::new(Duration::from_micros(100));
                for tick in 1..=5 {
                    (&mut interval).await;
                    assert_eq!(
                        calls.get(),
                        tick,
                        "expected one idle batch per timer interval"
                    );
                    interval.reset(Duration::from_micros(100));
                }
                drop(worker);
            })
            .expect("failed to create spinning idle-worker test executor")
            .join()
            .expect("spinning idle-worker test executor panicked");
    }

    #[test]
    fn entire_batch_precedes_spawned_work_and_prevents_parking() {
        let ex = local_executor();
        ex.run(async {
            let (sender, receiver) = local_channel::new_unbounded();
            let sender = Rc::new(sender);
            let visited = Rc::new(Cell::new(0));
            let workers = (0..4)
                .map(|index| {
                    let visited = visited.clone();
                    let sender = sender.clone();
                    executor()
                        .register_before_park(move || {
                            visited.set(visited.get() + 1);
                            let visited = visited.clone();
                            let sender = sender.clone();
                            let future = async move {
                                assert_eq!(
                                    visited.get(),
                                    4,
                                    "spawned work interrupted the idle batch"
                                );
                                sender.try_send(index).unwrap();
                            };
                            if index % 2 == 0 {
                                spawn_local(future).detach();
                            } else {
                                spawn_local_into(future, TaskQueueHandle::default())
                                    .unwrap()
                                    .detach();
                            }
                        })
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let mut completed = Vec::new();
            for _ in 0..4 {
                completed.push(receiver.recv().await.unwrap());
            }
            completed.sort_unstable();
            assert_eq!(completed, vec![0, 1, 2, 3]);
            drop(workers);
        });
    }

    #[test]
    fn worker_mutations_take_effect_in_the_next_batch() {
        let ex = local_executor();
        ex.run(async {
            let (sender, receiver) = local_channel::new_unbounded();
            let sender = Rc::new(sender);
            let visited = Rc::new(Cell::new(0));
            let handles: Rc<RefCell<Vec<BeforeParkHandle>>> = Rc::default();
            let replacement = Rc::new(RefCell::new(None));
            let changed = Rc::new(Cell::new(false));
            for _ in 0..4 {
                let handles_for_worker = handles.clone();
                let replacement = replacement.clone();
                let changed = changed.clone();
                let visited = visited.clone();
                let sender = sender.clone();
                let worker = executor()
                    .register_before_park(move || {
                        visited.set(visited.get() + 1);
                        if !changed.replace(true) {
                            handles_for_worker.borrow_mut().clear();
                            let visited = visited.clone();
                            let sender_for_replacement = sender.clone();
                            *replacement.borrow_mut() =
                                Some(executor().register_before_park(move || {
                                    assert_eq!(visited.get(), 4);
                                    sender_for_replacement.try_send("replacement").unwrap();
                                }));
                            sender.try_send("original").unwrap();
                        }
                    })
                    .unwrap();
                handles.borrow_mut().push(worker);
            }
            assert_eq!(receiver.recv().await, Some("original"));
            assert_eq!(
                visited.get(),
                4,
                "removal skipped members of the current batch"
            );
            assert_eq!(receiver.recv().await, Some("replacement"));
            drop(replacement.borrow_mut().take());
        });
    }

    #[test]
    fn each_batch_moves_its_first_worker_and_preserves_fn_mut_state() {
        let ex = local_executor();
        ex.run(async {
            let visited = Rc::new(RefCell::new(Vec::new()));
            let waker: Rc<RefCell<Option<Waker>>> = Rc::default();
            let workers = (0..4)
                .map(|index| {
                    let visited = visited.clone();
                    let waker = waker.clone();
                    let mut count = 0;
                    executor()
                        .register_before_park(move || {
                            count += 1;
                            visited.borrow_mut().push((index, count));
                            if let Some(waker) = waker.borrow_mut().take() {
                                waker.wake();
                            }
                        })
                        .unwrap()
                })
                .collect::<Vec<_>>();
            for batch in 1..=5 {
                poll_fn(|cx| {
                    if visited.borrow().len() >= batch * 4 {
                        Poll::Ready(())
                    } else {
                        *waker.borrow_mut() = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                let visited = visited.borrow();
                assert_eq!(visited.len(), batch * 4);
                assert!(visited[(batch - 1) * 4..]
                    .iter()
                    .all(|(_, count)| *count == batch));
                if batch > 1 {
                    assert_ne!(visited[(batch - 1) * 4].0, visited[(batch - 2) * 4].0);
                }
            }
            drop(workers);
        });
    }

    #[test]
    fn runnable_tasks_do_not_trigger_idle_workers() {
        let ex = local_executor();
        let calls = Rc::new(Cell::new(0));
        let worker = ex.run(async {
            let calls = calls.clone();
            let worker = executor()
                .register_before_park(move || calls.set(calls.get() + 1))
                .unwrap();
            for _ in 0..10 {
                executor().yield_now().await;
            }
            worker
        });
        assert_eq!(calls.get(), 0);
        drop(worker);
        ex.run(async {
            let (sender, receiver) = local_channel::new_bounded(1);
            let _worker = executor()
                .register_before_park(move || sender.try_send(()).unwrap())
                .unwrap();
            receiver.recv().await.unwrap();
        });
        assert_eq!(calls.get(), 0, "dropped worker was invoked");
    }

    struct OnDrop(Option<Box<dyn FnOnce()>>);

    impl Drop for OnDrop {
        fn drop(&mut self) {
            if let Some(callback) = self.0.take() {
                callback();
            }
        }
    }

    #[test]
    fn dropping_a_handle_between_runs_enters_its_owner_context() {
        let owner = local_executor();
        let dropped = Rc::new(Cell::new(false));
        let worker = owner.run(async {
            let owner_id = executor().id();
            let guard = OnDrop(Some(Box::new({
                let dropped = dropped.clone();
                move || {
                    assert_eq!(executor().id(), owner_id);
                    assert_eq!(executor().current_task_queue(), TaskQueueHandle::default());
                    let nested = executor().register_before_park(|| {}).unwrap();
                    drop(nested);
                    spawn_local(async move { dropped.set(true) }).detach();
                }
            })));
            executor()
                .register_before_park(move || {
                    let _ = &guard;
                })
                .unwrap()
        });
        drop(worker);
        assert!(
            dropped.get(),
            "callback destruction did not poll its spawned task"
        );
        crate::executor::with_executor_context(|context| assert!(context.is_none()));
    }

    #[test]
    fn dropping_a_handle_in_another_executor_restores_foreign_context() {
        let owner = local_executor();
        let other = local_executor();
        let dropped = Rc::new(Cell::new(false));
        let worker = owner.run(async {
            let owner_id = executor().id();
            let guard = OnDrop(Some(Box::new({
                let dropped = dropped.clone();
                move || {
                    assert_eq!(executor().id(), owner_id);
                    dropped.set(true);
                }
            })));
            executor()
                .register_before_park(move || {
                    let _ = &guard;
                })
                .unwrap()
        });
        other.run(async {
            let other_id = executor().id();
            let other_queue = executor().current_task_queue();
            let reactor = executor().reactor();
            drop(worker);
            assert!(dropped.get());
            assert_eq!(executor().id(), other_id);
            assert_eq!(executor().current_task_queue(), other_queue);
            assert!(Rc::ptr_eq(&executor().reactor(), &reactor));
        });
    }

    #[test]
    fn callback_destructor_panic_restores_foreign_context() {
        let owner = local_executor();
        let other = local_executor();
        let worker = owner.run(async {
            let owner_id = executor().id();
            let guard = OnDrop(Some(Box::new(move || {
                assert_eq!(executor().id(), owner_id);
                panic!("callback destructor failed");
            })));
            executor()
                .register_before_park(move || {
                    let _ = &guard;
                })
                .unwrap()
        });
        other.run(async {
            let other_id = executor().id();
            let other_queue = executor().current_task_queue();
            let error = catch_unwind(AssertUnwindSafe(|| drop(worker)))
                .expect_err("callback destructor should unwind");
            assert_eq!(
                error.downcast_ref::<&str>(),
                Some(&"callback destructor failed")
            );
            assert_eq!(executor().id(), other_id);
            assert_eq!(executor().current_task_queue(), other_queue);
        });
        owner.run(async {
            let _worker = executor().register_before_park(|| {}).unwrap();
        });
    }

    #[test]
    fn fallible_registration_reports_capacity_and_releases_rejected_captures() {
        let ex = local_executor();
        ex.run(async {
            let mut handles = (0..256)
                .map(|_| executor().register_before_park(|| {}).unwrap())
                .collect::<Vec<_>>();
            let dropped = Rc::new(Cell::new(false));
            let owner_id = executor().id();
            let guard = OnDrop(Some(Box::new({
                let dropped = dropped.clone();
                move || {
                    assert_eq!(executor().id(), owner_id);
                    dropped.set(true);
                }
            })));
            let error = executor()
                .register_before_park(move || {
                    let _ = &guard;
                })
                .unwrap_err();
            let crate::GlommioError::IoError(error) = error else {
                panic!("capacity exhaustion must return an I/O error");
            };
            assert_eq!(
                error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<BeforeParkCapacityError>(),
                Some(&BeforeParkCapacityError),
            );
            assert!(dropped.get());
            drop(handles.pop());
            let replacement = executor().register_before_park(|| {}).unwrap();
            drop(replacement);
        });
    }

    #[test]
    fn executor_shutdown_releases_workers_and_restores_foreign_context() {
        let owner = local_executor();
        let other = local_executor();
        let dropped = Rc::new(Cell::new(false));
        let (worker, weak_reactor) = owner.run(async {
            let reactor = executor().reactor();
            let weak = Rc::downgrade(&reactor);
            let owner_id = owner.id();
            let guard = OnDrop(Some(Box::new({
                let dropped = dropped.clone();
                move || {
                    assert_eq!(executor().id(), owner_id);
                    assert_eq!(executor().current_task_queue(), TaskQueueHandle::default());
                    let worker = executor().register_before_park(|| {}).unwrap();
                    drop(worker);
                    dropped.set(true);
                }
            })));
            let worker = executor()
                .register_before_park(move || {
                    let _ = (&guard, &reactor);
                })
                .unwrap();
            (worker, weak)
        });
        other.run(async {
            let other_id = executor().id();
            drop(owner);
            assert_eq!(executor().id(), other_id);
        });
        assert!(dropped.get());
        assert!(
            weak_reactor.upgrade().is_none(),
            "callback kept the reactor alive"
        );
        drop(worker);
    }

    #[test]
    fn callback_panic_restores_eager_spawning() {
        let ex = local_executor();
        let worker = ex.run(async {
            executor()
                .register_before_park(|| panic!("idle worker failed"))
                .unwrap()
        });
        assert!(catch_unwind(AssertUnwindSafe(|| ex.run(pending::<()>()))).is_err());
        drop(worker);
        ex.run(async {
            let polled = Rc::new(Cell::new(false));
            let task = spawn_local({
                let polled = polled.clone();
                async move { polled.set(true) }
            });
            assert!(polled.get(), "idle-batch spawn deferral survived unwinding");
            task.await;
        });
    }

    #[test]
    #[should_panic(expected = "this thread doesn't have a LocalExecutor running")]
    fn registration_requires_a_running_executor() {
        let _worker = executor().register_before_park(|| {}).unwrap();
    }
}
