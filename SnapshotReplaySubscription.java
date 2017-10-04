package reactor.mm;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class SnapshotReplaySubscription<T> implements Subscription {
    private final SnapshotReplayProcessor<T> parent;
    private CoreSubscriber<? super T> actual;


    volatile long requested;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SnapshotReplaySubscription> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(SnapshotReplaySubscription.class,
                    "requested");

    volatile int wip;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SnapshotReplaySubscription> WIP =
            AtomicIntegerFieldUpdater.newUpdater(SnapshotReplaySubscription.class,
                    "wip");
    private Object node;

    public SnapshotReplaySubscription(CoreSubscriber<? super T> actual,SnapshotReplayProcessor<T> parent){
        this.parent = parent;
        this.actual=actual;
    }
    @Override
    public void request(long n) {

        parent.replayItems(this);
    }


    public boolean isCancelled() {
        return requested == Long.MIN_VALUE;
    }

    @Override
    public void cancel() {
        if (REQUESTED.getAndSet(this, Long.MIN_VALUE) != Long.MIN_VALUE) {
            parent.remove(this);

            if (enter()) {
                node = null;
            }
        }
    }

    public boolean enter() {
        return WIP.getAndIncrement(this) == 0;
    }

    CoreSubscriber<? super T> actual() {
        return this.actual;
    }
}
