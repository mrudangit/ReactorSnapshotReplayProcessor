package reactor.mm;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxProcessor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SnapshotReplayProcessor<T> extends FluxProcessor<T, T> {


    private static final Logger logger = LoggerFactory.getLogger(SnapshotReplayProcessor.class);
    private final Function<T, Object> keyFunction;
    private final BiFunction<T,T, T> mergeFunction;


    volatile SnapshotReplaySubscription<T>[] subscribers;

    ConcurrentHashMap<Object,T> replayBuffer = new ConcurrentHashMap<>();


    static final SnapshotReplaySubscription[] EMPTY      = new SnapshotReplaySubscription[0];
    static final SnapshotReplaySubscription[] TERMINATED = new SnapshotReplaySubscription[0];
    static final AtomicReferenceFieldUpdater<SnapshotReplayProcessor, SnapshotReplaySubscription[]>  SUBSCRIBERS = AtomicReferenceFieldUpdater.newUpdater(SnapshotReplayProcessor.class, SnapshotReplaySubscription[].class,"subscribers");


    public SnapshotReplayProcessor(Function<T,Object> keyFunction, BiFunction<T,T,T> mergeFunction) {
        this.keyFunction = keyFunction;
        this.mergeFunction = mergeFunction;
        SUBSCRIBERS.lazySet(this, EMPTY);
    }

    @Override
    public void onSubscribe(Subscription s) {
        logger.info("New Subscription : {}", s);
    }

    @Override
    public void onNext(T t) {
        T newValue = t;

        Object key = keyFunction.apply(t);

        T currentValue = replayBuffer.getOrDefault(key, null);

        if ( currentValue == null){
            replayBuffer.put(key,t);
        }else {
            newValue = mergeFunction.apply(currentValue, t);
            replayBuffer.put(key,newValue);
        }
        for (SnapshotReplaySubscription<T> rs : subscribers) {
            final Subscriber<? super T> a = rs.actual();
            a.onNext(newValue);
        }


    }

    void replayItems(SnapshotReplaySubscription subscription) {
        if (!subscription.enter()) {
            return;
        }

        final Subscriber<? super T> a = subscription.actual();

        replayBuffer.values().forEach(t -> {
            a.onNext(t);
        });

    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error ", t);
    }

    @Override
    public void onComplete() {
        logger.info("Completed");
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {

        logger.info("Subscriber : {}", actual);

        SnapshotReplaySubscription<T> rs = new SnapshotReplaySubscription(actual, this);
        actual.onSubscribe(rs);

        if (add(rs)) {
            if (rs.isCancelled()) {
                remove(rs);
                return;
            }
        }
        // Replay Buffer
        replayItems(rs);

    }

    @SuppressWarnings("unchecked")
    void remove(SnapshotReplaySubscription<T> rs) {
        outer:
        for (; ; ) {
            SnapshotReplaySubscription<T>[] a = subscribers;
            if (a == TERMINATED || a == EMPTY) {
                return;
            }
            int n = a.length;

            for (int i = 0; i < n; i++) {
                if (a[i] == rs) {
                    SnapshotReplaySubscription<T>[] b;

                    if (n == 1) {
                        b = EMPTY;
                    }
                    else {
                        b = new SnapshotReplaySubscription[n - 1];
                        System.arraycopy(a, 0, b, 0, i);
                        System.arraycopy(a, i + 1, b, i, n - i - 1);
                    }

                    if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                        return;
                    }

                    continue outer;
                }
            }

            break;
        }
    }

    boolean add(SnapshotReplaySubscription<T> rs) {
        for (; ; ) {
            SnapshotReplaySubscription<T>[] a = subscribers;
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;

            @SuppressWarnings("unchecked")SnapshotReplaySubscription<T>[] b =
                    new SnapshotReplaySubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = rs;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }
}
