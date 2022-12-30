package com.robaho.jnatsd.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

/**
 * ring buffer designed for multiple writers and a single reader
 *
 * @param <T>
 */
public class RingBuffer<T> {
    private final AtomicReferenceArray<T> ring;
    private volatile int head;
    private final AtomicInteger tail = new AtomicInteger();
    private final int size;
    private volatile boolean shutdown;

    private static final boolean SPINS=true;
    private static final int SPIN_COUNT=1024;

    private final ConcurrentLinkedQueue<Thread> writers = new ConcurrentLinkedQueue<>();
    private volatile Thread reader;

    private volatile long putParks,putFast;
    private volatile long getParks,getFast;

    public RingBuffer(int size){
        ring = new AtomicReferenceArray<>(size);
        this.size=size;
    }

    private boolean offer(T t) {
        int _tail = tail.get();
        int _next_tail = next(_tail);
        if(ring.get(_tail)==null && _next_tail!=head){
            if(tail.compareAndSet(_tail,_next_tail)){
                return ring.compareAndSet(_tail,null,t);
            }
        }
        return false;
    }

    /** put an item in the ring buffer, blocking until space is available.
     * @param t the item
     * @throws InterruptedException if interrupted
     */
    public void put(T t) throws InterruptedException {
        Thread writer = Thread.currentThread();

        for(int i=0;SPINS && i<SPIN_COUNT;i++) {
            if(offer(t)) {
                putFast++;
                LockSupport.unpark(reader);
                return;
            }
//            LockSupport.parkNanos(1);
//            Thread.yield();
        }

        putParks++;
        writers.add(writer);
        while(!shutdown) {
            if(offer(t)) {
                writers.remove(writer);
                LockSupport.unpark(reader);
                return;
            }
            LockSupport.park();
        }
        throw new InterruptedException("queue shutdown");
    }

    private T poll() {
        T tmp = ring.getAndSet(head,null);
        if(tmp==null)
            return null;
        head=next(head);
        return tmp;
    }

    /** returns the next item available from the ring buffer, blocking
     * if not item is ready
     * @return the item
     * @throws InterruptedException if interrupted
     */
    public T get() throws InterruptedException {
        boolean parked=false;
        reader=Thread.currentThread();
        while(!shutdown) {
            T t = poll();
            if(t!=null) {
                if(parked) {
                    getParks++;
                } else {
                    getFast++;
                }
                reader=null;
                LockSupport.unpark(writers.peek());
                return t;
            }
            parked=true;
            LockSupport.park();
        }
        throw new InterruptedException("queue shutdown");
    }
    public boolean available(long timeout) {
        long deadline = System.nanoTime() + timeout;

        for(int i=0;SPINS && i<SPIN_COUNT;i++) {
            if (ring.get(head) != null) {
                return true;
            }
        }

        reader = Thread.currentThread();
        try {
            while (true) {
                if (ring.get(head) != null) {
                    return true;
                }
                long now = System.nanoTime();
                long diff = deadline-now;
                if (diff<=0)
                    return false;
//            Thread.onSpinWait();
                LockSupport.parkNanos(diff);
//                Thread.yield();
            }
        } finally {
            reader=null;
        }
    }
    private int next(int index) {
        return (++index)%size;
    }

    public String debug() {
        return "put parks "+putParks+", fast "+putFast+", get parks "+getParks+", fast "+getFast;

    }
    public void shutdown() {
        shutdown=true;
        LockSupport.unpark(reader);
        for(Thread writer : writers) {
            LockSupport.unpark(writer);
            writers.remove(writer);
        }
    }
}
