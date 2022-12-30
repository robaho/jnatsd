package com.robaho.jnatsd.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
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
    private static final int SPIN_COUNT=128;
    private static final long putPauseNS = TimeUnit.MILLISECONDS.toNanos(10);

    private volatile Thread reader;
    // last caller in put() to efficiently wake at least a writer
    private volatile Thread writer;

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
        writer=Thread.currentThread();
        while(!shutdown) {
            if(offer(t)) {
                writer=null;
                LockSupport.unpark(reader);
                return;
            }
            LockSupport.parkNanos(putPauseNS);
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
        reader=Thread.currentThread();
        while(!shutdown) {
            T t = poll();
            if(t!=null) {
                LockSupport.unpark(writer);
                reader=null;
                return t;
            }
            LockSupport.park();
        }
        throw new InterruptedException("queue shutdown");
    }
    public boolean available() {
        for(int i=0;(i==0 || SPINS) && i<SPIN_COUNT;i++) {
            if(head!=tail.get() || ring.get(tail.get())!=null) {
                return true;
            }
//            Thread.onSpinWait();
//            LockSupport.parkNanos(1);
            Thread.yield();
        }
        return false;
    }
    private int next(int index) {
        return (++index)%size;
    }
    public void shutdown() {
        shutdown=true;
        LockSupport.unpark(reader);
    }
}
