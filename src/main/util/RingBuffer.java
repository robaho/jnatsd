package com.robaho.jnatsd.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
        for(int i=0;SPINS && i<SPIN_COUNT;i++) {
            if(offer(t)) {
                synchronized (this) {
                    notifyAll();
                }
                return;
            }
            Thread.yield();
        }
        synchronized (this) {
            while(!shutdown) {
                if(offer(t)) {
                    notifyAll();
                    return;
                }
                wait();
            }
            throw new InterruptedException("queue shutdown");
        }
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
        // try to get without lock in case available was used
        {
            T t = poll();
            if (t != null) {
                synchronized (this) {
                    notifyAll();
                }
                return t;
            }
        }

        synchronized (this) {
            while(!shutdown) {
                T t = poll();
                if(t!=null) {
                    notifyAll();
                    return t;
                }
                wait();
            }
            throw new InterruptedException("queue shutdown");
        }
    }
    public boolean available() {
        for(int i=0;(i==0 || SPINS) && i<SPIN_COUNT;i++) {
            if(head!=tail.get() || ring.get(tail.get())!=null) {
                return true;
            }
            Thread.yield();
        }
        return false;
    }
    private int next(int index) {
        return (++index)%size;
    }
    public void shutdown() {
        shutdown=true;
        synchronized (this) {
            notifyAll();
        }
    }
}
