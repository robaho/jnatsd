package com.robaho.jnatsd.util;

import java.util.concurrent.ArrayBlockingQueue;
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
    private final ArrayBlockingQueue<T> ring;
    private volatile boolean shutdown;
    private T ready;

    public RingBuffer(int size){
        ring = new ArrayBlockingQueue<>(size);
    }

    /** put an item in the ring buffer, blocking until space is available.
     * @param t the item
     * @throws InterruptedException if interrupted
     */
    public void put(T t) throws InterruptedException {
        if(shutdown)
            return;
        ring.put(t);
    }

    /** returns the next item available from the ring buffer, blocking
     * if not item is ready
     * @return the item
     * @throws InterruptedException if interrupted
     */
    public T get() throws InterruptedException {
        if(shutdown)
            throw new InterruptedException("queue shutdown");
        if(ready!=null) {
            T t = ready;
            ready=null;
            return t;
        }
        return ring.take();
    }
    public boolean available(long timeout) {
        if(shutdown)
            return false;
        try {
            ready = ring.poll(timeout, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            return false;
        }
        return ready!=null;
    }

    public String debug() {
        return "";
    }
    public void shutdown() {
        shutdown=true;
        ring.clear();
    }
}
