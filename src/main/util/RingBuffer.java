package com.robaho.jnatsd.util;

import java.util.concurrent.ArrayBlockingQueue;
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

    public RingBuffer(int size){
        ring = new ArrayBlockingQueue<>(size);
    }

    /** put an item in the ring buffer, blocking until space is available.
     * @param t the item
     * @throws InterruptedException if interrupted
     */
    public void put(T t) throws InterruptedException {
        ring.put(t);
    }

    /** returns the next item available from the ring buffer, blocking
     * if not item is ready
     * @return the item
     * @throws InterruptedException if interrupted
     */
    public T get() throws InterruptedException {
        return ring.take();
    }
    public boolean available() {
        return !ring.isEmpty();
    }
    public void shutdown() {
        shutdown=true;
        ring.clear();
    }
}
