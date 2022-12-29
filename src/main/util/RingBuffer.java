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
    private int head;
    private final AtomicInteger tail = new AtomicInteger();
    private final int size;
    private volatile boolean shutdown;

    public RingBuffer(int size){
        ring = new AtomicReferenceArray<>(size);
        this.size=size;
    }

    private boolean offer(T t) {
        int _tail = tail.get();
        if(ring.get(_tail)==null){
            if(tail.compareAndSet(_tail,next(_tail))){
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
        for(int i=0;i<1000;i++) {
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
                if(offer(t))
                    return;
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
        for(int i=0;i<1000;i++) {
            T t = poll();
            if(t!=null) {
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
        for(int i=0;i<1000;i++) {
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
