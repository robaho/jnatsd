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

    public RingBuffer(int size){
        ring = new AtomicReferenceArray<>(size);
        this.size=size;
    }

    public boolean offer(T t) {
        int _tail = tail.get();
        if(ring.get(_tail)==null){
            if(tail.compareAndSet(_tail,next(_tail))){
                return ring.compareAndSet(_tail,null,t);
            }
        }

        return false;
    }
    public T poll() {
        T tmp = ring.getAndSet(head,null);
        if(tmp==null)
            return null;
        head=next(head);
        return tmp;
    }
    private int next(int index) {
        return (++index)%size;
    }
}
