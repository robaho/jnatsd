package com.robaho.jnatsd.util;

/**
 * ring buffer designed for multiple writers and a single reader
 *
 * @param <T>
 */
public class RingBuffer<T> {
    private final T[] ring;
    private volatile int head;
    private volatile int tail;

    public RingBuffer(int size){
        ring = (T[]) new Object[size];
    }

    public synchronized boolean offer(T t) {
        int n = next(tail);
        if(n!=head) {
            ring[tail]=t;
            tail=n;
            return true;
        }
        return false;
    }
    public T poll() {
        if(head==tail){
            return null;
        }
        T tmp = ring[head];
        ring[head]=null;
        head = next(head);
        return tmp;
    }
    public boolean isEmpty() {
        return head==tail;
    }
    private int next(int index) {
        return (++index)%ring.length;
    }
}
