package com.robaho.jnatsd.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * an efficient non-reentrant lock
 */
public class ExclusiveLock {
    private final int SPIN_COUNT=16;
    private final ConcurrentLinkedQueue<Thread> waiters = new ConcurrentLinkedQueue<>();
    private AtomicReference<Thread> holder = new AtomicReference<>();

    private boolean tryLock(Thread locker) {
        return holder.compareAndSet(null,locker);
    }

    public void lock() {
        Thread locker = Thread.currentThread();
        // spin a bit trying to lock to avoid context switch
        for(int i=0;i<SPIN_COUNT;i++) {
            if (tryLock(locker))
                return;
        }
        if(holder.get()==locker)
            throw new IllegalStateException("already holds lock");
        waiters.add(locker);
        while(true) {
            if(waiters.peek()==locker && tryLock(locker)) {
                waiters.remove(locker);
                return;
            } else {
                LockSupport.park();
            }
        }
    }
    public void unlock() {
        if(!holder.compareAndSet(Thread.currentThread(),null))
            throw new IllegalStateException("unlock does not hold lock");
        LockSupport.unpark(waiters.peek());
    }

    public boolean hasWaiters() {
        return !waiters.isEmpty();
    }
}
