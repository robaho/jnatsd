package com.robaho.jnatsd.util;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * an efficient non-reentrant lock
 */
public class ExclusiveLock {
    private static class WaitList {
        private final LinkedList<Thread> list = new LinkedList<>();
        private final AtomicBoolean lock = new AtomicBoolean();
        void add(Thread thread) {
            while(!lock.compareAndSet(false,true));
            list.add(thread);
            lock.set(false);
        }
        Thread peek() {
            while(!lock.compareAndSet(false,true));
            try {
                return list.peek();
            } finally {
                lock.set(false);
            }
        }
        void remove(Thread thread) {
            while(!lock.compareAndSet(false,true));
            list.remove(thread);
            lock.set(false);
        }
    }
    private final int SPIN_COUNT=1;
    private final WaitList waiters = new WaitList();
    private AtomicReference<Thread> holder = new AtomicReference<>();

    public boolean tryLock() {
        return holder.compareAndSet(null,Thread.currentThread());
    }

    public void lock() {
        Thread locker = Thread.currentThread();
        // spin a bit trying to lock to avoid context switch
        for(int i=0;i<SPIN_COUNT;i++) {
            if (tryLock())
                return;
        }
        if(holder.get()==locker)
            throw new IllegalStateException("already holds lock");
        waiters.add(locker);
        while(true) {
            if(waiters.peek()==locker && tryLock()) {
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
}
