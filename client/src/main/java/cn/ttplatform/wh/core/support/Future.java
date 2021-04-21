package cn.ttplatform.wh.core.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Wang Hao
 * @date 2021/4/20 14:00
 */
public class Future<T> {

    private Object res;
    private List<GenericListener> listeners;
    private Set<Thread> waitThreads;

    public Future<T> addListener(GenericListener listener) {
        if (listeners == null) {
            listeners = new ArrayList<>();
        }
        listeners.add(listener);
        return this;
    }

    public void put(T res) {
        synchronized (this) {
            this.res = res;
            this.notifyAll();
        }
        invokeListeners();
    }

    private void invokeListeners() {
        listeners.forEach(GenericListener::onComplete);
    }

    public T get() throws InterruptedException {
        synchronized (this) {
            while (res == null) {
                addWaiter();
                if (Thread.interrupted()) {
                    break;
                }
                this.wait();
            }
            return (T) res;
        }
    }

    public T get(long timeout) throws InterruptedException {
        long begin = System.currentTimeMillis();
        long waitTime;
        synchronized (this) {
            while (res == null) {
                waitTime = System.currentTimeMillis() - begin;
                if (waitTime < timeout) {
                    addWaiter();
                    if (Thread.interrupted()) {
                        break;
                    }
                    this.wait(timeout - waitTime);
                }
            }
            return (T) res;
        }
    }

    private void addWaiter() {
        if (waitThreads == null) {
            waitThreads = new HashSet<>();
        }
        waitThreads.add(Thread.currentThread());
    }

    public void interruptAllWaiters() {
        waitThreads.forEach(Thread::interrupt);
    }
}
