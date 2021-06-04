package cn.ttplatform.wh.support;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Wang Hao
 * @date 2021/6/1 22:05
 */
public class NamedThreadFactory implements java.util.concurrent.ThreadFactory {

    private final String prefix;
    private final AtomicInteger count;

    public NamedThreadFactory(String prefix) {
        this.prefix = prefix;
        this.count = new AtomicInteger();
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, prefix + count.getAndIncrement());
    }
}
