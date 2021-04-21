package cn.ttplatform.wh.core.support;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Wang Hao
 * @date 2021/4/20 14:09
 */
public class AutoIncrementIDGenerator implements IDGenerator {

    private final String prefix;
    private final AtomicInteger index;

    public AutoIncrementIDGenerator(String prefix) {
        this.prefix = prefix;
        this.index = new AtomicInteger();
    }

    public AutoIncrementIDGenerator() {
        this("");
    }

    @Override
    public String generate() {
        return prefix + System.currentTimeMillis() + index.getAndIncrement();
    }
}
