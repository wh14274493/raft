package cn.ttplatform.wh.data.pool.strategy;

import cn.ttplatform.wh.data.pool.BlockCache.Block;
import cn.ttplatform.wh.support.NamedThreadFactory;
import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/6/8 10:48
 */
@Slf4j
public class PriorityFlushStrategy implements FlushStrategy {

    private volatile boolean shutdown;
    private final ScheduledThreadPoolExecutor executor;
    private final TreeSet<Block> blocks;
    private final ReentrantLock lock = new ReentrantLock();

    public PriorityFlushStrategy(long interval) {
        blocks = new TreeSet<>((o1, o2) -> (int) (o1.getStartOffset() - o2.getStartOffset()));
        this.executor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("Priority-flush-"));
        executor.scheduleWithFixedDelay(() -> {
            if (blocks.isEmpty()) {
                return;
            }
            Block block;
            lock.lock();
            try {
                block = blocks.pollFirst();
            } finally {
                lock.unlock();
            }
            if (block != null && block.dirty()) {
                block.flush();
                log.info("flush a dirty block[{}].", block);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush(Block block) {
        if (!shutdown) {
            lock.lock();
            try {
                blocks.add(block);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void flush() {
        shutdown = true;
        while (!blocks.isEmpty()) {
            Block block;
            lock.lock();
            try {
                block = blocks.pollFirst();
            } finally {
                lock.unlock();
            }
            if (block != null && block.dirty()) {
                block.flush();
                log.info("flush a dirty block[{}].", block);
            }
        }
        executor.shutdown();
    }
}
