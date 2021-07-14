package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.support.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Wang Hao
 * @date 2021/6/8 10:48
 */
@Slf4j
public class PriorityFlushStrategy implements FlushStrategy {

    private volatile boolean shutdown;
    private final ScheduledThreadPoolExecutor executor;
    private final TreeSet<AsyncFileOperator.Block> blocks;

    public PriorityFlushStrategy(String prefix, long interval) {
        blocks = new TreeSet<>((o1, o2) -> (int) (o1.getStartOffset() - o2.getStartOffset()));
        this.executor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory(prefix + "-flush-"));
        executor.scheduleAtFixedRate(() -> {
            if (blocks.isEmpty()) {
                return;
            }
            AsyncFileOperator.Block block;
            synchronized (blocks) {
                block = blocks.pollFirst();
            }
            if (block == null) {
                return;
            }
            block.flush();
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush(AsyncFileOperator.Block block) {
        if (!shutdown) {
            synchronized (blocks) {
                blocks.add(block);
            }
        }
    }

    @Override
    public void synFlushAll() {
        shutdown = true;
        while (!blocks.isEmpty()) {
            AsyncFileOperator.Block block;
            synchronized (blocks) {
                block = blocks.pollFirst();
            }
            if (block != null) {
                block.flush();
            }
        }
        executor.shutdown();
    }

}
