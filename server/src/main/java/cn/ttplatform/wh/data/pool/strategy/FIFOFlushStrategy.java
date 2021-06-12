package cn.ttplatform.wh.data.pool.strategy;

import cn.ttplatform.wh.data.pool.BlockCache.Block;
import cn.ttplatform.wh.support.NamedThreadFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Wang Hao
 * @date 2021/6/8 10:39
 */
public class FIFOFlushStrategy implements FlushStrategy{

    static class FlushTask implements Runnable {

        Block block;

        FlushTask(Block block) {
            this.block = block;
        }

        @Override
        public void run() {

        }
    }

    private final BlockingQueue<Block> queue = new ArrayBlockingQueue<>(10);
    private final ScheduledThreadPoolExecutor executor;

    public FIFOFlushStrategy(ScheduledThreadPoolExecutor executor) {
        this.executor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("flush-"));
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (queue.isEmpty()) {
                    return;
                }
                // TODO
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush(Block block) {
        queue.add(block);
    }
}
