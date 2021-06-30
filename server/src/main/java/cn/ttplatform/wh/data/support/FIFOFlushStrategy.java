package cn.ttplatform.wh.data.support;

import cn.ttplatform.wh.support.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Wang Hao
 * @date 2021/6/8 10:39
 */
@Slf4j
public class FIFOFlushStrategy implements FlushStrategy {

    private final BlockingQueue<AsyncFileOperator.Block> queue = new LinkedBlockingQueue<>();
    private final ScheduledThreadPoolExecutor executor;
    private volatile boolean shutdown;

    public FIFOFlushStrategy(long interval) {
        this.executor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("fifo-flush-"));
        executor.scheduleAtFixedRate(() -> {
            if (queue.isEmpty()) {
                return;
            }
            try {
                AsyncFileOperator.Block block = queue.poll(interval / 2, TimeUnit.MILLISECONDS);
                if (block != null && block.dirty()) {
                    block.flush();
                    log.info("flush a dirty block[{}].", block);
                }
            } catch (InterruptedException e) {
                log.info("a flush task had been interrupt. reason: {}", e.getLocalizedMessage());
                Thread.currentThread().interrupt();
            }

        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void flush(AsyncFileOperator.Block block) {
        if (!shutdown) {
            queue.offer(block);
        }
    }

    @Override
    public void synFlushAll() {
        shutdown = true;
        while (!queue.isEmpty()) {
            try {
                AsyncFileOperator.Block block = queue.poll(100L, TimeUnit.MILLISECONDS);
                if (block != null && block.dirty()) {
                    block.flush();
                    log.info("flush a dirty block[{}].", block);
                }
            } catch (InterruptedException e) {
                log.info("a flush task had been interrupt. reason: {}", e.getLocalizedMessage());
                Thread.currentThread().interrupt();
            }
        }
        executor.shutdown();
    }
}
