package cn.ttplatform.lc.schedule;

import cn.ttplatform.lc.environment.SettingProperties;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:28
 */
public class DefaultScheduler implements Scheduler {

    private final SettingProperties properties;
    private final ScheduledExecutorService executor;

    public DefaultScheduler(SettingProperties properties) {
        this.properties = properties;
        executor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "scheduler"));
    }

    public ScheduledFuture<?> scheduleElectionTimeoutTask(Runnable task) {
        int timeout =
            new Random().nextInt(properties.getMaxElectionTimeout() - properties.getMinElectionTimeout()) + properties
                .getMinElectionTimeout();
        return executor.schedule(task, timeout, TimeUnit.MILLISECONDS);
    }

    public ScheduledFuture<?> scheduleLogReplicationTask(Runnable task) {
        return executor
            .scheduleWithFixedDelay(task, properties.getLogReplicationDelay(), properties.getLogReplicationInterval(),
                TimeUnit.MILLISECONDS);
    }
}
