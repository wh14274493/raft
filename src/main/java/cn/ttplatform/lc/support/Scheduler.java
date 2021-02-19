package cn.ttplatform.lc.support;

import cn.ttplatform.lc.config.ServerProperties;
import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Scheduler {

    default DefaultScheduler newDefaultScheduler(ServerProperties properties) {
        return new DefaultScheduler(properties);
    }

    ScheduledFuture<?> scheduleElectionTimeoutTask(Runnable task);

    ScheduledFuture<?> scheduleLogReplicationTask(Runnable task);

}
