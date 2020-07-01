package cn.ttplatform.lc.schedule;

import cn.ttplatform.lc.environment.SettingProperties;
import java.util.concurrent.ScheduledFuture;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:25
 */
public interface Scheduler {

    default DefaultScheduler newDefaultScheduler(SettingProperties properties) {
        return new DefaultScheduler(properties);
    }

    ScheduledFuture<?> scheduleElectionTimeoutTask(Runnable task);

    ScheduledFuture<?> scheduleLogReplicationTask(Runnable task);
}
