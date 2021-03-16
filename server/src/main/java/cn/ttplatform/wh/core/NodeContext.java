package cn.ttplatform.wh.core;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.log.Log;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
@Builder
@AllArgsConstructor
public class NodeContext {

    private final Scheduler scheduler;
    private final TaskExecutor taskExecutor;
    private final NodeState nodeState;
    private final Log log;
    private final Cluster cluster;
    private final ServerProperties properties;

    public Scheduler scheduler() {
        return scheduler;
    }


    public TaskExecutor executor() {
        return taskExecutor;
    }

    public Log log() {
        return log;
    }

    public Cluster cluster() {
        return cluster;
    }

    public NodeState nodeState() {
        return nodeState;
    }

    public ServerProperties config() {
        return properties;
    }
}
