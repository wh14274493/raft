package cn.ttplatform.wh.core;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.common.BufferPool;
import cn.ttplatform.wh.core.common.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.core.common.MessageDispatcher;
import cn.ttplatform.wh.core.common.MessageFactoryManager;
import cn.ttplatform.wh.core.common.Scheduler;
import cn.ttplatform.wh.core.common.TaskExecutor;
import cn.ttplatform.wh.core.connector.message.factory.AppendLogEntriesMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.AppendLogEntriesResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.InstallSnapshotMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.InstallSnapshotResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.PreVoteMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.PreVoteResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.RequestVoteMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.RequestVoteResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.handler.AppendLogEntriesMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.AppendLogEntriesResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.InstallSnapshotMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.InstallSnapshotResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.PreVoteMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.PreVoteResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.RequestVoteMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.RequestVoteResultMessageHandler;
import cn.ttplatform.wh.core.log.Log;
import io.protostuff.LinkedBuffer;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
@Builder
@AllArgsConstructor
public class NodeContext {

    private Node node;
    private BufferPool<LinkedBuffer> pool;
    private MessageFactoryManager factoryManager;
    private MessageDispatcher messageDispatcher;
    private final Scheduler scheduler;
    private final TaskExecutor taskExecutor;
    private final NodeState nodeState;
    private final Log log;
    private final Cluster cluster;
    private final ServerProperties properties;

    public Node node() {
        return node;
    }

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

    public BufferPool<LinkedBuffer> bufferPool() {
        return pool;
    }

    public MessageFactoryManager factoryManager() {
        return factoryManager;
    }

    public MessageDispatcher messageDispatcher() {
        return messageDispatcher;
    }

    public void register(Node node) {
        this.node = node;
        initMessageHandler();
        initMessageFactory();
    }

    private void initMessageHandler() {
        messageDispatcher = new MessageDispatcher();
        messageDispatcher.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageHandler(node));
        messageDispatcher
            .register(MessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageHandler(node));
        messageDispatcher.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageHandler(node));
        messageDispatcher.register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageHandler(node));
        messageDispatcher.register(MessageType.PRE_VOTE, new PreVoteMessageHandler(node));
        messageDispatcher.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageHandler(node));
        messageDispatcher.register(MessageType.REQUEST_VOTE, new RequestVoteMessageHandler(node));
        messageDispatcher.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageHandler(node));
    }

    private void initMessageFactory() {
        factoryManager = new MessageFactoryManager();
        pool = new FixedSizeLinkedBufferPool(properties.getBuffPollSize());
        factoryManager.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageFactory(pool));
        factoryManager.register(MessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageFactory(pool));
        factoryManager.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageFactory(pool));
        factoryManager.register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageFactory(pool));
        factoryManager.register(MessageType.PRE_VOTE, new PreVoteMessageFactory(pool));
        factoryManager.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageFactory(pool));
        factoryManager.register(MessageType.REQUEST_VOTE, new RequestVoteMessageFactory(pool));
        factoryManager.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageFactory(pool));
    }
}
