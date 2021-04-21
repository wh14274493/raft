package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetResponseCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetResponseCommandFactory;
import cn.ttplatform.wh.cmd.handler.GetCommandHandler;
import cn.ttplatform.wh.cmd.handler.SetCommandHandler;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.MessageType;
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
import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.core.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import cn.ttplatform.wh.core.support.MessageFactoryManager;
import cn.ttplatform.wh.core.support.Scheduler;
import cn.ttplatform.wh.core.support.TaskExecutor;
import io.protostuff.LinkedBuffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
@Getter
@Builder
@AllArgsConstructor
public class NodeContext {

    private Node node;
    private BufferPool<LinkedBuffer> pool;
    private MessageFactoryManager factoryManager;
    private MessageDispatcher dispatcher;
    private final Scheduler scheduler;
    private final TaskExecutor executor;
    private final NodeState nodeState;
    private final Log log;
    private final Cluster cluster;
    private final ServerProperties properties;

    public void register(Node node) {
        this.node = node;
        initMessageHandler();
        initMessageFactory();
    }

    private void initMessageHandler() {
        dispatcher = new MessageDispatcher();

        dispatcher.register(MessageType.SET_COMMAND, new SetCommandHandler(node));
        dispatcher.register(MessageType.GET_COMMAND, new GetCommandHandler(node));

        dispatcher.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageHandler(node));
        dispatcher
            .register(MessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageHandler(node));
        dispatcher.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageHandler(node));
        dispatcher.register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageHandler(node));
        dispatcher.register(MessageType.PRE_VOTE, new PreVoteMessageHandler(node));
        dispatcher.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageHandler(node));
        dispatcher.register(MessageType.REQUEST_VOTE, new RequestVoteMessageHandler(node));
        dispatcher.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageHandler(node));
    }

    private void initMessageFactory() {
        factoryManager = new MessageFactoryManager();
        pool = new FixedSizeLinkedBufferPool(properties.getLinkedBuffPollSize());
        factoryManager.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandFactory(pool));
        factoryManager.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandFactory(pool));
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(pool));
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(pool));

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
