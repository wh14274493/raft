package cn.ttplatform.wh.core.common;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.BufferPool;
import cn.ttplatform.wh.core.Node;
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
import cn.ttplatform.wh.core.server.command.factory.GetCommandFactory;
import cn.ttplatform.wh.core.server.command.factory.SetCommandFactory;
import cn.ttplatform.wh.core.server.command.handler.GetCommandHandler;
import cn.ttplatform.wh.core.server.command.handler.SetCommandHandler;
import io.protostuff.LinkedBuffer;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2021/2/18 19:19
 */
@Getter
public class MessageContext {

    private Node node;
    private final ServerProperties properties;
    private final BufferPool<LinkedBuffer> pool;
    private final MessageFactoryManager factoryManager;
    private final MessageDispatcher dispatcher;

    public MessageContext(ServerProperties properties) {
        this.properties = properties;
        this.pool = new FixedSizeLinkedBufferPool(properties.getBuffPollSize());
        this.dispatcher = new MessageDispatcher();
        this.factoryManager = new MessageFactoryManager();
        initMessageFactory();
    }

    public void register(Node node) {
        this.node = node;
        initMessageHandler();
    }

    public void initMessageHandler() {
        dispatcher.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageHandler(node));
        dispatcher.register(MessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageHandler(node));
        dispatcher.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageHandler(node));
        dispatcher.register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageHandler(node));
        dispatcher.register(MessageType.PRE_VOTE, new PreVoteMessageHandler(node));
        dispatcher.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageHandler(node));
        dispatcher.register(MessageType.REQUEST_VOTE, new RequestVoteMessageHandler(node));
        dispatcher.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageHandler(node));
        dispatcher.register(MessageType.SET_COMMAND, new SetCommandHandler(node));
        dispatcher.register(MessageType.GET_COMMAND, new GetCommandHandler(node));
    }

    public void initMessageFactory() {
        factoryManager.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageFactory(pool));
        factoryManager.register(MessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageFactory(pool));
        factoryManager.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageFactory(pool));
        factoryManager.register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageFactory(pool));
        factoryManager.register(MessageType.PRE_VOTE, new PreVoteMessageFactory(pool));
        factoryManager.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageFactory(pool));
        factoryManager.register(MessageType.REQUEST_VOTE, new RequestVoteMessageFactory(pool));
        factoryManager.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageFactory(pool));
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(pool));
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(pool));
    }

}
