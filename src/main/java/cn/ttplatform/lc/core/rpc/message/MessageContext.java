package cn.ttplatform.lc.core.rpc.message;

import cn.ttplatform.lc.config.ServerProperties;
import cn.ttplatform.lc.constant.RpcMessageType;
import cn.ttplatform.lc.core.Node;
import cn.ttplatform.lc.core.rpc.message.factory.AppendLogEntriesMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.AppendLogEntriesResultMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.InstallSnapshotMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.InstallSnapshotResultMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.MessageFactoryManager;
import cn.ttplatform.lc.core.rpc.message.factory.PreVoteMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.PreVoteResultMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.RequestVoteMessageFactory;
import cn.ttplatform.lc.core.rpc.message.factory.RequestVoteResultMessageFactory;
import cn.ttplatform.lc.core.rpc.message.handler.AppendLogEntriesMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.AppendLogEntriesResultMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.InstallSnapshotMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.InstallSnapshotResultMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.MessageDispatcher;
import cn.ttplatform.lc.core.rpc.message.handler.PreVoteMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.PreVoteResultMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.RequestVoteMessageHandler;
import cn.ttplatform.lc.core.rpc.message.handler.RequestVoteResultMessageHandler;
import lombok.Getter;

/**
 * @author Wang Hao
 * @date 2021/2/18 19:19
 */
@Getter
public class MessageContext {

    private final Node node;
    private final ServerProperties properties;
    private final LinkedBufferPool pool;
    private final MessageFactoryManager factoryManager;
    private final MessageDispatcher dispatcher;

    public MessageContext(ServerProperties properties, Node node) {
        this.properties = properties;
        this.node = node;
        this.pool = new LinkedBufferPool(properties.getBuffPollSize());
        this.dispatcher = new MessageDispatcher();
        this.factoryManager = new MessageFactoryManager();
        initMessageHandler();
        initMessageFactory();
    }

    public void initMessageHandler() {
        dispatcher.register(RpcMessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageHandler(node));
        dispatcher.register(RpcMessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageHandler(node));
        dispatcher.register(RpcMessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageHandler(node));
        dispatcher.register(RpcMessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageHandler(node));
        dispatcher.register(RpcMessageType.PRE_VOTE, new PreVoteMessageHandler(node));
        dispatcher.register(RpcMessageType.PRE_VOTE_RESULT, new PreVoteResultMessageHandler(node));
        dispatcher.register(RpcMessageType.REQUEST_VOTE, new RequestVoteMessageHandler(node));
        dispatcher.register(RpcMessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageHandler(node));
    }

    public void initMessageFactory() {
        factoryManager.register(RpcMessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageFactory());
        factoryManager.register(RpcMessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageFactory());
        factoryManager.register(RpcMessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageFactory());
        factoryManager.register(RpcMessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageFactory());
        factoryManager.register(RpcMessageType.PRE_VOTE, new PreVoteMessageFactory());
        factoryManager.register(RpcMessageType.PRE_VOTE_RESULT, new PreVoteResultMessageFactory());
        factoryManager.register(RpcMessageType.REQUEST_VOTE, new RequestVoteMessageFactory());
        factoryManager.register(RpcMessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageFactory());
    }

}
