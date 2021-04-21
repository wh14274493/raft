package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetResponseCommandFactory;
import cn.ttplatform.wh.cmd.factory.RedirectCommandFactory;
import cn.ttplatform.wh.cmd.factory.RequestFailedCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetResponseCommandFactory;
import cn.ttplatform.wh.config.ClientProperties;
import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.connector.NioConnector;
import cn.ttplatform.wh.core.handler.GetResponseCommandHandler;
import cn.ttplatform.wh.core.handler.RedirectCommandHandler;
import cn.ttplatform.wh.core.handler.RequestFailedCommandHandler;
import cn.ttplatform.wh.core.handler.SetResponseCommandHandler;
import cn.ttplatform.wh.core.support.BufferPool;
import cn.ttplatform.wh.core.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.core.support.Future;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import cn.ttplatform.wh.core.support.MessageFactoryManager;
import cn.ttplatform.wh.core.support.RequestRecord;
import io.netty.channel.Channel;
import io.protostuff.LinkedBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/4/20 12:00
 */
@Slf4j
@Getter
@Builder
@AllArgsConstructor
public class ClientContext {

    private final Map<Channel, Set<RequestRecord<?>>> channelRequestRecordMap = new ConcurrentHashMap<>();
    private final Map<String, RequestRecord<?>> undoneMap = new ConcurrentHashMap<>();
    private final BufferPool<LinkedBuffer> pool;
    private final MessageFactoryManager factoryManager;
    private final ClientProperties properties;
    private final MessageDispatcher dispatcher;
    private final NioConnector connector;
    private final ClusterInfo clusterInfo;
    private final Object lock = new Object();

    public ClientContext(ClientProperties clientProperties) {
        properties = clientProperties;
        this.clusterInfo = new ClusterInfo(clientProperties.getMemberInfos());
        pool = new FixedSizeLinkedBufferPool(properties.getLinkedBuffPollSize());
        factoryManager = new MessageFactoryManager();
        dispatcher = new MessageDispatcher();
        initCommandHandler();
        initFactoryManager();
        connector = new NioConnector(this);
    }

    private void initCommandHandler() {
        dispatcher.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandHandler(this));
        dispatcher.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandHandler(this));
        dispatcher.register(MessageType.REQUEST_FAILED_COMMAND, new RequestFailedCommandHandler(this));
        dispatcher.register(MessageType.REDIRECT_COMMAND, new RedirectCommandHandler(this));
    }

    private void initFactoryManager() {
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(pool));
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(pool));
        factoryManager.register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandFactory(pool));
        factoryManager.register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandFactory(pool));
        factoryManager.register(MessageType.REDIRECT_COMMAND, new RedirectCommandFactory(pool));
        factoryManager.register(MessageType.REQUEST_FAILED_COMMAND, new RequestFailedCommandFactory(pool));
    }

    public void addRequestRecord(String id, RequestRecord<?> requestRecord) {
        undoneMap.put(id, requestRecord);
    }

    public RequestRecord removeRequestRecord(String id) {
        RequestRecord<?> requestRecord = undoneMap.remove(id);
//        Set<RequestRecord<?>> requestRecordSet = channelRequestRecordMap.get(requestRecord.getChannel());
//        if (requestRecordSet != null) {
//            requestRecordSet.remove(requestRecord);
//        }
        return requestRecord;
    }

    public void updateClusterInfo(List<MemberInfo> source, String leaderId) {
        clusterInfo.update(source, leaderId);
    }

    public MemberInfo getMaster() {
        return clusterInfo.getMaster();
    }

    public int getClusterSize() {
        return clusterInfo.getSize();
    }

    public void removeMaster(String masterId) {
        clusterInfo.removeInvalidMaster(masterId);
    }

    public Future<Command> send(Command cmd) {
//        Channel channel = connector.send(cmd, getMaster()).channel();
        Future<Command> future = new Future<>();
//        RequestRecord<Command> requestRecord = new RequestRecord<>(cmd, future, channel);
//        addRequestRecord(cmd.getId(), requestRecord);
//         Record the records related to the channel
//        channelRequestRecordMap.computeIfAbsent(channel, v -> Collections.synchronizedSet(new HashSet<>()));
//        channelRequestRecordMap.get(channel).add(requestRecord);
        return future;
    }

    public void redirect(String id) {
        RequestRecord<?> requestRecord = undoneMap.get(id);
        if (requestRecord == null) {
            log.warn("request record for id[{}] not found", id);
            return;
        }
        send(requestRecord.getCommand());
    }

    public void reSendAllCommandInChannel(Channel channel) {
        Set<RequestRecord<?>> requestRecords = channelRequestRecordMap.remove(channel);
        requestRecords.forEach(requestRecord -> {
            requestRecord.cancel();
            undoneMap.remove(requestRecord.getId());
            send(requestRecord.getCommand());
        });
    }
}
