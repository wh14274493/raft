package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.cmd.ClusterChangeResultCommand;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.GetResultCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.SetResultCommand;
import cn.ttplatform.wh.cmd.factory.ClusterChangeCommandFactory;
import cn.ttplatform.wh.cmd.factory.ClusterChangeResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetClusterInfoCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetClusterInfoResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.RedirectCommandFactory;
import cn.ttplatform.wh.cmd.factory.RequestFailedCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetResultCommandFactory;
import cn.ttplatform.wh.config.RunMode;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.DistributableType;
import cn.ttplatform.wh.constant.ReadWriteFileStrategy;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import cn.ttplatform.wh.core.connector.message.factory.AppendLogEntriesMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.AppendLogEntriesResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.InstallSnapshotMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.InstallSnapshotResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.PreVoteMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.PreVoteResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.RequestVoteMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.RequestVoteResultMessageFactory;
import cn.ttplatform.wh.core.connector.message.factory.SyncingMessageFactory;
import cn.ttplatform.wh.core.connector.message.handler.AppendLogEntriesMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.AppendLogEntriesResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.InstallSnapshotMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.InstallSnapshotResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.PreVoteMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.PreVoteResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.RequestVoteMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.RequestVoteResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.SyncingMessageHandler;
import cn.ttplatform.wh.core.connector.nio.NioConnector;
import cn.ttplatform.wh.core.executor.Scheduler;
import cn.ttplatform.wh.core.executor.SingleThreadScheduler;
import cn.ttplatform.wh.core.executor.SingleThreadTaskExecutor;
import cn.ttplatform.wh.core.executor.TaskExecutor;
import cn.ttplatform.wh.core.group.Cluster;
import cn.ttplatform.wh.core.group.Endpoint;
import cn.ttplatform.wh.core.listener.handler.ClusterChangeCommandHandler;
import cn.ttplatform.wh.core.listener.handler.GetClusterInfoCommandHandler;
import cn.ttplatform.wh.core.listener.handler.GetCommandHandler;
import cn.ttplatform.wh.core.listener.handler.SetCommandHandler;
import cn.ttplatform.wh.core.log.FileLog;
import cn.ttplatform.wh.core.log.Log;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryFactory;
import cn.ttplatform.wh.core.log.tool.DirectByteBufferPool;
import cn.ttplatform.wh.core.log.tool.IndirectByteBufferPool;
import cn.ttplatform.wh.core.support.ChannelPool;
import cn.ttplatform.wh.core.support.CommonDistributor;
import cn.ttplatform.wh.support.ByteArrayPool;
import cn.ttplatform.wh.support.DistributableFactory;
import cn.ttplatform.wh.support.DistributableFactoryManager;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.support.Pool;
import cn.ttplatform.wh.support.PooledByteBuffer;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.protostuff.LinkedBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
@Setter
@Getter
public class GlobalContext {

    private final Logger logger = LoggerFactory.getLogger(GlobalContext.class);
    private final Map<Integer, List<GetCommand>> pendingGetCommandMap = new HashMap<>();
    private final Map<Integer, SetCommand> pendingSetCommandMap = new HashMap<>();
    private ClusterChangeCommand clusterChangeCommand;
    private Node node;
    private final ServerProperties properties;
    private final Pool<LinkedBuffer> linkedBufferPool;
    private final Pool<PooledByteBuffer> byteBufferPool;
    private final Pool<byte[]> byteArrayPool;
    private final CommonDistributor distributor;
    private final DistributableFactoryManager factoryManager;
    private final TaskExecutor executor;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final StateMachine stateMachine;
    private final Log log;
    private Scheduler scheduler;
    private Cluster cluster;
    private Connector connector;

    public GlobalContext(Node node) {
        this.node = node;
        this.properties = node.getProperties();
        this.linkedBufferPool = new FixedSizeLinkedBufferPool(properties.getLinkedBuffPollSize());
        if (ReadWriteFileStrategy.DIRECT.equals(properties.getReadWriteFileStrategy())) {
            logger.debug("use DirectBufferAllocator");
            this.byteBufferPool = new DirectByteBufferPool(properties.getByteBufferPoolSize(),
                properties.getByteBufferSizeLimit());
        } else {
            logger.debug("use BufferAllocator");
            this.byteBufferPool = new IndirectByteBufferPool(properties.getByteBufferPoolSize(),
                properties.getByteBufferSizeLimit());
        }
        this.byteArrayPool = new ByteArrayPool(properties.getByteArrayPoolSize(), properties.getByteArraySizeLimit());
        this.distributor = buildDistributor();
        this.factoryManager = buildFactoryManager();
        this.executor = new SingleThreadTaskExecutor();
        this.boss = new NioEventLoopGroup(properties.getBossThreads());
        this.worker = new NioEventLoopGroup(properties.getWorkerThreads());
        this.stateMachine = new StateMachine(this);
        this.log = new FileLog(this);
    }

    private CommonDistributor buildDistributor() {
        CommonDistributor commonDistributor = new CommonDistributor();
        commonDistributor.register(new GetClusterInfoCommandHandler(this));
        commonDistributor.register(new ClusterChangeCommandHandler(this));
        commonDistributor.register(new SetCommandHandler(this));
        commonDistributor.register(new GetCommandHandler(this));
        commonDistributor.register(new AppendLogEntriesMessageHandler(this));
        commonDistributor.register(new AppendLogEntriesResultMessageHandler(this));
        commonDistributor.register(new RequestVoteMessageHandler(this));
        commonDistributor.register(new RequestVoteResultMessageHandler(this));
        commonDistributor.register(new PreVoteMessageHandler(this));
        commonDistributor.register(new PreVoteResultMessageHandler(this));
        commonDistributor.register(new InstallSnapshotMessageHandler(this));
        commonDistributor.register(new InstallSnapshotResultMessageHandler(this));
        commonDistributor.register(new SyncingMessageHandler(this));
        return commonDistributor;
    }


    private DistributableFactoryManager buildFactoryManager() {
        DistributableFactoryManager manager = new DistributableFactoryManager();
        manager.register(new SetCommandFactory(linkedBufferPool));
        manager.register(new SetResultCommandFactory(linkedBufferPool));
        manager.register(new GetCommandFactory(linkedBufferPool));
        manager.register(new GetResultCommandFactory(linkedBufferPool));
        manager.register(new RedirectCommandFactory(linkedBufferPool));
        manager.register(new ClusterChangeCommandFactory(linkedBufferPool));
        manager.register(new ClusterChangeResultCommandFactory(linkedBufferPool));
        manager.register(new RequestFailedCommandFactory(linkedBufferPool));
        manager.register(new GetClusterInfoCommandFactory(linkedBufferPool));
        manager.register(new GetClusterInfoResultCommandFactory(linkedBufferPool));
        manager.register(new AppendLogEntriesMessageFactory(linkedBufferPool));
        manager.register(new AppendLogEntriesResultMessageFactory(linkedBufferPool));
        manager.register(new RequestVoteMessageFactory(linkedBufferPool));
        manager.register(new RequestVoteResultMessageFactory(linkedBufferPool));
        manager.register(new PreVoteMessageFactory(linkedBufferPool));
        manager.register(new PreVoteResultMessageFactory(linkedBufferPool));
        manager.register(new InstallSnapshotMessageFactory(linkedBufferPool));
        manager.register(new InstallSnapshotResultMessageFactory(linkedBufferPool));
        manager.register(new SyncingMessageFactory(linkedBufferPool));
        return manager;
    }

    public void enterClusterMode() {
        node.setMode(RunMode.CLUSTER);
        this.cluster = new Cluster(this);
        this.scheduler = new SingleThreadScheduler(properties);
        this.connector = new NioConnector(this);
    }

    public ScheduledFuture<?> electionTimeoutTask() {
        return scheduler.scheduleElectionTimeoutTask(this::election);
    }

    private void election() {
        executor.execute(this::prepareElection);
    }

    private void prepareElection() {
        if (node.isLeader()) {
            logger.warn("current node[{}] role type is leader, ignore this process.", properties.getNodeId());
            return;
        }
        int currentTerm = node.getTerm();
        if (node.isCandidate()) {
            startElection(currentTerm + 1);
        } else {
            String selfId = cluster.getSelfId();
            int oldCounts = cluster.inOldConfig(selfId) ? 1 : 0;
            int newCounts = cluster.inNewConfig(selfId) ? 1 : 0;
            node.changeToFollower(currentTerm, null, null, oldCounts, newCounts, 0L);
            PreVoteMessage preVoteMessage = PreVoteMessage.builder()
                .nodeId(selfId)
                .lastLogTerm(log.getLastLogTerm())
                .lastLogIndex(log.getLastLogIndex())
                .build();
            sendMessageToOthers(preVoteMessage);
        }
    }

    public void startElection(int term) {
        logger.debug("startElection in term[{}].", term);
        String selfId = cluster.getSelfId();
        node.changeToCandidate(term, cluster.inOldConfig(selfId) ? 1 : 0, cluster.inOldConfig(selfId) ? 1 : 0);
        RequestVoteMessage requestVoteMessage = RequestVoteMessage.builder()
            .candidateId(selfId)
            .lastLogIndex(log.getLastLogIndex())
            .lastLogTerm(log.getLastLogTerm())
            .term(term)
            .build();
        sendMessageToOthers(requestVoteMessage);
    }

    public ScheduledFuture<?> logReplicationTask() {
        return scheduler.scheduleLogReplicationTask(this::logReplication);
    }

    public void logReplication() {
        executor.execute(this::doLogReplication);
    }

    public void doLogReplication() {
        int currentTerm = node.getTerm();
        cluster.getAllEndpointExceptSelf().forEach(endpoint -> {
            // If the log is not being transmitted, the heartbeat detection information will be sent every time
            // the scheduled task is executed, otherwise the message will be sent only when a certain time has passed.
            // Doing so will cause a problem that the results of the slave processing log snapshot messages may not be
            // returned in time, causing the master to resend the last message because it does not receive a reply. If
            // the slave does not handle it, an unknown error will occur.
            if (!endpoint.isReplicating() || System.currentTimeMillis() - endpoint.getLastHeartBeat() >= properties
                .getRetryTimeout()) {
                doLogReplication(endpoint, currentTerm);
            }
        });
    }

    public void doLogReplication(Endpoint endpoint, int currentTerm) {

        Message message = log
            .createAppendLogEntriesMessage(node.getSelfId(), currentTerm, endpoint, properties.getMaxTransferLogs());
        if (message == null) {
            // start snapshot replication
            message = log
                .createInstallSnapshotMessage(currentTerm, endpoint.getSnapshotOffset(), properties.getMaxTransferSize());
        }
        sendMessage(message, endpoint);
        endpoint.setReplicating(true);
        endpoint.setLastHeartBeat(System.currentTimeMillis());
    }

    public void sendMessageToOthers(Message message) {
        cluster.getAllEndpointExceptSelf().forEach(endpoint -> sendMessage(message, endpoint));
    }

    public void sendMessage(Message message, Endpoint endpoint) {
        message.setSourceId(properties.getNodeId());
        connector.send(message, endpoint.getMetaData());
    }

    public void sendMessage(Message message, String nodeId) {
        if (message == null) {
            return;
        }
        message.setSourceId(properties.getNodeId());
        connector.send(message, nodeId);
    }

    public void advanceLastApplied(int newCommitIndex) {
        int lastApplied = stateMachine.getLastApplied();
        int lastIncludeIndex = log.getLastIncludeIndex();
        if (lastApplied == 0 && lastIncludeIndex > 0) {
            logger.debug("lastApplied is 0, and there is a none empty snapshot, then apply snapshot.");
            stateMachine.applySnapshotData(log.getSnapshotData(), lastIncludeIndex);
            lastApplied = lastIncludeIndex;
        }
        Optional.ofNullable(log.subList(lastApplied + 1, newCommitIndex + 1))
            .orElse(Collections.emptyList()).forEach(logEntry -> {
            if (node.isLeader() && node.getMode() == RunMode.CLUSTER) {
                if (logEntry.getType() == LogEntry.OLD_NEW) {
                    // At this point, the leader has persisted the Coldnew log to the disk
                    // file, and then needs to send a Cnew log and then enter the NEW phase
                    logger.info("OLD_NEW log had been committed");
                    cluster.enterNewPhase();
                } else if (logEntry.getType() == LogEntry.NEW) {
                    // At this point, Cnew Log had been committed, then enter STABLE phase, if the node
                    // is not exist in new config, the then node will go offline.
                    logger.info("NEW log had been committed");
                    cluster.enterStablePhase();
                }
            }
            apply(logEntry);
        });
        if (log.shouldGenerateSnapshot(properties.getSnapshotGenerateThreshold())) {
            logger.debug("entry file size more than SnapshotGenerateThreshold then generate snapshot");
            byte[] snapshotData = stateMachine.generateSnapshotData();
            logger.debug("had generated snapshot data");
            log.generateSnapshot(lastApplied, snapshotData);
        }
    }

    public void applySnapshot(int lastIncludeIndex) {
        PooledByteBuffer snapshotData = log.getSnapshotData();
        stateMachine.applySnapshotData(snapshotData, lastIncludeIndex);
    }

    public int pendingLog(int type, byte[] cmd) {
        return log.pendingEntry(LogEntryFactory.createEntry(type, node.getTerm(), 0, cmd, cmd.length));
    }

    public void apply(LogEntry entry) {
        if (entry.getIndex() <= stateMachine.getLastApplied()) {
            return;
        }
        int type = entry.getType();
        if (type == LogEntry.SET) {
            replySetResult(entry);
        } else if (type == LogEntry.NEW) {
            replyClusterChangeResult();
        } else {
            logger.debug("log[{}] can not apply.", entry);
        }
        stateMachine.setLastApplied(entry.getIndex());
    }

    private void replyClusterChangeResult() {
        if (clusterChangeCommand != null) {
            String id = clusterChangeCommand.getId();
            ChannelPool.reply(id, ClusterChangeResultCommand.builder().id(id).done(true).build());
            clusterChangeCommand = null;
        }
    }

    private void replySetResult(LogEntry entry) {
        SetCommand setCmd = pendingSetCommandMap.remove(entry.getIndex());
        if (setCmd == null) {
            DistributableFactory factory = factoryManager.getFactory(DistributableType.SET_COMMAND);
            byte[] command = entry.getCommand();
            setCmd = (SetCommand) factory.create(command, entry.getCommandLength());
        }
        stateMachine.set(setCmd.getKey(), setCmd.getValue());
        String requestId = setCmd.getId();
        if (requestId != null) {
            ChannelFuture channelFuture = ChannelPool
                .reply(requestId, SetResultCommand.builder().id(requestId).result(true).build());
            if (channelFuture != null) {
                channelFuture.addListener(future -> {
                    if (future.isSuccess()) {
                        replyGetResult(entry.getIndex());
                    }
                });
            }
        }
    }

    private void replyGetResult(int index) {
        Optional.ofNullable(pendingGetCommandMap.remove(index))
            .orElse(Collections.emptyList())
            .forEach(cmd -> ChannelPool
                .reply(cmd.getId(), GetResultCommand.builder().id(cmd.getId()).value(stateMachine.get(cmd.getKey())).build()));
    }

    public void replyGetResult(GetCommand cmd) {
        ChannelPool.reply(cmd.getId(), GetResultCommand.builder().id(cmd.getId()).value(stateMachine.get(cmd.getKey())).build());
    }

    public void addGetTasks(int index, GetCommand cmd) {
        if (index > stateMachine.getLastApplied()) {
            List<GetCommand> getCommands = pendingGetCommandMap.computeIfAbsent(index, k -> new ArrayList<>());
            getCommands.add(cmd);
        } else {
            ChannelPool
                .reply(cmd.getId(), GetResultCommand.builder().id(cmd.getId()).value(stateMachine.get(cmd.getKey())).build());
        }
    }

    public boolean setCurrentClusterChangeTask(ClusterChangeCommand command) {
        if (clusterChangeCommand != null) {
            logger.info("clusterChangeCommand is not null");
            return false;
        }
        clusterChangeCommand = command;
        logger.info("update clusterChangeCommand success.");
        return true;
    }

    public void removeClusterChangeTask() {
        clusterChangeCommand = null;
    }

    public void addPendingCommand(int index, SetCommand cmd) {
        pendingSetCommandMap.put(index, cmd);
    }

    public void close() {
        log.close();
        executor.close();
        scheduler.close();
    }


}
