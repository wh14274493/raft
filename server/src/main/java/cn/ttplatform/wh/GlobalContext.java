package cn.ttplatform.wh;

import cn.ttplatform.wh.cmd.ClusterChangeCommand;
import cn.ttplatform.wh.cmd.ClusterChangeResultCommand;
import cn.ttplatform.wh.cmd.Entry;
import cn.ttplatform.wh.cmd.GetClusterInfoResultCommand;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.GetResultCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.SetResultCommand;
import cn.ttplatform.wh.cmd.factory.ClusterChangeCommandFactory;
import cn.ttplatform.wh.cmd.factory.ClusterChangeResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.EntryFactory;
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
import cn.ttplatform.wh.constant.ReadWriteFileStrategy;
import cn.ttplatform.wh.data.DataManager;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.data.log.LogFactory;
import cn.ttplatform.wh.data.snapshot.GenerateSnapshotTask;
import cn.ttplatform.wh.data.tool.DirectByteBufferPool;
import cn.ttplatform.wh.data.tool.HeapByteBufferPool;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.group.Endpoint;
import cn.ttplatform.wh.handler.ClusterChangeCommandHandler;
import cn.ttplatform.wh.handler.GetClusterInfoCommandHandler;
import cn.ttplatform.wh.handler.GetCommandHandler;
import cn.ttplatform.wh.handler.SetCommandHandler;
import cn.ttplatform.wh.message.PreVoteMessage;
import cn.ttplatform.wh.message.RequestVoteMessage;
import cn.ttplatform.wh.message.factory.AppendLogEntriesMessageFactory;
import cn.ttplatform.wh.message.factory.AppendLogEntriesResultMessageFactory;
import cn.ttplatform.wh.message.factory.InstallSnapshotMessageFactory;
import cn.ttplatform.wh.message.factory.InstallSnapshotResultMessageFactory;
import cn.ttplatform.wh.message.factory.PreVoteMessageFactory;
import cn.ttplatform.wh.message.factory.PreVoteResultMessageFactory;
import cn.ttplatform.wh.message.factory.RequestVoteMessageFactory;
import cn.ttplatform.wh.message.factory.RequestVoteResultMessageFactory;
import cn.ttplatform.wh.message.factory.SyncingMessageFactory;
import cn.ttplatform.wh.message.handler.AppendLogEntriesMessageHandler;
import cn.ttplatform.wh.message.handler.AppendLogEntriesResultMessageHandler;
import cn.ttplatform.wh.message.handler.InstallSnapshotMessageHandler;
import cn.ttplatform.wh.message.handler.InstallSnapshotResultMessageHandler;
import cn.ttplatform.wh.message.handler.PreVoteMessageHandler;
import cn.ttplatform.wh.message.handler.PreVoteResultMessageHandler;
import cn.ttplatform.wh.message.handler.RequestVoteMessageHandler;
import cn.ttplatform.wh.message.handler.RequestVoteResultMessageHandler;
import cn.ttplatform.wh.message.handler.SyncingMessageHandler;
import cn.ttplatform.wh.scheduler.Scheduler;
import cn.ttplatform.wh.scheduler.SingleThreadScheduler;
import cn.ttplatform.wh.support.ChannelPool;
import cn.ttplatform.wh.support.CommonDistributor;
import cn.ttplatform.wh.support.DistributableFactoryRegistry;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Message;
import cn.ttplatform.wh.support.NamedThreadFactory;
import cn.ttplatform.wh.support.Pool;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.protostuff.LinkedBuffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Builder;
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
@Builder
@AllArgsConstructor
public class GlobalContext {

    private final Logger logger = LoggerFactory.getLogger(GlobalContext.class);
    private final Map<Integer, List<GetCommand>> pendingGetCommandMap = new HashMap<>();
    private final Map<Integer, SetCommand> pendingSetCommandMap = new HashMap<>();
    private final ServerProperties properties;
    private final Pool<LinkedBuffer> linkedBufferPool;
    private final Pool<ByteBuffer> byteBufferPool;
    private final CommonDistributor distributor;
    private final DistributableFactoryRegistry factoryManager;
    private final ThreadPoolExecutor snapshotGenerateExecutor;
    private final ThreadPoolExecutor executor;
    private final NioEventLoopGroup boss;
    private final NioEventLoopGroup worker;
    private final StateMachine stateMachine;
    private final DataManager dataManager;
    private final EntryFactory entryFactory;
    private final ChannelPool channelPool;
    private Node node;
    private Scheduler scheduler;
    private Cluster cluster;
    private Sender connector;
    private ClusterChangeCommand clusterChangeCommand;

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
            this.byteBufferPool = new HeapByteBufferPool(properties.getByteBufferPoolSize(),
                properties.getByteBufferSizeLimit());
        }
        this.distributor = buildDistributor();
        this.factoryManager = buildFactoryManager();
        this.executor = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory("core-"));
        this.snapshotGenerateExecutor = new ThreadPoolExecutor(
            0,
            1,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            new NamedThreadFactory("snapshotTask-"),
            (r, e) -> logger.error("There is currently an executing task, reject this operation."));
        this.boss = new NioEventLoopGroup(properties.getBossThreads());
        this.worker = new NioEventLoopGroup(properties.getWorkerThreads());
        this.stateMachine = new StateMachine(this);
        this.dataManager = new DataManager(this);
        this.entryFactory = new EntryFactory(this.linkedBufferPool);
        this.channelPool = new ChannelPool();
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


    private DistributableFactoryRegistry buildFactoryManager() {
        DistributableFactoryRegistry manager = new DistributableFactoryRegistry();
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
        this.connector = new Sender(this);
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
                .lastLogTerm(dataManager.getTermOfLastLog())
                .lastLogIndex(dataManager.getIndexOfLastLog())
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
            .lastLogIndex(dataManager.getIndexOfLastLog())
            .lastLogTerm(dataManager.getTermOfLastLog())
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
                .getMinElectionTimeout()) {
                doLogReplication(endpoint, currentTerm);
            }
        });
    }

    public void doLogReplication(Endpoint endpoint, int currentTerm) {

        Message message = dataManager
            .createAppendLogEntriesMessage(node.getSelfId(), currentTerm, endpoint, properties.getMaxTransferLogs());
        if (message == null) {
            // start snapshot replication
            message = dataManager
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
        int applied = stateMachine.getApplied();
        int lastIncludeIndex = dataManager.getLastIncludeIndex();
        if (applied == 0 && lastIncludeIndex > 0) {
            logger.debug("lastApplied is 0, and there is a none empty snapshot, then apply snapshot.");
            ByteBuffer byteBuffer = dataManager.getSnapshotData();
            try {
                stateMachine.applySnapshotData(byteBuffer, lastIncludeIndex);
            } finally {
                byteBufferPool.recycle(byteBuffer);
            }
            applied = lastIncludeIndex;
        }
        applyLogs(dataManager.range(applied + 1, newCommitIndex + 1));
        if (dataManager.shouldGenerateSnapshot(properties.getSnapshotGenerateThreshold())) {
            boolean result = stateMachine.startGenerating();
            if (result) {
                snapshotGenerateExecutor.execute(new GenerateSnapshotTask(this, stateMachine.getApplied()));
            } else {
                // Perhaps the threshold for log snapshot generation should be appropriately increased
                logger.warn("There is currently an executing task, reject this operation.");
            }
        }
    }

    public void applySnapshot(int lastIncludeIndex) {
        ByteBuffer byteBuffer = dataManager.getSnapshotData();
        stateMachine.applySnapshotData(byteBuffer, lastIncludeIndex);
        byteBufferPool.recycle(byteBuffer);
    }

    public void applyLogs(List<Log> logs) {
        Optional.ofNullable(logs).orElse(Collections.emptyList()).forEach(logEntry -> {
            if (node.isLeader() && node.getMode() == RunMode.CLUSTER) {
                if (logEntry.getType() == Log.OLD_NEW) {
                    // At this point, the leader has persisted the Coldnew log to the disk
                    // file, and then needs to send a Cnew log and then enter the NEW phase
                    logger.info("OLD_NEW log had been committed");
                    cluster.enterNewPhase();
                } else if (logEntry.getType() == Log.NEW) {
                    // At this point, Cnew Log had been committed, then enter STABLE phase, if the node
                    // is not exist in new config, the then node will go offline.
                    logger.info("NEW log had been committed");
                    cluster.enterStablePhase();
                }
            }
            applyLog(logEntry);
        });
    }

    public void applyLog(Log log) {
        if (log.getIndex() <= stateMachine.getApplied()) {
            return;
        }
        int type = log.getType();
        if (type == Log.SET) {
            replySetResult(log);
        } else if (type == Log.NEW) {
            replyClusterChangeResult();
        } else {
            logger.debug("log[{}] can not be applied, skip.", log);
        }
        stateMachine.setApplied(log.getIndex());
    }

    private void replyClusterChangeResult() {
        if (clusterChangeCommand != null) {
            String id = clusterChangeCommand.getId();
            channelPool.reply(id, ClusterChangeResultCommand.builder().id(id).done(true).build());
            clusterChangeCommand = null;
        }
    }

    private void replySetResult(Log log) {
        SetCommand setCmd = pendingSetCommandMap.remove(log.getIndex());
        Entry entry;
        if (setCmd == null) {
            byte[] command = log.getCommand();
            entry = entryFactory.create(command, command.length);
            stateMachine.set(entry.getKey(), entry.getValue());
        } else {
            entry = setCmd.getEntry();
            stateMachine.set(entry.getKey(), entry.getValue());
            String requestId = setCmd.getId();
            if (requestId != null) {
                ChannelFuture channelFuture = channelPool
                    .reply(requestId, SetResultCommand.builder().id(requestId).result(true).build());
                if (channelFuture != null) {
                    channelFuture.addListener(future -> {
                        if (future.isSuccess()) {
                            replyGetResult(log.getIndex());
                        }
                    });
                }
            }
        }
    }

    private void replyGetResult(int index) {
        Optional.ofNullable(pendingGetCommandMap.remove(index))
            .orElse(Collections.emptyList())
            .forEach(cmd -> channelPool
                .reply(cmd.getId(), GetResultCommand.builder().id(cmd.getId()).value(stateMachine.get(cmd.getKey())).build()));
    }

    public void replyGetResult(GetCommand cmd) {
        channelPool.reply(cmd.getId(), GetResultCommand.builder().id(cmd.getId()).value(stateMachine.get(cmd.getKey())).build());
    }

    public void replyGetClusterInfoResult(String requestId) {
        RunMode mode = node.getMode();
        GetClusterInfoResultCommand respCommand = GetClusterInfoResultCommand.builder()
            .id(requestId)
            .leader(node.getSelfId())
            .mode(mode.toString())
            .size(stateMachine.getPairs())
            .build();
        if (mode == RunMode.CLUSTER) {
            respCommand.setPhase(cluster.getPhase().toString());
            respCommand.setNewConfig(cluster.getNewConfigMap().toString());
            respCommand.setOldConfig(cluster.getEndpointMap().toString());
        }
        channelPool.reply(requestId, respCommand);
    }

    public void addGetTasks(int index, GetCommand cmd) {
        if (index > stateMachine.getApplied()) {
            List<GetCommand> getCommands = pendingGetCommandMap.computeIfAbsent(index, k -> new ArrayList<>());
            getCommands.add(cmd);
        } else {
            channelPool
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

    public int pendingLog(int type, byte[] cmd) {
        return dataManager.pendingLog(LogFactory.createEntry(type, node.getTerm(), 0, cmd, cmd.length));
    }

    public void addPendingCommand(int index, SetCommand cmd) {
        pendingSetCommandMap.put(index, cmd);
    }

    public void close() {
        snapshotGenerateExecutor.shutdownNow();
        dataManager.close();
        if (executor != null) {
            executor.shutdown();
        }
        if (scheduler != null) {
            scheduler.close();
        }
    }

}
