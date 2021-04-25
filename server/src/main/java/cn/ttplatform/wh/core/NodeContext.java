package cn.ttplatform.wh.core;

import cn.ttplatform.wh.common.Message;
import cn.ttplatform.wh.cmd.factory.GetCommandFactory;
import cn.ttplatform.wh.cmd.factory.GetResultCommandFactory;
import cn.ttplatform.wh.cmd.factory.RedirectCommandFactory;
import cn.ttplatform.wh.cmd.factory.RequestFailedCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandFactory;
import cn.ttplatform.wh.cmd.factory.SetResultCommandFactory;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.constant.MessageType;
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
import cn.ttplatform.wh.core.connector.message.handler.AppendLogEntriesMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.AppendLogEntriesResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.InstallSnapshotMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.InstallSnapshotResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.PreVoteMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.PreVoteResultMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.RequestVoteMessageHandler;
import cn.ttplatform.wh.core.connector.message.handler.RequestVoteResultMessageHandler;
import cn.ttplatform.wh.core.connector.nio.NioConnector;
import cn.ttplatform.wh.core.log.FileLog;
import cn.ttplatform.wh.core.log.Log;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogFactory;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.support.SingleThreadScheduler;
import cn.ttplatform.wh.core.support.DirectByteBufferPool;
import cn.ttplatform.wh.core.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.core.support.IndirectByteBufferPool;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import cn.ttplatform.wh.core.support.MessageFactoryManager;
import cn.ttplatform.wh.core.support.Scheduler;
import cn.ttplatform.wh.core.support.SingleThreadTaskExecutor;
import cn.ttplatform.wh.core.support.TaskExecutor;
import cn.ttplatform.wh.server.handler.ClusterChangeCommandHandler;
import cn.ttplatform.wh.server.handler.GetCommandHandler;
import cn.ttplatform.wh.server.handler.SetCommandHandler;
import cn.ttplatform.wh.support.BufferPool;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.protostuff.LinkedBuffer;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午9:39
 */
@Getter
public class NodeContext {

    private final Logger logger = LoggerFactory.getLogger(NodeContext.class);
    private Node node;
    private final File basePath;
    private final BufferPool<LinkedBuffer> linkedBufferPool;
    private final BufferPool<ByteBuffer> byteBufferPool;
    private final MessageFactoryManager factoryManager;
    private final MessageDispatcher dispatcher;
    private final Scheduler scheduler;
    private final TaskExecutor executor;
    private final NodeState nodeState;
    private final Log log;
    private final Cluster cluster;
    private final Connector connector;
    private final ServerProperties properties;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final StateMachine stateMachine;
    private final LogFactory logFactory = LogFactory.getInstance();

    public NodeContext(ServerProperties properties) {
        this.properties = properties;

        this.basePath = new File(properties.getBasePath());
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
        this.boss = new NioEventLoopGroup(properties.getBossThreads());
        this.worker = new NioEventLoopGroup(properties.getWorkerThreads());
        this.scheduler = new SingleThreadScheduler(properties);
        this.executor = new SingleThreadTaskExecutor();
        this.nodeState = new NodeState(this);
        this.cluster = new Cluster(this);
        this.connector = new NioConnector(this);
        this.log = new FileLog(this);
        this.stateMachine = new StateMachine(this);
        this.dispatcher = new MessageDispatcher();
        this.factoryManager = new MessageFactoryManager();
    }

    public void register(Node node) {
        this.node = node;
        initMessageHandler();
        initMessageFactory();
    }

    private void initMessageHandler() {
        dispatcher.register(MessageType.CLUSTER_CHANGE_COMMAND, new ClusterChangeCommandHandler(this));
        dispatcher.register(MessageType.SET_COMMAND, new SetCommandHandler(this));
        dispatcher.register(MessageType.GET_COMMAND, new GetCommandHandler(this));
        dispatcher.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageHandler(this));
        dispatcher
            .register(MessageType.APPEND_LOG_ENTRIES_RESULT, new AppendLogEntriesResultMessageHandler(this));
        dispatcher.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageHandler(this));
        dispatcher.register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageHandler(this));
        dispatcher.register(MessageType.PRE_VOTE, new PreVoteMessageHandler(this));
        dispatcher.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageHandler(this));
        dispatcher.register(MessageType.REQUEST_VOTE, new RequestVoteMessageHandler(this));
        dispatcher.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageHandler(this));
    }

    private void initMessageFactory() {
        factoryManager
            .register(MessageType.REQUEST_FAILED_COMMAND, new RequestFailedCommandFactory(linkedBufferPool));
        factoryManager.register(MessageType.REDIRECT_COMMAND, new RedirectCommandFactory(linkedBufferPool));
        factoryManager
            .register(MessageType.SET_COMMAND_RESULT, new SetResultCommandFactory(linkedBufferPool));
        factoryManager
            .register(MessageType.GET_COMMAND_RESULT, new GetResultCommandFactory(linkedBufferPool));
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandFactory(linkedBufferPool));
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandFactory(linkedBufferPool));
        factoryManager.register(MessageType.APPEND_LOG_ENTRIES, new AppendLogEntriesMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.APPEND_LOG_ENTRIES_RESULT,
            new AppendLogEntriesResultMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.INSTALL_SNAPSHOT, new InstallSnapshotMessageFactory(linkedBufferPool));
        factoryManager
            .register(MessageType.INSTALL_SNAPSHOT_RESULT, new InstallSnapshotResultMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.PRE_VOTE, new PreVoteMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.PRE_VOTE_RESULT, new PreVoteResultMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.REQUEST_VOTE, new RequestVoteMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.REQUEST_VOTE_RESULT, new RequestVoteResultMessageFactory(linkedBufferPool));
    }

    public ScheduledFuture<?> electionTimeoutTask() {
        return scheduler.scheduleElectionTimeoutTask(this::election);
    }

    public ScheduledFuture<?> logReplicationTask() {
        return scheduler.scheduleLogReplicationTask(this::doLogReplication);
    }

    private void prepareElection() {
        if (isLeader()) {
            logger.warn("current node[{}] role type is leader, ignore this process.", properties.getNodeId());
            return;
        }
        int currentTerm = node.getTerm();
        int newTerm = currentTerm + 1;
        if (isCandidate()) {
            startElection(newTerm);
        } else {
            changeToFollower(currentTerm, null, null, 1);
            PreVoteMessage preVoteMessage = PreVoteMessage.builder()
                .nodeId(node.getSelfId())
                .lastLogTerm(log.getLastLogTerm())
                .lastLogIndex(log.getLastLogIndex())
                .build();
            sendMessageToOthers(preVoteMessage);
        }
    }

    public void startElection(int term) {
        changeToCandidate(term, 1);
        RequestVoteMessage requestVoteMessage = RequestVoteMessage.builder()
            .candidateId(node.getSelfId())
            .lastLogIndex(log.getLastLogIndex())
            .lastLogTerm(log.getLastLogTerm())
            .term(term)
            .build();
        sendMessageToOthers(requestVoteMessage);
    }

    public void doLogReplication() {
        logger.debug("log replication task scheduled.");
        int currentTerm = node.getTerm();
        cluster.getAllEndpointExceptSelf().forEach(endpoint -> {
            long now = System.currentTimeMillis();
            // If the log snapshot is not being transmitted, the heartbeat detection information will be sent every time
            // the scheduled task is executed, otherwise the message will be sent only when a certain time has passed.
            // Doing so will cause a problem that the results of the slave processing log snapshot messages may not be
            // returned in time, causing the master to resend the last message because it does not receive a reply. If
            // the slave does not handle it, an unknown error will occur.
            if (!endpoint.isReplicating() || now - endpoint.getLastHeartBeat() >= properties
                .getReplicationHeartBeat()) {
                Message message = log
                    .createAppendLogEntriesMessage(node.getSelfId(), currentTerm, endpoint.getNextIndex(),
                        properties.getMaxTransferLogs());
                if (message == null) {
                    message = log.createInstallSnapshotMessage(currentTerm, endpoint.getSnapshotOffset(),
                        properties.getMaxTransferSize());
                    // start snapshot replication
                    endpoint.setReplicating(true);
                }
                sendMessage(message, endpoint);
            }
        });
    }

    private void election() {
        executor.execute(this::prepareElection);
    }

    public void sendMessageToOthers(Message message) {
        cluster.getAllEndpointExceptSelf().forEach(endpoint -> sendMessage(message, endpoint));
    }

    public void sendMessage(Message message, Endpoint endpoint) {
        logger.debug("send message to {}", endpoint);
        message.setSourceId(properties.getNodeId());
        connector.send(message, endpoint.getMetaData()).addListener(future -> {
            if (future.isSuccess()) {
                endpoint.setLastHeartBeat(System.currentTimeMillis());
                logger.debug("send message {} success", message);
            } else {
                logger.debug("send message {} failed", message);
            }
        });
    }

    public void advanceLastApplied(int newCommitIndex) {
        int lastApplied = stateMachine.getLastApplied();
        int lastIncludeIndex = log.getLastIncludeIndex();
        if (lastApplied == 0 && lastIncludeIndex > 0) {
            stateMachine.applySnapshotData(log.getSnapshotData(), lastIncludeIndex);
            lastApplied = lastIncludeIndex;
        }
        Optional.ofNullable(log.subList(lastApplied + 1, newCommitIndex + 1))
            .orElse(Collections.emptyList()).forEach(logEntry -> {
            if (isLeader() && logEntry.getType() == LogEntry.OLD_NEW) {
                // At this point, the leader has persisted the Coldnew log to the disk
                // file, and then needs to send a Cnew log and then enter the NEW phase
                pendingNewConfigLog();
            } else if (logEntry.getType() == LogEntry.NEW) {
                // At this point, Cnew Log had been committed, then enter STABLE phase, if the node
                // is not exist in new config, the then node will go offline.
                cluster.enterStablePhase();
                if (!cluster.inNewConfig(node.getSelfId())) {
                    node.stop();
                }
            }
            stateMachine.apply(logEntry);
        });
        if (log.shouldGenerateSnapshot(properties.getSnapshotGenerateThreshold())) {
            // if entry file size more than SnapshotGenerateThreshold then generate snapshot
            byte[] snapshotData = stateMachine.generateSnapshotData();
            LogEntry entry = log.getEntry(lastApplied);
            log.generateSnapshot(lastApplied, entry.getTerm(), snapshotData);
        }
    }

    public void applySnapshot(int lastIncludeIndex) {
        byte[] snapshotData = log.getSnapshotData();
        stateMachine.applySnapshotData(snapshotData, lastIncludeIndex);
    }

    public void pendingNewConfigLog() {
        log.pendingEntry(logFactory.createEntry(LogEntry.NEW, node.getTerm(), log.getNextIndex(),
            cluster.getNewConfig()));
        cluster.enterNewPhase();
    }

    public void pendingOldNewConfigLog() {
        log.pendingEntry(logFactory.createEntry(LogEntry.OLD_NEW, node.getTerm(), log.getNextIndex(),
            cluster.getOldNewConfig()));
        cluster.enterOldNewPhase();
    }

    public boolean isLeader() {
        return node.getRole() instanceof Leader;
    }

    public boolean isFollower() {
        return node.getRole() instanceof Follower;
    }

    public boolean isCandidate() {
        return node.getRole() instanceof Candidate;
    }

    public void changeToFollower(int term, String leaderId, String voteTo, int preVoteCounts) {
        Follower follower = Follower.builder().scheduledFuture(electionTimeoutTask())
            .term(term)
            .leaderId(leaderId)
            .voteTo(voteTo)
            .preVoteCounts(preVoteCounts)
            .build();
        changeToRole(follower);
    }

    public void changeToCandidate(int term, int voteCounts) {
        Candidate candidate = Candidate.builder()
            .scheduledFuture(electionTimeoutTask())
            .voteCounts(voteCounts)
            .term(term)
            .build();
        changeToRole(candidate);
    }

    public void changeToRole(Role newRole) {
        Role role = node.getRole();
        role.cancelTask();
        if (newRole.compareState(role)) {
            nodeState.setCurrentTerm(newRole.getTerm());
            nodeState.setVoteTo(newRole instanceof Follower ? ((Follower) newRole).getVoteTo() : null);
        }
        if (newRole instanceof Leader) {
            cluster.resetReplicationStates(log.getNextIndex());
        }
        node.setRole(newRole);
    }

    public void close() {
        log.close();
        nodeState.close();
        executor.close();
        scheduler.close();
    }
}