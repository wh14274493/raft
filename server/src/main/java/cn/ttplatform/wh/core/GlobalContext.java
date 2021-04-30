package cn.ttplatform.wh.core;

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
import cn.ttplatform.wh.config.ServerProperties;
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
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.core.role.RoleType;
import cn.ttplatform.wh.core.support.CommonDistributor;
import cn.ttplatform.wh.core.executor.Scheduler;
import cn.ttplatform.wh.core.executor.SingleThreadScheduler;
import cn.ttplatform.wh.core.executor.SingleThreadTaskExecutor;
import cn.ttplatform.wh.core.executor.TaskExecutor;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.DistributableFactoryManager;
import cn.ttplatform.wh.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.support.Message;
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
public class GlobalContext {

    private final Logger logger = LoggerFactory.getLogger(GlobalContext.class);
    private Node node;
    private final RoleCache roleCache;
    private final File basePath;
    private final BufferPool<LinkedBuffer> linkedBufferPool;
    private final BufferPool<ByteBuffer> byteBufferPool;
    private final CommonDistributor distributor;
    private final DistributableFactoryManager factoryManager;
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

    public GlobalContext(ServerProperties properties) {
        this.properties = properties;
        this.roleCache = new RoleCache();
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
        this.distributor = new CommonDistributor();
        this.factoryManager = new DistributableFactoryManager();
    }

    public void register(Node node) {
        this.node = node;
        initDistributor();
        initFactoryManager();
    }

    private void initDistributor() {
        distributor.register(new GetClusterInfoCommandHandler(this));
        distributor.register(new ClusterChangeCommandHandler(this));
        distributor.register(new SetCommandHandler(this));
        distributor.register(new GetCommandHandler(this));
        distributor.register(new AppendLogEntriesMessageHandler(this));
        distributor.register(new AppendLogEntriesResultMessageHandler(this));
        distributor.register(new RequestVoteMessageHandler(this));
        distributor.register(new RequestVoteResultMessageHandler(this));
        distributor.register(new PreVoteMessageHandler(this));
        distributor.register(new PreVoteResultMessageHandler(this));
        distributor.register(new InstallSnapshotMessageHandler(this));
        distributor.register(new InstallSnapshotResultMessageHandler(this));
    }


    private void initFactoryManager() {
        factoryManager.register(new SetCommandFactory(linkedBufferPool));
        factoryManager.register(new SetResultCommandFactory(linkedBufferPool));
        factoryManager.register(new GetCommandFactory(linkedBufferPool));
        factoryManager.register(new GetResultCommandFactory(linkedBufferPool));
        factoryManager.register(new RedirectCommandFactory(linkedBufferPool));
        factoryManager.register(new ClusterChangeCommandFactory(linkedBufferPool));
        factoryManager.register(new ClusterChangeResultCommandFactory(linkedBufferPool));
        factoryManager.register(new RequestFailedCommandFactory(linkedBufferPool));
        factoryManager.register(new GetClusterInfoCommandFactory(linkedBufferPool));
        factoryManager.register(new GetClusterInfoResultCommandFactory(linkedBufferPool));
        factoryManager.register(new AppendLogEntriesMessageFactory(linkedBufferPool));
        factoryManager.register(new AppendLogEntriesResultMessageFactory(linkedBufferPool));
        factoryManager.register(new RequestVoteMessageFactory(linkedBufferPool));
        factoryManager.register(new RequestVoteResultMessageFactory(linkedBufferPool));
        factoryManager.register(new PreVoteMessageFactory(linkedBufferPool));
        factoryManager.register(new PreVoteResultMessageFactory(linkedBufferPool));
        factoryManager.register(new InstallSnapshotMessageFactory(linkedBufferPool));
        factoryManager.register(new InstallSnapshotResultMessageFactory(linkedBufferPool));
    }

    public ScheduledFuture<?> electionTimeoutTask() {
        return scheduler.scheduleElectionTimeoutTask(this::election);
    }

    private void election() {
        executor.execute(this::prepareElection);
    }

    private void prepareElection() {
        if (isLeader()) {
            logger.warn("current node[{}] role type is leader, ignore this process.", properties.getNodeId());
            return;
        }
        int currentTerm = node.getTerm();
        if (isCandidate()) {
            startElection(currentTerm + 1);
        } else {
            String selfId = cluster.getSelfId();
            int oldCounts = cluster.inOldConfig(selfId) ? 1 : 0;
            int newCounts = cluster.inNewConfig(selfId) ? 1 : 0;
            changeToFollower(currentTerm, null, null, oldCounts, newCounts, 0L);
            PreVoteMessage preVoteMessage = PreVoteMessage.builder()
                .nodeId(selfId)
                .lastLogTerm(log.getLastLogTerm())
                .lastLogIndex(log.getLastLogIndex())
                .build();
            sendMessageToOthers(preVoteMessage);
        }
    }

    public void startElection(int term) {
        changeToCandidate(term, 0, 0);
        logger.debug("startElection in term[{}].", term);
        String selfId = cluster.getSelfId();
        changeToCandidate(term, cluster.inOldConfig(selfId) ? 1 : 0, cluster.inOldConfig(selfId) ? 1 : 0);
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

        Message message = log.createAppendLogEntriesMessage(node.getSelfId(), currentTerm, endpoint.getNextIndex(),
            properties.getMaxTransferLogs());
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
            if (isLeader()) {
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
            stateMachine.apply(logEntry);
        });
        if (log.shouldGenerateSnapshot(properties.getSnapshotGenerateThreshold())) {
            logger.debug("entry file size more than SnapshotGenerateThreshold then generate snapshot");
            byte[] snapshotData = stateMachine.generateSnapshotData();
            logger.debug("had generated snapshot data");
            log.generateSnapshot(lastApplied, snapshotData);
        }
    }

    public void applySnapshot(int lastIncludeIndex) {
        byte[] snapshotData = log.getSnapshotData();
        stateMachine.applySnapshotData(snapshotData, lastIncludeIndex);
    }

    public int pendingLog(int type, byte[] cmd) {
        return log.pendingEntry(LogEntryFactory.createEntry(type, node.getTerm(), 0, cmd));
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

    private int getVoteCounts(int oldVoteCounts, int newVoteCounts) {
        switch (cluster.getPhase()) {
            case NEW:
                return newVoteCounts;
            case OLD_NEW:
                return oldVoteCounts | (newVoteCounts << 16);
            default:
                return oldVoteCounts;
        }
    }

    public void changeToFollower(int term, String leaderId, String voteTo, int oldVoteCounts, int newVoteCounts,
        long lastHeartBeat) {
        int voteCounts = getVoteCounts(oldVoteCounts, newVoteCounts);
        Role role = node.getRole();
        Follower follower;
        if (role.getType() != RoleType.FOLLOWER) {
            nodeState.setCurrentTerm(term);
            nodeState.setVoteTo(voteTo);
            roleCache.recycle(role);
            follower = roleCache.getFollower();
        } else {
            if (term != role.getTerm()) {
                nodeState.setCurrentTerm(term);
                nodeState.setVoteTo(voteTo);
            }
            follower = (Follower) role;
            role.cancelTask();
        }
        follower.setTerm(term);
        follower.setScheduledFuture(electionTimeoutTask());
        follower.setLeaderId(leaderId);
        follower.setPreVoteCounts(voteCounts);
        follower.setVoteTo(voteTo);
        follower.setLastHeartBeat(lastHeartBeat);
        node.setRole(follower);
    }

    public void changeToCandidate(int term, int oldVoteCounts, int newVoteCounts) {
        int voteCounts = getVoteCounts(oldVoteCounts, newVoteCounts);
        Role role = node.getRole();
        Candidate candidate;
        if (role.getType() != RoleType.CANDIDATE) {
            nodeState.setCurrentTerm(term);
            nodeState.setVoteTo(null);
            roleCache.recycle(role);
            candidate = roleCache.getCandidate();

        } else {
            if (term != role.getTerm()) {
                nodeState.setCurrentTerm(term);
                nodeState.setVoteTo(null);
            }
            candidate = (Candidate) role;
            candidate.cancelTask();
        }
        candidate.setTerm(term);
        candidate.setScheduledFuture(electionTimeoutTask());
        candidate.setVoteCounts(voteCounts);
        node.setRole(candidate);
    }

    public void changeToLeader(int term) {
        Role role = node.getRole();
        Leader leader;
        if (role.getType() != RoleType.LEADER) {
            nodeState.setCurrentTerm(term);
            nodeState.setVoteTo(null);
            roleCache.recycle(role);
            leader = roleCache.getLeader();
        } else {
            if (term != role.getTerm()) {
                nodeState.setCurrentTerm(term);
                nodeState.setVoteTo(null);
            }
            leader = (Leader) role;
            leader.cancelTask();
        }
        leader.setTerm(term);
        leader.setScheduledFuture(logReplicationTask());
        node.setRole(leader);
        int index = pendingLog(LogEntry.NO_OP_TYPE, new byte[0]);
        cluster.resetReplicationStates(index);
        if (logger.isDebugEnabled()) {
            logger.info("become leader.");
            logger.info("reset all node replication state with nextIndex[{}]", index);
            logger.info("pending first no op log in this term, then start log replicating");
        }
    }

    public void close() {
        log.close();
        nodeState.close();
        executor.close();
        scheduler.close();
    }


    public static final class RoleCache {

        private Follower follower;
        private Leader leader;
        private Candidate candidate;

        public void recycle(Role role) {
            switch (role.getType()) {
                case LEADER:
                    recycleLeader((Leader) role);
                    break;
                case CANDIDATE:
                    recycleCandidate((Candidate) role);
                    break;
                default:
                    recycleFollower((Follower) role);
            }
        }

        public Candidate getCandidate() {
            if (candidate == null) {
                candidate = new Candidate();
            }
            return candidate;
        }

        private void recycleCandidate(Candidate candidate) {
            candidate.cancelTask();
            candidate.setTerm(0);
            candidate.setScheduledFuture(null);
            candidate.setVoteCounts(0);
            this.candidate = candidate;
        }

        public Follower getFollower() {
            if (follower == null) {
                follower = new Follower();
            }
            return follower;
        }

        private void recycleFollower(Follower follower) {
            follower.cancelTask();
            follower.setTerm(0);
            follower.setScheduledFuture(null);
            follower.setLeaderId(null);
            follower.setPreVoteCounts(0);
            follower.setVoteTo(null);
            follower.setLastHeartBeat(0L);
        }

        public Leader getLeader() {
            if (leader == null) {
                leader = new Leader();
            }
            return leader;
        }

        private void recycleLeader(Leader leader) {
            leader.cancelTask();
            leader.setTerm(0);
        }
    }

}
