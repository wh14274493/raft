package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.Message;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.factory.GetCommandMessageFactory;
import cn.ttplatform.wh.cmd.factory.GetResponseCommandMessageFactory;
import cn.ttplatform.wh.cmd.factory.SetCommandMessageFactory;
import cn.ttplatform.wh.cmd.factory.SetResponseCommandMessageFactory;
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
import cn.ttplatform.wh.core.support.DefaultScheduler;
import cn.ttplatform.wh.core.support.DirectByteBufferPool;
import cn.ttplatform.wh.core.support.FixedSizeLinkedBufferPool;
import cn.ttplatform.wh.core.support.IndirectByteBufferPool;
import cn.ttplatform.wh.core.support.MessageDispatcher;
import cn.ttplatform.wh.core.support.MessageFactoryManager;
import cn.ttplatform.wh.core.support.Scheduler;
import cn.ttplatform.wh.core.support.SingleThreadTaskExecutor;
import cn.ttplatform.wh.core.support.TaskExecutor;
import cn.ttplatform.wh.server.handler.GetCommandHandler;
import cn.ttplatform.wh.server.handler.SetCommandHandler;
import cn.ttplatform.wh.support.BufferPool;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.protostuff.LinkedBuffer;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
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
    private final BufferPool<LinkedBuffer> linkedBufferPool;
    private final BufferPool<ByteBuffer> bufferBufferPool;
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
        Set<ClusterMember> members = initClusterMembers(properties);
        File base = new File(properties.getBasePath());
        this.linkedBufferPool = new FixedSizeLinkedBufferPool(properties.getLinkedBuffPollSize());
        if (ReadWriteFileStrategy.DIRECT.equals(properties.getReadWriteFileStrategy())) {
            logger.debug("use DirectBufferAllocator");
            this.bufferBufferPool = new DirectByteBufferPool(properties.getByteBufferPoolSize(),
                properties.getByteBufferSizeLimit());
        } else {
            logger.debug("use BufferAllocator");
            this.bufferBufferPool = new IndirectByteBufferPool(properties.getByteBufferPoolSize(),
                properties.getByteBufferSizeLimit());
        }
        this.boss = new NioEventLoopGroup(properties.getBossThreads());
        this.worker = new NioEventLoopGroup(properties.getWorkerThreads());
        this.scheduler = new DefaultScheduler(properties);
        this.executor = new SingleThreadTaskExecutor();
        this.nodeState = new NodeState(base, bufferBufferPool);
        this.cluster = new Cluster(members, properties.getNodeId());
        this.connector = new NioConnector(this);
        this.log = new FileLog(base, bufferBufferPool);
        this.stateMachine = new StateMachine(this);
        this.dispatcher = new MessageDispatcher();
        this.factoryManager = new MessageFactoryManager();
    }

    private Set<ClusterMember> initClusterMembers(ServerProperties properties) {
        String[] clusterConfig = properties.getClusterInfo().split(" ");
        return Arrays.stream(clusterConfig).map(memberInfo -> {
            String[] pieces = memberInfo.split(",");
            if (pieces.length != 3) {
                throw new IllegalArgumentException("illegal node info [" + memberInfo + "]");
            }
            String nodeId = pieces[0];
            String host = pieces[1];
            int port;
            try {
                port = Integer.parseInt(pieces[2]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("illegal port in node info [" + memberInfo + "]");
            }
            return ClusterMember.builder().memberInfo(MemberInfo.builder().nodeId(nodeId).host(host).port(port).build())
                .build();
        }).collect(Collectors.toSet());
    }

    public void register(Node node) {
        this.node = node;
        initMessageHandler();
        initMessageFactory();
    }

    private void initMessageHandler() {
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
            .register(MessageType.SET_COMMAND_RESPONSE, new SetResponseCommandMessageFactory(linkedBufferPool));
        factoryManager
            .register(MessageType.GET_COMMAND_RESPONSE, new GetResponseCommandMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.SET_COMMAND, new SetCommandMessageFactory(linkedBufferPool));
        factoryManager.register(MessageType.GET_COMMAND, new GetCommandMessageFactory(linkedBufferPool));
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
        cluster.listAllEndpointExceptSelf().forEach(member -> {
            long now = System.currentTimeMillis();
            // If the log snapshot is not being transmitted, the heartbeat detection information will be sent every time
            // the scheduled task is executed, otherwise the message will be sent only when a certain time has passed.
            // Doing so will cause a problem that the results of the slave processing log snapshot messages may not be
            // returned in time, causing the master to resend the last message because it does not receive a reply. If
            // the slave does not handle it, an unknown error will occur.
            if (!member.isReplicating() || now - member.getLastHeartBeat() >= properties.getReplicationHeartBeat()) {
                Message message = log
                    .createAppendLogEntriesMessage(node.getSelfId(), currentTerm, member.getNextIndex(),
                        properties.getMaxTransferLogs());
                if (message == null) {
                    message = log.createInstallSnapshotMessage(currentTerm, member.getSnapshotOffset(),
                        properties.getMaxTransferSize());
                    // start snapshot replication
                    member.setReplicating(true);
                }
                sendMessage(message, member);
            }
        });
    }

    private void election() {
        executor.execute(this::prepareElection);
    }

    public void applySnapshot(int lastIncludeIndex) {
        byte[] snapshotData = log.getSnapshotData();
        stateMachine.applySnapshotData(snapshotData, lastIncludeIndex);
    }

    public void sendMessageToOthers(Message message) {
        cluster.listAllEndpointExceptSelf().forEach(member -> sendMessage(message, member));
    }

    public void sendMessage(Message message, ClusterMember member) {
        logger.debug("send message to {}", member);
        message.setSourceId(properties.getNodeId());
        connector.send(message, member.getMemberInfo()).addListener(future -> {
            if (future.isSuccess()) {
                member.setLastHeartBeat(System.currentTimeMillis());
                logger.debug("send message {} success", message);
            } else {
                logger.debug("send message {} failed", message);
            }
        });
    }

    public void handleGetCommand(GetCommand command) {
        executor.execute(() -> {
            int nextIndex = log.getNextIndex();
            int key = nextIndex - 1;
            stateMachine.addGetTasks(key, command);
        });
    }

    public void pendingEntry(SetCommand command) {
        executor.execute(() -> {
            LogEntry logEntry = logFactory
                .createEntry(LogEntry.OP_TYPE, node.getTerm(), log.getNextIndex(), command.getCmd());
            command.setCmd(null);
            stateMachine.addSetCommand(logEntry.getIndex(), command);
            log.appendEntry(logEntry);
        });
    }

    public void advanceLastApplied(List<LogEntry> logEntries, int newCommitIndex) {
        int lastApplied = stateMachine.getLastApplied();
        int lastIncludeIndex = log.getLastIncludeIndex();
        if (lastApplied == 0 && lastIncludeIndex > 0) {
            stateMachine.applySnapshotData(log.getSnapshotData(), lastIncludeIndex);
            lastApplied = lastIncludeIndex;
        }
        Optional.ofNullable(log.subList(lastApplied + 1, newCommitIndex + 1))
            .orElse(Collections.emptyList()).forEach(stateMachine::apply);
        if (!logEntries.isEmpty()) {
            logEntries.forEach(stateMachine::apply);
            if (log.shouldGenerateSnapshot(properties.getSnapshotGenerateThreshold())) {
                // if entry file size more than SnapshotGenerateThreshold then generate snapshot
                byte[] snapshotData = stateMachine.generateSnapshotData();
                LogEntry entry = log.getEntry(lastApplied);
                log.generateSnapshot(lastApplied, entry.getTerm(), snapshotData);
            }
        }
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
}
