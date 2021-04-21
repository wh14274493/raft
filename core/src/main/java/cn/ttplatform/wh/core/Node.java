package cn.ttplatform.wh.core;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.support.Listener;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.OpLogEntry;
import cn.ttplatform.wh.core.connector.message.Message;
import cn.ttplatform.wh.core.connector.message.PreVoteMessage;
import cn.ttplatform.wh.core.connector.message.RequestVoteMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:16
 **/
@Slf4j
@Getter
@Setter
@Builder
@AllArgsConstructor
public class Node {

    private String selfId;
    private Role role;
    private NodeContext context;
    private StateMachine stateMachine;
    private Connector connector;
    private Listener listener;
    private final Map<Integer, List<GetCommand>> pendingGetCommandMap = new HashMap<>();
    private final Map<Integer, SetCommand> pendingSetCommandMap = new HashMap<>();
    private boolean start;

    public synchronized void start() {
        if (!start) {
            NodeState nodeState = context.getNodeState();
            this.role = Follower.builder()
                .scheduledFuture(electionTimeoutTask())
                .term(nodeState.getCurrentTerm())
                .voteTo(nodeState.getVoteTo())
                .preVoteCounts(1)
                .build();
            listener.listen();
        }
        start = true;
    }

    public void applySnapshot(int lastIncludeIndex) {
        byte[] snapshotData = context.getLog().getSnapshotData();
        stateMachine.applySnapshotData(snapshotData, lastIncludeIndex);
    }

    public ScheduledFuture<?> electionTimeoutTask() {
        return context.getScheduler().scheduleElectionTimeoutTask(this::election);
    }

    public ScheduledFuture<?> logReplicationTask() {
        return context.getScheduler().scheduleLogReplicationTask(this::logReplication);
    }

    private void election() {
        context.getExecutor().execute(this::prepareElection);
    }

    private void prepareElection() {
        if (isLeader()) {
            log.warn("current node[{}] role type is leader, ignore this process.", selfId);
            return;
        }
        int currentTerm = role.getTerm();
        int newTerm = currentTerm + 1;
        if (isCandidate()) {
            startElection(newTerm);
        } else {
            Follower follower = Follower.builder()
                .scheduledFuture(electionTimeoutTask())
                .term(currentTerm)
                .preVoteCounts(1)
                .build();
            changeToRole(follower);
            PreVoteMessage preVoteMessage = PreVoteMessage.builder()
                .nodeId(selfId)
                .lastLogTerm(context.getLog().getLastLogTerm())
                .lastLogIndex(context.getLog().getLastLogIndex())
                .build();
            sendMessageToOtherActiveEndpoint(preVoteMessage);
        }
    }

    public void startElection(int term) {
        Candidate candidate = Candidate.builder().voteCounts(1)
            .scheduledFuture(electionTimeoutTask())
            .term(term)
            .build();
        changeToRole(candidate);
        RequestVoteMessage requestVoteMessage = RequestVoteMessage.builder()
            .candidateId(selfId)
            .lastLogIndex(context.getLog().getLastLogIndex())
            .lastLogTerm(context.getLog().getLastLogTerm())
            .term(term)
            .build();
        sendMessageToOtherActiveEndpoint(requestVoteMessage);
    }

    private void logReplication() {
        context.getExecutor().execute(this::doLogReplication);
    }

    public void doLogReplication() {
        log.debug("start log replication.");
        int term = role.getTerm();
        ServerProperties config = context.getProperties();
        context.getCluster().listAllEndpointExceptSelf().forEach(member -> {
            long now = System.currentTimeMillis();
            if (!member.isReplicating() || now - member.getLastHeartBeat() >= config.getReplicationHeartBeat()) {
                Message message = context.getLog()
                    .createAppendLogEntriesMessage(selfId, term, member.getNextIndex(), config.getMaxTransferLogs());
                sendMessage(
                    message == null ? context.getLog()
                        .createInstallSnapshotMessage(term, member.getSnapshotOffset(), config.getMaxTransferSize())
                        : message, member);
                member.setLastHeartBeat(now);
                member.setReplicating(true);
            }
        });
    }

    public void sendMessageToOtherActiveEndpoint(Message message) {
        context.getCluster().listAllEndpointExceptSelf().forEach(member -> sendMessage(message, member));
    }

    public void sendMessage(Message message, ClusterMember member) {
        log.debug("send message to {}", member);
        message.setSourceId(selfId);
        connector.send(message, member.getMemberInfo());
    }

    public void changeToRole(Role newRole) {
        role.cancelTask();
        if (newRole.compareState(role)) {
            NodeState nodeState = context.getNodeState();
            nodeState.setCurrentTerm(newRole.getTerm());
            nodeState.setVoteTo(newRole instanceof Follower ? ((Follower) newRole).getVoteTo() : null);
        }
        if (newRole instanceof Leader) {
            context.getCluster().resetReplicationStates(context.getLog().getNextIndex());
        }
        this.role = newRole;
    }

    public void handleGetCommand(GetCommand command) {
        context.getExecutor().execute(() -> {
            int nextIndex = context.getLog().getNextIndex();
            int key = nextIndex - 1;
            List<GetCommand> getCommands = pendingGetCommandMap.computeIfAbsent(key, k -> new ArrayList<>());
            getCommands.add(command);
        });
    }

    public void pendingEntry(SetCommand command) {
        context.getExecutor().execute(() -> {
            OpLogEntry logEntry = OpLogEntry.builder()
                .type(LogEntry.OP_TYPE)
                .term(role.getTerm())
                .index(context.getLog().getNextIndex())
                .command(command.getCmd())
                .build();
            command.setCmd(null);
            pendingSetCommandMap.put(logEntry.getIndex(), command);
            context.getLog().appendEntry(logEntry);
        });
    }

    public List<GetCommand> getPendingGetTasks(int index) {
        return pendingGetCommandMap.remove(index);
    }

    public SetCommand getPendingSetTasks(int index) {
        return pendingSetCommandMap.remove(index);
    }

    public boolean isLeader() {
        return role instanceof Leader;
    }

    public boolean isFollower() {
        return role instanceof Follower;
    }

    public boolean isCandidate() {
        return role instanceof Candidate;
    }
}
