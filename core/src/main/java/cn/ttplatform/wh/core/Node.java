package cn.ttplatform.wh.core;

import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.core.common.Listener;
import cn.ttplatform.wh.core.connector.Connector;
import cn.ttplatform.wh.core.role.Candidate;
import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.role.Leader;
import cn.ttplatform.wh.core.role.Role;
import cn.ttplatform.wh.domain.cmd.GetCommand;
import cn.ttplatform.wh.domain.cmd.SetCommand;
import cn.ttplatform.wh.domain.entry.LogEntry;
import cn.ttplatform.wh.domain.entry.OpLogEntry;
import cn.ttplatform.wh.domain.message.Message;
import cn.ttplatform.wh.domain.message.PreVoteMessage;
import cn.ttplatform.wh.domain.message.RequestVoteMessage;
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
            NodeState nodeState = context.nodeState();
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
        byte[] snapshotData = context.log().getSnapshotData();
        stateMachine.applySnapshotData(snapshotData, lastIncludeIndex);
    }

    public ScheduledFuture<?> electionTimeoutTask() {
        return context.scheduler().scheduleElectionTimeoutTask(this::election);
    }

    public ScheduledFuture<?> logReplicationTask() {
        return context.scheduler().scheduleLogReplicationTask(this::logReplication);
    }

    private void election() {
        context.executor().execute(this::prepareElection);
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
                .lastLogTerm(context.log().getLastLogTerm())
                .lastLogIndex(context.log().getLastLogIndex())
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
            .lastLogIndex(context.log().getLastLogIndex())
            .lastLogTerm(context.log().getLastLogTerm())
            .term(term)
            .build();
        sendMessageToOtherActiveEndpoint(requestVoteMessage);
    }

    private void logReplication() {
        context.executor().execute(this::doLogReplication);
    }

    public void doLogReplication() {
        log.debug("start log replication.");
        int term = role.getTerm();
        ServerProperties config = context.config();
        context.cluster().listAllEndpointExceptSelf().forEach(member -> {
            long now = System.currentTimeMillis();
            if (!member.isReplicating() || now - member.getLastHeartBeat() >= 900) {
                Message message = context.log()
                    .createAppendLogEntriesMessage(selfId, term, member.getNextIndex(), config.getMaxTransferLogs());
                sendMessage(
                    message == null ? context.log()
                        .createInstallSnapshotMessage(term, member.getSnapshotOffset(), config.getMaxTransferSize())
                        : message, member);
                member.setLastHeartBeat(now);
                member.setReplicating(true);
            }
        });
    }

    public void sendMessageToOtherActiveEndpoint(Message message) {
        context.cluster().listAllEndpointExceptSelf().forEach(member -> sendMessage(message, member));
    }

    public void sendMessage(Message message, ClusterMember member) {
        log.debug("send message to {}", member);
        message.setSourceId(selfId);
        connector.send(message, member);
    }

    public void changeToRole(Role newRole) {
        role.cancelTask();
        if (newRole.compareState(role)) {
            NodeState nodeState = context.nodeState();
            nodeState.setCurrentTerm(newRole.getTerm());
            nodeState.setVoteTo(newRole instanceof Follower ? ((Follower) newRole).getVoteTo() : null);
        }
        if (newRole instanceof Leader) {
            context.cluster().resetReplicationStates(context.log().getNextIndex());
        }
        this.role = newRole;
    }

    public void handleGetCommand(GetCommand command) {
        context.executor().execute(() -> {
            int nextIndex = context.log().getNextIndex();
            int key = nextIndex - 1;
            List<GetCommand> getCommands = pendingGetCommandMap.computeIfAbsent(key, k -> new ArrayList<>());
            getCommands.add(command);
        });
    }

    public void pendingEntry(SetCommand command) {
        context.executor().execute(() -> {
            OpLogEntry logEntry = OpLogEntry.builder()
                .type(LogEntry.OP_TYPE)
                .term(role.getTerm())
                .index(context.log().getNextIndex())
                .command(command.getCmd())
                .build();
            command.setCmd(null);
            pendingSetCommandMap.put(logEntry.getIndex(), command);
            context.log().appendEntry(logEntry);
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
