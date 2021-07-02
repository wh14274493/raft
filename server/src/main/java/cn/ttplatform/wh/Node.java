package cn.ttplatform.wh;

import cn.ttplatform.wh.config.RunMode;
import cn.ttplatform.wh.config.ServerProperties;
import cn.ttplatform.wh.data.log.Log;
import cn.ttplatform.wh.exception.OperateFileException;
import cn.ttplatform.wh.group.Cluster;
import cn.ttplatform.wh.role.*;
import cn.ttplatform.wh.scheduler.SingleThreadScheduler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static cn.ttplatform.wh.data.FileConstant.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * @author : wang hao
 * @date :  2020/8/15 23:16
 **/
@Getter
@Setter
@Slf4j
public class Node {

    private String selfId;
    private Role role;
    private RunMode mode;
    private final RoleCache roleCache;
    private final ServerProperties properties;
    private final GlobalContext context;
    private final NodeState nodeState;
    private boolean start;
    private final Receiver receiver;

    public Node(ServerProperties properties) {
        this.properties = properties;
        this.selfId = properties.getNodeId();
        this.mode = properties.getMode();
        this.roleCache = new RoleCache();
        this.context = new GlobalContext(this);
        this.nodeState = new NodeState();
        this.receiver = new Receiver(context);
    }

    public synchronized void start() {
        if (!start) {
            if (mode == RunMode.SINGLE) {
                startInSingleMode();
            } else {
                startInClusterMode();
            }
        }
        start = true;
    }

    public synchronized void stop() {
        if (start) {
            nodeState.close();
            receiver.stop();
            context.close();
        }
    }

    private void startInSingleMode() {
        int term = nodeState.getCurrentTerm() + 1;
        this.role = Leader.builder().term(term).build();
        int index = context.pendingLog(Log.NO_OP_TYPE, new byte[0]);
        if (context.getDataManager().advanceCommitIndex(index, term)) {
            context.advanceLastApplied(index);
        }
        this.receiver.listen();
    }

    private void startInClusterMode() {
        context.setSender(new Sender(context));
        context.setScheduler(new SingleThreadScheduler(properties));
        context.setCluster(new Cluster(context));
        this.role = Follower.builder()
                .scheduledFuture(context.electionTimeoutTask())
                .term(nodeState.getCurrentTerm())
                .voteTo(nodeState.getVoteTo())
                .preVoteCounts(1)
                .build();
        this.receiver.listen();
    }

    public int getTerm() {
        return role.getTerm();
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

    private int getVoteCounts(int oldVoteCounts, int newVoteCounts) {
        switch (context.getCluster().getPhase()) {
            case NEW:
                return newVoteCounts;
            case OLD_NEW:
                return oldVoteCounts | (newVoteCounts << 16);
            default:
                return oldVoteCounts;
        }
    }

    public void changeToFollower(int term, String leaderId, String voteTo, int oldVoteCounts, int newVoteCounts, long lastHeartBeat) {
        int voteCounts = getVoteCounts(oldVoteCounts, newVoteCounts);
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
        follower.setScheduledFuture(context.electionTimeoutTask());
        follower.setLeaderId(leaderId);
        follower.setPreVoteCounts(voteCounts);
        follower.setVoteTo(voteTo);
        follower.setLastHeartBeat(lastHeartBeat);
        this.role = follower;
    }

    public void changeToCandidate(int term, int oldVoteCounts, int newVoteCounts) {
        int voteCounts = getVoteCounts(oldVoteCounts, newVoteCounts);
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
        candidate.setScheduledFuture(context.electionTimeoutTask());
        candidate.setVoteCounts(voteCounts);
        this.role = candidate;
    }

    public void changeToLeader(int term) {
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
        leader.setScheduledFuture(context.logReplicationTask());
        this.role = leader;
        int index = context.pendingLog(Log.NO_OP_TYPE, new byte[0]);
        context.getCluster().resetReplicationStates(context.getDataManager().getLastIncludeIndex() + 1, index);
        if (log.isInfoEnabled()) {
            log.info("become leader.");
            log.info("reset all node replication state with nextIndex[{}]", index);
            log.info("pending first no op log in this term, then start log replicating");
        }
    }

    class NodeState {

        private static final int SIZE_POSITION = 0;
        private static final int TERM_POSITION = 4;
        private static final int VOTE_TO_POSITION = 8;
        private final MappedByteBuffer mappedByteBuffer;
        private final FileChannel fileChannel;
        private int spaceSize;

        public NodeState() {
            try {
                File stateFile = new File(properties.getBase(), METADATA_FILE_NAME);
                this.fileChannel = FileChannel.open(stateFile.toPath(), READ, WRITE, CREATE, DSYNC);
                this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, NODE_STATE_SPACE_POSITION, NODE_STATE_SPACE_SIZE);
                this.spaceSize = mappedByteBuffer.getInt();
                if (spaceSize < VOTE_TO_POSITION) {
                    spaceSize = VOTE_TO_POSITION;
                    updateFileSize();
                }
            } catch (IOException e) {
                throw new OperateFileException("open file channel error.", e);
            }

        }

        private void updateFileSize() {
            mappedByteBuffer.position(SIZE_POSITION);
            mappedByteBuffer.putInt(spaceSize);
        }

        /**
         * Write {@code term} into file, memory etc.
         *
         * @param term Current node's term
         */
        public void setCurrentTerm(int term) {
            mappedByteBuffer.position(TERM_POSITION);
            mappedByteBuffer.putInt(term);
        }

        /**
         * Get term from file, memory etc, when node restart
         *
         * @return currentTerm
         */
        public int getCurrentTerm() {
            mappedByteBuffer.position(TERM_POSITION);
            return mappedByteBuffer.getInt();
        }

        /**
         * if node's role is {@link Follower} then it's {@code voteTo} should be recorded， otherwise there may be a problem of
         * repeating voting.
         *
         * @param voteTo the node id that vote for
         */
        public void setVoteTo(String voteTo) {
            if (voteTo == null || "".equals(voteTo)) {
                return;
            }
            byte[] voteToBytes = voteTo.getBytes(StandardCharsets.UTF_8);
            if (voteToBytes.length > NODE_STATE_SPACE_SIZE - VOTE_TO_POSITION) {
                throw new IllegalArgumentException("the length of node id can not be greater than 120 bytes.");
            }
            mappedByteBuffer.position(VOTE_TO_POSITION);
            mappedByteBuffer.put(voteToBytes);
            spaceSize = Math.max(spaceSize, VOTE_TO_POSITION + voteToBytes.length);
            updateFileSize();
        }

        /**
         * Get {@code voteTo} from file, memory etc, when node restart
         *
         * @return the node id that vote for
         */
        public String getVoteTo() {
            if (spaceSize <= VOTE_TO_POSITION) {
                return null;
            }
            mappedByteBuffer.position(VOTE_TO_POSITION);
            byte[] voteToBytes = new byte[spaceSize - VOTE_TO_POSITION];
            mappedByteBuffer.get(voteToBytes);
            return new String(voteToBytes, StandardCharsets.UTF_8);
        }

        public void close() {
            mappedByteBuffer.force();
            try {
                if (fileChannel.isOpen()) {
                    fileChannel.close();
                }
            } catch (IOException e) {
                throw new OperateFileException("close a file channel error");
            }
        }

    }
}
