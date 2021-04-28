package cn.ttplatform.wh.core;

import cn.ttplatform.wh.core.role.Follower;
import cn.ttplatform.wh.core.log.tool.ByteBufferWriter;
import cn.ttplatform.wh.core.log.tool.ReadableAndWriteableFile;
import java.io.File;
import java.nio.charset.Charset;

/**
 * @author Wang Hao
 * @date 2020/6/30 下午10:40
 */
public class NodeState {

    /**
     * Node state will be stored in {@code nodeStoreFile}
     */
    public static final String NODE_STATE_FILE_NAME = "node.state";

    private final ReadableAndWriteableFile file;

    public NodeState(NodeContext context) {
        this.file = new ByteBufferWriter(new File(context.getBasePath(), NODE_STATE_FILE_NAME),
            context.getByteBufferPool());
    }

    /**
     * Write {@code term} into file, memory etc.
     *
     * @param term Current node's term
     */
    public void setCurrentTerm(int term) {
        file.writeIntAt(0L, term);
    }

    /**
     * Get term from file, memory etc, when node restart
     *
     * @return currentTerm
     */
    public int getCurrentTerm() {
        if (file.isEmpty()) {
            return 0;
        }
        return file.readIntAt(0L);
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
        byte[] content = voteTo.getBytes(Charset.defaultCharset());
        file.writeBytesAt(Integer.BYTES, content);
    }

    /**
     * Get {@code voteTo} from file, memory etc, when node restart
     *
     * @return the node id that vote for
     */
    public String getVoteTo() {
        if (file.isEmpty()) {
            return null;
        }
        long offset = Integer.BYTES;
        int size = (int) (file.size() - offset);
        if (size == 0) {
            return "";
        }
        return new String(file.readBytesAt(offset, size), Charset.defaultCharset());
    }

    public void close() {
        file.close();
    }

}
