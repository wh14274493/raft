package cn.ttplatform.wh.core.log;

import static cn.ttplatform.wh.core.support.ByteConvertor.bytesToInt;
import static cn.ttplatform.wh.core.support.ByteConvertor.bytesToLong;
import static cn.ttplatform.wh.core.support.ByteConvertor.fillIntBytes;
import static cn.ttplatform.wh.core.support.ByteConvertor.fillLongBytes;

import cn.ttplatform.wh.core.log.entry.FileLogEntry;
import cn.ttplatform.wh.core.log.entry.FileLogEntryIndex;
import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.core.log.snapshot.SnapshotHeader;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.log.entry.LogEntryIndex;
import cn.ttplatform.wh.core.log.entry.NoOpLogEntry;
import cn.ttplatform.wh.core.log.entry.OpLogEntry;
import java.util.Arrays;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class LogFactory {

    private static class LazyHolder {

        private static final LogFactory INSTANCE = new LogFactory();
    }

    public static LogFactory getInstance() {
        return LazyHolder.INSTANCE;
    }

    private LogFactory() {
    }

    public LogEntry createEntry(int type, int term, int index, byte[] command) {
        switch (type) {
            case LogEntry.NO_OP_TYPE:
                return NoOpLogEntry.builder().type(LogEntry.NO_OP_TYPE).term(term).index(index).build();
            case LogEntry.OP_TYPE:
                return OpLogEntry.builder().type(LogEntry.OP_TYPE).term(term).index(index).command(command).build();
            default:
                throw new IllegalArgumentException("unknown entry type [" + type + "]");
        }
    }

    /**
     * Convert a total of 16 bytes of data from the {@param start} position in the array to {@link LogEntryIndex}
     *
     * @param content source array
     * @param start   start index
     * @return res
     */
    public SnapshotHeader transferBytesToSnapshotHeader(byte[] content, int start) {
        long size = bytesToLong(content, 0);
        int lastIncludeIndex = bytesToInt(content, 8);
        int lastIncludeTerm = bytesToInt(content, 12);
        return SnapshotHeader.builder().size(size).lastIncludeIndex(lastIncludeIndex).lastIncludeTerm(lastIncludeTerm)
            .build();
    }

    /**
     * Convert a total of 20 bytes of data from the {@param start} position in the array to {@link LogEntryIndex}
     *
     * @param content source array
     * @param start   start index
     * @return res
     */
    public LogEntryIndex transferBytesToLogEntryIndex(byte[] content, int start) {
        int index = bytesToInt(content, start);
        int term = bytesToInt(content, start + 4);
        int type = bytesToInt(content, start + 8);
        long offset = bytesToLong(content, start + 12);
        return LogEntryIndex.builder().type(type).term(term).offset(offset).index(index)
            .build();
    }

    /**
     * Convert a byte array from the {@param start} position in the array to {@link LogEntry}
     *
     * @param content source array
     * @param start   start index
     * @return res
     */
    public LogEntry transferBytesToLogEntry(byte[] content, int start) {
        int index = bytesToInt(content, start);
        int term = bytesToInt(content, start + 4);
        int type = bytesToInt(content, start + 8);
        byte[] cmd = Arrays.copyOfRange(content, start+16, content.length);
        return createEntry(type, term, index, cmd);
    }

    /**
     * Convert a {@link SnapshotHeader} object to byte array
     *
     * @param snapshotHeader source object
     * @return res
     */
    public byte[] transferSnapshotHeaderToBytes(SnapshotHeader snapshotHeader) {
        byte[] res = new byte[FileSnapshot.HEADER_LENGTH];
        fillLongBytes(snapshotHeader.getSize(), res, 7);
        fillIntBytes(snapshotHeader.getLastIncludeIndex(), res, 11);
        fillIntBytes(snapshotHeader.getLastIncludeTerm(), res, 15);
        return res;
    }

    /**
     * Convert a {@link LogEntry} object to byte array
     *
     * @param logEntry source object
     * @return res
     */
    public byte[] transferLogEntryToBytes(LogEntry logEntry) {
        byte[] command = logEntry.getCommand();
        byte[] res = new byte[FileLogEntry.LOG_ENTRY_HEADER_SIZE + command.length];
        fillIntBytes(logEntry.getIndex(), res, 3);
        fillIntBytes(logEntry.getTerm(), res, 7);
        fillIntBytes(logEntry.getType(), res, 11);
        fillIntBytes(command.length, res, 15);
        int index = 16;
        for (byte b : command) {
            res[index++] = b;
        }
        return res;
    }

    /**
     * Convert a {@link LogEntryIndex} object to byte array
     *
     * @param logEntryIndex source object
     * @return res
     */
    public byte[] transferLogEntryIndexToBytes(LogEntryIndex logEntryIndex) {
        byte[] res = new byte[FileLogEntryIndex.ITEM_LENGTH];
        fillIntBytes(logEntryIndex.getIndex(), res, 3);
        fillIntBytes(logEntryIndex.getTerm(), res, 7);
        fillIntBytes(logEntryIndex.getType(), res, 11);
        fillLongBytes(logEntryIndex.getOffset(), res, 19);
        return res;
    }


}
