package cn.ttplatform.wh.core.log.entry;

import static cn.ttplatform.wh.core.log.tool.ByteConvertor.bytesToInt;
import static cn.ttplatform.wh.core.log.tool.ByteConvertor.bytesToLong;
import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillIntBytes;
import static cn.ttplatform.wh.core.log.tool.ByteConvertor.fillLongBytes;

import cn.ttplatform.wh.core.log.snapshot.FileSnapshot;
import cn.ttplatform.wh.core.log.snapshot.SnapshotHeader;
import java.util.Arrays;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午10:13
 */
public class LogEntryFactory {

    private static class LazyHolder {

        private static final LogEntryFactory INSTANCE = new LogEntryFactory();
    }

    public static LogEntryFactory getInstance() {
        return LazyHolder.INSTANCE;
    }

    private LogEntryFactory() {
    }

    public static LogEntry createEntry(int type, int term, int index, byte[] command) {
        switch (type) {
            case LogEntry.NO_OP_TYPE:
                return NoOpLogEntry.builder()
                    .metadata(LogEntryIndex.builder().type(type).term(term).index(index).build())
                    .build();
            case LogEntry.NEW:
            case LogEntry.OLD_NEW:
            case LogEntry.SET:
                return OpLogEntry.builder()
                    .metadata(LogEntryIndex.builder().type(type).term(term).index(index).build())
                    .command(command).build();
            default:
                throw new IllegalArgumentException("unknown entry type [" + type + "]");
        }
    }

    /**
     * Convert a total of 16 bytes of data to {@link LogEntryIndex}
     *
     * @param content source array
     * @return res
     */
    public SnapshotHeader transferBytesToSnapshotHeader(byte[] content) {
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
     * Convert a byte array to {@link LogEntry}
     *
     * @param content source array
     * @return res
     */
    public LogEntry transferBytesToLogEntry(byte[] content) {
        // index[0-3]
        int index = bytesToInt(content, 0);
        // term[4-7]
        int term = bytesToInt(content, 4);
        // type[8,11]
        int type = bytesToInt(content, 8);
        // commandLength[12,15]
        // cmd[16,content.length]
        byte[] cmd = Arrays.copyOfRange(content, 16, content.length);
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
        // index[0-3]
        fillIntBytes(logEntry.getIndex(), res, 3);
        // term[4-7]
        fillIntBytes(logEntry.getTerm(), res, 7);
        // type[8,11]
        fillIntBytes(logEntry.getType(), res, 11);
        // commandLength[12,15]
        fillIntBytes(command.length, res, 15);
        // cmd[16,content.length]
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
        // index[0-3]
        fillIntBytes(logEntryIndex.getIndex(), res, 3);
        // term[4-7]
        fillIntBytes(logEntryIndex.getTerm(), res, 7);
        // term[8-11]
        fillIntBytes(logEntryIndex.getType(), res, 11);
        // term[12-19]
        fillLongBytes(logEntryIndex.getOffset(), res, 19);
        return res;
    }


}
