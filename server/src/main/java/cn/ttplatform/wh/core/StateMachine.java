package cn.ttplatform.wh.core;

import cn.ttplatform.wh.cmd.Command;
import cn.ttplatform.wh.cmd.GetCommand;
import cn.ttplatform.wh.cmd.GetResultCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.SetResultCommand;
import cn.ttplatform.wh.constant.MessageType;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import cn.ttplatform.wh.core.support.ChannelCache;
import cn.ttplatform.wh.support.BufferPool;
import cn.ttplatform.wh.support.Factory;
import io.netty.channel.Channel;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:16
 */
@Slf4j
public class StateMachine {

    private Data data = new Data();
    private final NodeContext context;
    private final DataFactory dataFactory;
    private volatile int lastApplied;
    private final Map<Integer, List<GetCommand>> pendingGetCommandMap = new HashMap<>();
    private final Map<Integer, Command> pendingSetCommandMap = new HashMap<>();

    public StateMachine(NodeContext context) {
        this.context = context;
        this.dataFactory = new DataFactory(context.getLinkedBufferPool());
    }

    public void apply(LogEntry entry) {
        if (entry.getIndex() <= lastApplied) {
            return;
        }
        if (entry.getType() == LogEntry.OP_TYPE) {
            replySetResult(entry);
        } else {

        }
    }

    private void replyClusterChangeResult(LogEntry entry) {
    }

    private void replySetResult(LogEntry entry) {
        SetCommand setCmd = (SetCommand) pendingSetCommandMap.remove(entry.getIndex());
        if (setCmd == null) {
            @SuppressWarnings("unchecked")
            Factory<SetCommand> factory = context.getFactoryManager().getFactory(MessageType.SET_COMMAND);
            setCmd = factory.create(entry.getCommand());
        }
        data.put(setCmd.getKey(), setCmd.getValue());
        lastApplied = entry.getIndex();
        Channel channel;
        if (setCmd.getId() != null && (channel = ChannelCache.getChannel(setCmd.getId())) != null) {
            channel.writeAndFlush(SetResultCommand.builder().id(setCmd.getId()).result(true).build())
                .addListener(future -> {
                    if (future.isSuccess()) {
                        replyGetResult(entry.getIndex());
                    }
                });
        }
    }

    private void replyGetResult(int index) {
        Optional.ofNullable(pendingGetCommandMap.remove(index)).orElse(Collections.emptyList())
            .forEach(cmd -> ChannelCache.getChannel(cmd.getId()).writeAndFlush(
                GetResultCommand.builder().id(cmd.getId()).value(data.get(cmd.getKey())).build()));
    }

    public void addGetTasks(int index, GetCommand cmd) {
        if (index > lastApplied) {
            List<GetCommand> getCommands = pendingGetCommandMap.computeIfAbsent(index, k -> new ArrayList<>());
            getCommands.add(cmd);
        } else {
            Channel channel = ChannelCache.getChannel(cmd.getId());
            channel.writeAndFlush(GetResultCommand.builder().id(cmd.getId()).value(data.get(cmd.getKey())).build());
        }
    }

    public void addPendingCommand(int index, Command cmd) {
        pendingSetCommandMap.put(index, cmd);
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public byte[] generateSnapshotData() {
        return dataFactory.getBytes(data);
    }

    public void applySnapshotData(byte[] snapshot, int lastIncludeIndex) {
        data = dataFactory.create(snapshot);
        lastApplied = lastIncludeIndex;
        log.info("apply snapshot that lastIncludeIndex is {}.", lastIncludeIndex);
    }

    private static class Data extends HashMap<String, String> {

    }

    private static class DataFactory implements Factory<Data> {

        private static final Schema<Data> DATA_SCHEMA = RuntimeSchema.getSchema(Data.class);
        private final BufferPool<LinkedBuffer> pool;

        public DataFactory(BufferPool<LinkedBuffer> pool) {
            this.pool = pool;
        }

        @Override
        public Data create(byte[] content) {
            Data data = new Data();
            ProtostuffIOUtil.mergeFrom(content, data, DATA_SCHEMA);
            return data;
        }

        @Override
        public byte[] getBytes(Data data) {
            LinkedBuffer buffer = pool.allocate();
            try {
                return ProtostuffIOUtil.toByteArray(data, DATA_SCHEMA, buffer);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                pool.recycle(buffer);
            }
        }
    }

}
