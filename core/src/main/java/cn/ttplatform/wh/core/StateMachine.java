package cn.ttplatform.wh.core;

import cn.ttplatform.wh.core.support.ChannelCache;
import cn.ttplatform.wh.cmd.GetResponseCommand;
import cn.ttplatform.wh.cmd.SetCommand;
import cn.ttplatform.wh.cmd.SetResponseCommand;
import cn.ttplatform.wh.core.log.entry.LogEntry;
import io.netty.channel.Channel;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

/**
 * @author Wang Hao
 * @date 2021/2/21 16:16
 */
public class StateMachine {

    private static final Schema<Data> DATA_SCHEMA = RuntimeSchema.getSchema(Data.class);
    private static final Schema<SetCommand> SET_COMMAND_SCHEMA = RuntimeSchema.getSchema(SetCommand.class);
    private Node node;
    private Data data = new Data();
    private volatile int lastApplied;

    public void register(Node node) {
        this.node = node;
    }

    public void apply(LogEntry entry) {
        if (entry.getIndex() <= lastApplied) {
            return;
        }
        SetCommand setCmd = node.getPendingSetTasks(entry.getIndex());
        if (setCmd == null) {
            setCmd = new SetCommand();
            ProtostuffIOUtil.mergeFrom(entry.getCommand(), setCmd, SET_COMMAND_SCHEMA);
        }
        data.put(setCmd.getKey(), setCmd.getValue());
        lastApplied = entry.getIndex();
        Channel channel;
        if (setCmd.getId() != null && (channel = ChannelCache.getChannel(setCmd.getId())) != null) {
            channel.writeAndFlush(SetResponseCommand.builder().id(setCmd.getId()).result(true).build())
                .addListener(future -> {
                    if (future.isSuccess()) {
                        Optional.ofNullable(node.getPendingGetTasks(entry.getIndex())).orElse(Collections.emptyList())
                            .forEach(cmd -> {
                                Channel getChannel = ChannelCache.getChannel(cmd.getId());
                                getChannel.writeAndFlush(
                                    GetResponseCommand.builder().id(cmd.getId()).value(data.get(cmd.getKey()))
                                        .build());
                            });
                    }
                });
        }
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public byte[] generateSnapshotData() {
        LinkedBuffer buffer = LinkedBuffer.allocate();
        return ProtobufIOUtil.toByteArray(data, DATA_SCHEMA, buffer);
    }

    public void applySnapshotData(byte[] snapshot, int lastIncludeIndex) {
        data = new Data();
        lastApplied = lastIncludeIndex;
        ProtostuffIOUtil.mergeFrom(snapshot, data, DATA_SCHEMA);
    }

    public static class Data extends HashMap<String, String> {

    }

}
