package cn.ttplatform.wh.core.data.log;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * @author Wang Hao
 * @date 2020/7/1 下午9:39
 */
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OpLog extends AbstractLog {

    private byte[] command;
    private int commandLength;

    @Override
    public byte[] getCommand() {
        return command;
    }

    @Override
    public int getCommandLength() {
        return commandLength;
    }

    @Override
    public String toString() {
        return "OpLogEntry{metadata=" + metadata + '}';
    }

}
