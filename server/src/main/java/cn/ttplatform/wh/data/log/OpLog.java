package cn.ttplatform.wh.data.log;

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

    @Override
    public byte[] getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "OpLog{metadata=" + metadata + '}';
    }

}
