package cn.ttplatform.lc.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author : Wang Hao
 * @date :  2020/8/16 17:05
 **/
@Getter
@Setter
@Builder
@AllArgsConstructor
public class InstallSnapshotResultMessage implements Message {

    private int term;
    private boolean success;

}
