package cn.ttplatform.lc.rpc.message;

/**
 * @author Wang Hao
 * @date 2020/10/2 下午4:29
 */
public class AbstractMessage implements Message{

    int type;

    @Override
    public int getType() {
        return type;
    }
}
