package cn.ttplatform.wh.core.data.tool;

/**
 * @author Wang Hao
 * @date 2021/4/19 23:07
 */
public class ByteConvertor {

    private ByteConvertor() {
    }

    /**
     * Convert the four bytes starting from {@param index} in the {@param content} to integer type
     *
     * @param content source array
     * @param index   start index
     * @return res
     */
    public static int bytesToInt(byte[] content, int index) {
        int bit = 32 - 8;
        int res = 0;
        while (bit >= 0) {
            res |= ((content[index++] & 0xff) << bit);
            bit -= 8;
        }
        return res;
    }

    /**
     * Convert integer to byte type and fill in array, range [index-3,index]
     *
     * @param v       source num
     * @param content target array
     * @param index   end index
     */
    public static void fillIntBytes(int v, byte[] content, int index) {
        int count = 4;
        while (count > 0) {
            content[index--] = (byte) (v & 0xff);
            v >>>= 8;
            count--;
        }
    }

    /**
     * Convert the eight bytes starting from {@param index} in the {@param content} to long type
     *
     * @param content source array
     * @param index   start index
     * @return res
     */
    public static long bytesToLong(byte[] content, int index) {
        int bit = 64 - 8;
        long res = 0;
        while (bit >= 0) {
            res |= ((long) (content[index++] & 0xff) << bit);
            bit -= 8;
        }
        return res;
    }

    /**
     * Convert long to byte type and fill in array, range [index-7,index]
     *
     * @param v       source num
     * @param content target array
     * @param index   end index
     */
    public static void fillLongBytes(long v, byte[] content, int index) {
        int count = 8;
        while (count > 0) {
            content[index--] = (byte) (v & 0xff);
            v >>>= 8;
            count--;
        }
    }

}
