package cn.ttplatform.wh.data.tool;

import java.nio.ByteBuffer;

/**
 * @author Wang Hao
 * @date 2021/6/10 13:41
 */
public class Bits {

    private Bits() {
    }

    public static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (b3 << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | (b0 & 0xff);
    }

    public static long makeLong(byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1, byte b0) {
        return ((((long) b7) << 56) |
            (((long) b6 & 0xff) << 48) |
            (((long) b5 & 0xff) << 40) |
            (((long) b4 & 0xff) << 32) |
            (((long) b3 & 0xff) << 24) |
            (((long) b2 & 0xff) << 16) |
            (((long) b1 & 0xff) << 8) |
            (((long) b0 & 0xff)));
    }

    public static byte int3(int x) {
        return (byte) (x >> 24);
    }

    public static byte int2(int x) {
        return (byte) (x >> 16);
    }

    public static byte int1(int x) {
        return (byte) (x >> 8);
    }

    public static byte int0(int x) {
        return (byte) x;
    }

    public static byte long7(long x) {
        return (byte) (x >> 56);
    }

    public static byte long6(long x) {
        return (byte) (x >> 48);
    }

    public static byte long5(long x) {
        return (byte) (x >> 40);
    }

    public static byte long4(long x) {
        return (byte) (x >> 32);
    }

    public static byte long3(long x) {
        return (byte) (x >> 24);
    }

    public static byte long2(long x) {
        return (byte) (x >> 16);
    }

    public static byte long1(long x) {
        return (byte) (x >> 8);
    }

    public static byte long0(long x) {
        return (byte) x;
    }

    public static void putInt(int v, ByteBuffer byteBuffer) {
        byteBuffer.put(int3(v));
        byteBuffer.put(int2(v));
        byteBuffer.put(int1(v));
        byteBuffer.put(int0(v));
    }

    public static void putLong(long v, ByteBuffer byteBuffer) {
        byteBuffer.put(long7(v));
        byteBuffer.put(long6(v));
        byteBuffer.put(long5(v));
        byteBuffer.put(long4(v));
        byteBuffer.put(long3(v));
        byteBuffer.put(long2(v));
        byteBuffer.put(long1(v));
        byteBuffer.put(long0(v));
    }

    public static int getInt(ByteBuffer byteBuffer) {
        return makeInt(byteBuffer.get(), byteBuffer.get(), byteBuffer.get(), byteBuffer.get());
    }

    public static long getLong(ByteBuffer byteBuffer) {
        return makeLong(byteBuffer.get(), byteBuffer.get(), byteBuffer.get(), byteBuffer.get(), byteBuffer.get(),
            byteBuffer.get(), byteBuffer.get(), byteBuffer.get());
    }

}
