package org.bitmap.intint;

import org.bitmap.core.BitSliceIndex;
import org.bitmap.core.Operation;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * 功能：Rbm32BitSliceIndex 整数
 *         每一个切片对应一个 RoaringBitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/6/16 00:52
 */
public class Rbm32BitSliceIndex implements BitSliceIndex {
    private int maxValue = -1;
    private int minValue = -1;
    private int sliceSize = 0;
    private RoaringBitmap[] slices;
    private RoaringBitmap ebm;
    private Boolean runOptimized = false;

    /**
     * 构造器
     * @param minValue 最小值
     * @param maxValue 最大值
     */
    public Rbm32BitSliceIndex(int minValue, int maxValue) {
        if (minValue < 0) {
            throw new IllegalArgumentException("Value should be non-negative");
        }
        // 索引切片个数等于最大整数二进制位数，即32减去最大整数二进制填充0个数
        sliceSize = 32 - Integer.numberOfLeadingZeros(maxValue);
        this.slices = new RoaringBitmap[sliceSize];
        for (int i = 0; i < slices.length; i++) {
            this.slices[i] = new RoaringBitmap();
        }
        this.ebm = new RoaringBitmap();
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public Rbm32BitSliceIndex() {
        this(0, 0);
    }

    /**
     * 切片个数
     *      最大值二进制位数
     * @return
     */
    @Override
    public int sliceSize() {
        return sliceSize;
    }

    /**
     * 基数基数
     * @return
     */
    @Override
    public long getLongCardinality() {
        return this.ebm.getLongCardinality();
    }

    /**
     * 如果 BSI 不包含 key-value 映射，返回 true
     */
    @Override
    public boolean isEmpty() {
        return this.getLongCardinality() == 0;
    }

    /**
     * 从 BSI 中删除所有的映射，BSI 变空
     */
    public void clear() {
        this.maxValue = -1;
        this.minValue = -1;
        this.ebm = new RoaringBitmap();
        this.slices = null;
        this.sliceSize = 0;
    }

    /**
     * 指定的 key 是否关联指定的 value
     * @param key
     * @return
     */
    public boolean containsKey(int key) {
        return this.ebm.contains(key);
    }

    /**
     * 指定的 value 是否关联指定的 key
     * @param value
     * @return
     */
    public boolean containsValue(int value) {
        return false;
    }

    /**
     * 为指定的 Key 关联指定的 Value
     * @param key
     * @param value
     */
    @Override
    public void put(int key, int value) {
        // 更新最大值和最小值
        if (this.isEmpty()) {
            this.minValue = value;
            this.maxValue = value;
        } else if (this.minValue > value) {
            this.minValue = value;
        } else if (this.maxValue < value) {
            this.maxValue = value;
        }
        // 调整切片个数
        int newSliceSize = Integer.toBinaryString(value).length();
        resize(newSliceSize);
        // 为指定 Key 关联指定 Value
        putValueInternal(key, value);
    }

    @Override
    public void putAll(BitSliceIndex otherBsi) {
//        if (null == otherBsi || otherBsi.isEmpty()) {
//            return;
//        }
//
//        this.ebm.or(otherBsi.keys());
//
//        // 调整切片个数
//        if (otherBsi.sliceSize() > this.sliceSize()) {
//            resize(otherBsi.sliceSize());
//        }
//
//        for (int i = 0; i < otherBsi.sliceSize(); i++) {
//            mergeSliceInternal(otherBsi.slices[i], i);
//        }
//
//        this.minValue = minValue();
//        this.maxValue = maxValue();
    }

    /**
     * 获取指定 key 关联的 value
     * @param key
     * @return
     */
    @Override
    public int get(int key) {
        if (!this.containsKey(key)) {
            return -1;
        }
        return getValueInternal(key);
    }

    /**
     * 删除指定 key 关联的 value
     *
     * @param key 删除指定的 key
     * @return 如果指定 key 关联的 value 不存在返回 -1，否则返回 value
     */
    @Override
    public int remove(int key) {
        if (!this.containsKey(key)) {
            return -1;
        }
        return removeValueInternal(key);
    }

    /**
     * 返回所有 key 的 RoaringBitmap
     * @return
     */
    @Override
    public RoaringBitmap keys() {
        // ebm 只能通过 bsi 方法操作
        return this.ebm.clone();
    }

    @Override
    public Collection<Integer> values() {
        throw new RuntimeException("dont support keys");
    }

    /**
     * 最小值
     * @return
     */
    @Override
    public int minValue() {
        if (this.isEmpty()) {
            return 0;
        }

        RoaringBitmap minValuesId = ebm;
        for (int i = this.sliceSize - 1; i >= 0; i -= 1) {
            RoaringBitmap tmp = RoaringBitmap.andNot(minValuesId, slices[i]);
            if (!tmp.isEmpty()) {
                minValuesId = tmp;
            }
        }

        return getValueInternal(minValuesId.first());
    }

    /**
     * 最大值
     * @return
     */
    @Override
    public int maxValue() {
        if (this.isEmpty()) {
            return 0;
        }

        RoaringBitmap maxValuesId = ebm;
        for (int i = this.sliceSize - 1; i >= 0; i -= 1) {
            RoaringBitmap tmp = RoaringBitmap.and(maxValuesId, slices[i]);
            if (!tmp.isEmpty()) {
                maxValuesId = tmp;
            }
        }

        return getValueInternal(maxValuesId.first());
    }

    /**
     * 克隆
     * @return
     */
    @Override
    public BitSliceIndex clone() {
        Rbm32BitSliceIndex bitSliceIndex = new Rbm32BitSliceIndex();
        // 克隆属性
        bitSliceIndex.minValue = this.minValue;
        bitSliceIndex.maxValue = this.maxValue;
        bitSliceIndex.sliceSize = this.sliceSize;
        bitSliceIndex.runOptimized = this.runOptimized;
        bitSliceIndex.ebm = this.ebm.clone();
        // 克隆切片
        RoaringBitmap[] cloneSlices = new RoaringBitmap[this.sliceSize];
        for (int i = 0; i < cloneSlices.length; i++) {
            cloneSlices[i] = this.slices[i].clone();
        }
        bitSliceIndex.slices = cloneSlices;
        return bitSliceIndex;

    }

    /**
     * 范围查询 等于 value 的 key
     * @param value 查找值
     * @return 返回由等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap eq(int value) {
        return oNeilRange(Operation.EQ, value);
    }

    /**
     * 范围查询 不等于 value 的 key
     * @param value 查找值
     * @return 返回由不等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap neq(int value) {
        return oNeilRange(Operation.NEQ, value);
    }

    /**
     * 范围查询 小于等于 value 的 key
     * @param value 查找值
     * @return 返回由小于等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap le(int value) {
        return oNeilRange(Operation.LE, value);
    }

    /**
     * 范围查询 小于 value 的 key
     * @param value 查找值
     * @return 返回由小于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap lt(int value) {
        return oNeilRange(Operation.LT, value);
    }

    /**
     * 范围查询 大于等于 value 的 key
     * @param value 查找值
     * @return 返回由大于等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap ge(int value) {
        return oNeilRange(Operation.GE, value);
    }

    /**
     * 范围查询 大于 value 的 key
     * @param value 查找值
     * @return 返回由大于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap gt(int value) {
        return oNeilRange(Operation.GT, value);
    }

    /**
     * 范围查询 大于 value 的 key
     * @param value 查找值
     * @return 返回由大于 value 的 key 构成的 RoaringBitmap
     */
    /**
     * 范围查询 [lower, upper] 区间内的 key
     * @param lower 下限
     * @param upper 上限
     * @return 返回由[lower, upper] 区间内的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap between(int lower, int upper) {
        RoaringBitmap lowerBitmap = oNeilRange(Operation.GE, lower);
        RoaringBitmap upperBitmap = oNeilRange(Operation.LE, upper);
        RoaringBitmap resultBitmap = lowerBitmap;
        resultBitmap.and(upperBitmap);
        return resultBitmap;
    }

    /**
     * 指定 Key 集合的 RoaringBitmap 计算 Value 的 SUM 值
     * @param rbm Key 集合的 RoaringBitmap
     * @return Value 的 SUM 值
     */
    @Override
    public Long sum(RoaringBitmap rbm) {
        if (null == rbm || rbm.isEmpty()) {
            return 0L;
        }
        long sum = 0;
        for (int i = 0; i < this.sliceSize; i ++) {
            long sliceValue = 1 << i;
            sum += sliceValue * RoaringBitmap.andCardinality(this.slices[i], rbm);
        }
        return sum;
    }


    /**
     * 序列化该 BSI 所需的字节大小
     *   这是使用 serialize 方法时写入的字节数。
     * @return 以字节为单位的大小
     */
    @Override
    public int serializedSizeInBytes() {
        int size = 0;
        for (RoaringBitmap rbm : this.slices) {
            size += rbm.serializedSizeInBytes();
        }
        // minValue、maxValue、sliceSize、runOptimized、ebm、slices
        return 4 + 4 + 4 + 1 + 4 + this.ebm.serializedSizeInBytes() + size;
    }

    /**
     * 序列化
     * @param buffer
     * @throws IOException
     */
    @Override
    public void serialize(ByteBuffer buffer) throws IOException {
        // 属性
        buffer.putInt(this.minValue);
        buffer.putInt(this.maxValue);
        buffer.putInt(this.sliceSize);
        buffer.put(this.runOptimized ? (byte) 1 : (byte) 0);
        // ebm
        this.ebm.serialize(buffer);
        // 切片
        buffer.putInt(this.sliceSize);
        for (RoaringBitmap rbm : this.slices) {
            rbm.serialize(buffer);
        }
    }

    /**
     * 反序列化
     * @param buffer
     * @throws IOException
     */
    @Override
    public void deserialize(ByteBuffer buffer) throws IOException {
        this.clear();
        // 属性
        this.minValue = buffer.getInt();
        this.maxValue = buffer.getInt();
        this.sliceSize = buffer.getInt();
        this.runOptimized = buffer.get() == (byte) 1;
        // ebm
        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(buffer);
        this.ebm = ebm;
        // 切片
        buffer.position(buffer.position() + ebm.serializedSizeInBytes());
        this.sliceSize = buffer.getInt();
        RoaringBitmap[] slices = new RoaringBitmap[this.sliceSize];
        for (int i = 0; i < this.sliceSize; i++) {
            RoaringBitmap rbm = new RoaringBitmap();
            rbm.deserialize(buffer);
            slices[i] = rbm;
            buffer.position(buffer.position() + rbm.serializedSizeInBytes());
        }
        this.slices = slices;
    }

    /**
     * 序列化
     * @param output
     * @throws IOException
     */
    @Override
    public void serialize(DataOutput output) throws IOException {
        // 属性
        output.writeInt(this.minValue);
        output.writeInt(this.maxValue);
        output.writeInt(this.sliceSize);
        output.writeInt(this.runOptimized ? (byte) 1 : (byte) 0);
        // ebm
        this.ebm.serialize(output);
        // 切片
        output.writeInt(this.sliceSize);
        for (RoaringBitmap rbm : this.slices) {
            rbm.serialize(output);
        }
    }

    @Override
    public void deserialize(DataInput in) throws IOException {
        this.clear();
        // 属性
        this.minValue = in.readInt();
        this.maxValue = in.readInt();
        this.sliceSize = in.readInt();
        this.runOptimized = in.readInt() == (byte) 1;
        // ebm
        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(in);
        this.ebm = ebm;
        // 切片
        this.sliceSize = in.readInt();
        RoaringBitmap[] slices = new RoaringBitmap[this.sliceSize];
        for (int i = 0; i < this.sliceSize; i++) {
            RoaringBitmap rbm = new RoaringBitmap();
            rbm.deserialize(in);
            slices[i] = rbm;
        }
        this.slices = slices;
    }

    /**
     * 序列化为字节数组
     * @return
     * @throws IOException
     */
    @Override
    public byte[] serialize() throws IOException {
        byte[] bytes = new byte[this.serializedSizeInBytes()];
        this.serialize(ByteBuffer.wrap(bytes));
        return bytes;
    }

    /**
     * 字节数组反序列化为 BSI
     * @param bytes
     * @throws IOException
     */
    @Override
    public void deserialize(byte[] bytes) throws IOException {
        this.deserialize(ByteBuffer.wrap(bytes));
    }

    //------------------------------------------------------------------------------------------
    // 内部方法

    /**
     * 调整切片个数
     */
    private void resize(int newSliceSize) {
        if (newSliceSize <= this.sliceSize) {
            // 小于等于之前切片个数不需要调整
            return;
        }
        RoaringBitmap[] newSlices = new RoaringBitmap[newSliceSize];
        // 复制旧切片
        if (this.sliceSize != 0) {
            System.arraycopy(this.slices, 0, newSlices, 0, this.sliceSize);
        }
        // 增加新切片
        for (int i = newSliceSize - 1; i >= this.sliceSize; i--) {
            newSlices[i] = new RoaringBitmap();
            if (this.runOptimized) {
                newSlices[i].runOptimize();
            }
        }
        this.slices = newSlices;
        this.sliceSize = newSliceSize;
    }

    /**
     * 为指定的 Key 关联指定的 Value
     * @param key
     * @param value
     */
    private void putValueInternal(int key, int value) {
        // 为 value 的每个切片 bitmap 添加 x
        for (int i = 0; i < this.sliceSize(); i += 1) {
            if ((value & (1 << i)) > 0) {
                this.slices[i].add(key);
            } else {
                this.slices[i].remove(key);
            }
        }
        this.ebm.add(key);
    }

    /**
     * 获取指定 key 关联的 value
     * @param key
     * @return
     */
    private int getValueInternal(int key) {
        int value = 0;
        for (int i = 0; i < this.sliceSize; i += 1) {
            // 切片 i 包含指定的 key 则关联的 value 第 i 位为 1
            if (this.slices[i].contains(key)) {
                value |= (1 << i);
            }
        }
        return value;
    }

    /**
     * 删除指定 key 关联的 value
     * @param key
     * @return
     */
    private int removeValueInternal(int key) {
        int value = 0;
        for (int i = 0; i < this.sliceSize; i += 1) {
            // 切片 i 包含指定的 key 则关联的 value 第 i 位为 1
            if (this.slices[i].contains(key)) {
                value |= (1 << i);
                this.slices[i].remove(key);
            }
        }
        this.ebm.remove(key);
        return value;
    }

    /**
     * 合并切片
     * @param slice
     * @param sliceIndex
     */
    private void mergeSliceInternal(RoaringBitmap slice, int sliceIndex) {
        RoaringBitmap carry = RoaringBitmap.and(slice, this.slices[sliceIndex]);
        this.slices[sliceIndex].xor(slice);
        if (!carry.isEmpty()) {
            if (sliceIndex + 1 >= this.sliceSize()) {
                resize(this.sliceSize + 1);
            }
            this.mergeSliceInternal(carry, sliceIndex + 1);
        }
    }

    /**
     * oNeil 范围查询算法实现
     * @param operation
     * @param value
     * @return
     */
    private RoaringBitmap oNeilRange(Operation operation, int value) {
        RoaringBitmap GT = new RoaringBitmap();
        RoaringBitmap LT = new RoaringBitmap();
        RoaringBitmap EQ = this.ebm; // 不需要 this.ebm.clone()
        // 从高位到低位开始遍历
        for (int i = this.sliceSize - 1; i >= 0; i--) {
            // 第 i 位的值 1或者0
            int bit = (value >> i) & 1;
            if (bit == 1) {
                LT = RoaringBitmap.or(LT, RoaringBitmap.andNot(EQ, this.slices[i]));
                EQ = RoaringBitmap.and(EQ, this.slices[i]);
            } else {
                GT = RoaringBitmap.or(GT, RoaringBitmap.and(EQ, this.slices[i]));
                EQ = RoaringBitmap.andNot(EQ, this.slices[i]);
            }
        }

        switch (operation) {
            case EQ:
                return EQ;
            case NEQ:
                return RoaringBitmap.andNot(this.ebm, EQ);
            case GT:
                return GT;
            case LT:
                return LT;
            case LE:
                return RoaringBitmap.or(LT, EQ);
            case GE:
                return RoaringBitmap.or(GT, EQ);
            default:
                throw new IllegalArgumentException("");
        }
    }
}
