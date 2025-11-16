package org.bitmap.intint;

import org.bitmap.core.BitSliceIndex;
import org.bitmap.core.Operation;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Objects;

/**
 * 功能：Rbm32BitSliceIndex 整数
 *         每一个切片对应一个 RoaringBitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/6/16 00:52
 */
public class Rbm32BitSliceIndex implements BitSliceIndex<Integer, Integer> {
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
    public Rbm32BitSliceIndex(Integer minValue, Integer maxValue) {
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
     * BSI 基数,即 Key 的个数
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
     * 清空所有的 Key
     */
    public void clear() {
        this.maxValue = -1;
        this.minValue = -1;
        this.ebm = new RoaringBitmap();
        this.slices = null;
        this.sliceSize = 0;
    }

    /**
     * 指定的 key 是否有对应的 value
     * @param key
     * @return
     */
    public boolean containsKey(Integer key) {
        return this.ebm.contains(key);
    }

    /**
     * 指定的 value 是否关联指定的 key
     * @param value
     * @return
     */
    public boolean containsValue(Integer value) {
        RoaringBitmap bitmap = eq(value);
        return !bitmap.isEmpty();
    }

    /**
     * 为指定的 Key 关联指定的 Value
     * @param key
     * @param value
     */
    @Override
    public void put(Integer key, Integer value) {
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
        // 为指定的 Key 设置 Value
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
    public Integer get(Integer key) {
        if (!this.containsKey(key)) {
            return -1;
        }
        return getValueInternal(key);
    }

    /**
     * 删除指定 key 的 value
     * @param key 删除指定的 key
     * @return 如果指定 key 关联的 value 不存在返回 -1，否则返回 value
     */
    @Override
    public Integer remove(Integer key) {
        // 不存在返回 -1
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
        // return this.ebm ?
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
    public Integer minValue() {
        return minValue;
    }

    /**
     * 查询指定 Key 集合中的最小值
     * @param rbm Key 集合
     * @return
     */
    @Override
    public Integer minValue(RoaringBitmap rbm) {
        if (this.isEmpty() || Objects.equals(rbm, null) || rbm.getLongCardinality() == 0) {
            return -1;
        }
        // 指定 Key 与 BSI 中 Key 的交集
        RoaringBitmap keys = RoaringBitmap.and(rbm, ebm);
        if (keys.getLongCardinality() == 0) {
            return -1;
        }
        // 查询最小值
        for (int i = this.sliceSize - 1; i >= 0; i -= 1) {
            RoaringBitmap tmp = RoaringBitmap.andNot(keys, slices[i]);
            if (!tmp.isEmpty()) {
                keys = tmp;
            }
        }
        // 可能存在多个 Key 拥有最小值
        return getValueInternal(keys.first());
    }

    @Override
    public Integer maxValue() {
        return maxValue;
    }

    /**
     * 查询指定 Key 集合中的最大值
     * @return
     */
    @Override
    public Integer maxValue(RoaringBitmap rbm) {
        if (this.isEmpty() || Objects.equals(rbm, null) || rbm.getLongCardinality() == 0) {
            return -1;
        }
        // 指定 Key 与 BSI 中 Key 的交集
        RoaringBitmap keys = RoaringBitmap.and(rbm, ebm);
        if (keys.getLongCardinality() == 0) {
            return -1;
        }
        for (int i = this.sliceSize - 1; i >= 0; i -= 1) {
            RoaringBitmap tmp = RoaringBitmap.and(keys, slices[i]);
            if (!tmp.isEmpty()) {
                keys = tmp;
            }
        }
        // 可能存在多个 Key 拥有最大值
        return getValueInternal(keys.first());
    }

    /**
     * 克隆
     * @return
     */
    @Override
    public Rbm32BitSliceIndex clone() {
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
    public RoaringBitmap eq(Integer value) {
        return oNeilRange(Operation.EQ, value);
    }

    /**
     * 范围查询 不等于 value 的 key
     * @param value 查找值
     * @return 返回由不等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap neq(Integer value) {
        return oNeilRange(Operation.NEQ, value);
    }

    /**
     * 范围查询 小于等于 value 的 key
     * @param value 查找值
     * @return 返回由小于等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap le(Integer value) {
        return oNeilRange(Operation.LE, value);
    }

    /**
     * 范围查询 小于 value 的 key
     * @param value 查找值
     * @return 返回由小于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap lt(Integer value) {
        return oNeilRange(Operation.LT, value);
    }

    /**
     * 范围查询 大于等于 value 的 key
     * @param value 查找值
     * @return 返回由大于等于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap ge(Integer value) {
        return oNeilRange(Operation.GE, value);
    }

    /**
     * 范围查询 大于 value 的 key
     * @param value 查找值
     * @return 返回由大于 value 的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap gt(Integer value) {
        return oNeilRange(Operation.GT, value);
    }

    /**
     * 范围查询 [lower, upper] 区间内的 key
     * @param lower 下限
     * @param upper 上限
     * @return 返回由[lower, upper] 区间内的 key 构成的 RoaringBitmap
     */
    @Override
    public RoaringBitmap between(Integer lower, Integer upper) {
        RoaringBitmap lowerBitmap = oNeilRange(Operation.GE, lower);
        RoaringBitmap upperBitmap = oNeilRange(Operation.LE, upper);
        RoaringBitmap resultBitmap = lowerBitmap;
        resultBitmap.and(upperBitmap);
        return resultBitmap;
    }

    /**
     * 指定 Key 的 Value 求和
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
        // minValue(4)、maxValue(4)、sliceSize(4)、runOptimized(1)、ebm(ebm.serializedSizeInBytes)、slices(4+size)
        // 与 serialize 方法一一对应
        return 4 + 4 + 4 + 1 + this.ebm.serializedSizeInBytes() + 4 + size;
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
        // 切片数组(切片个数、切片)
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

    /**
     * BSI 压缩优化
     */
    @Override
    public void runOptimize() {
        // ebm 压缩优化
        this.ebm.runOptimize();
        // 切片压缩优化
        for (RoaringBitmap slice : this.slices) {
            slice.runOptimize();
        }
        this.runOptimized = true;
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
     * 为指定的 Key 设置 Value
     * @param key
     * @param value
     */
    private void putValueInternal(Integer key, Integer value) {
        // 在 value 二进制位对应切片 Bitmap 中添加 key
        // 从低位到高位切片 Bitmap 遍历，如果 value 二进制位对应的 bit 为 1 则对应的切片 Bitmap 添加 key
        for (int i = 0; i < this.sliceSize(); i += 1) {
            if ((value & (1 << i)) > 0) {
                this.slices[i].add(key);
            } else {
                // 一个 Key 只能设置一个 Value，旧值会被新值覆盖
                this.slices[i].remove(key);
            }
        }
        this.ebm.add(key);
    }

    /**
     * 获取指定 key 的 value
     * @param key
     * @return
     */
    private Integer getValueInternal(Integer key) {
        int value = 0;
        for (int i = 0; i < this.sliceSize; i += 1) {
            if (this.slices[i].contains(key)) {
                // 通过位图反向重建原始值
                value |= (1 << i);
            }
        }
        return value;
    }

    /**
     * 删除指定 key
     * @param key
     * @return
     */
    private Integer removeValueInternal(Integer key) {
        int value = 0;
        // 从低位到高位遍历切片 Bitmap
        for (int i = 0; i < this.sliceSize; i += 1) {
            // 切片包含指定的 key 则从切片中移除该 Key 并重建原始值
            if (this.slices[i].contains(key)) {
                // 通过位移操作反向重建原始值
                value |= (1 << i);
                // 移除对应的 Key
                this.slices[i].remove(key);
            }
        }
        // 存在位图移除对应的 Key
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
    private RoaringBitmap oNeilRange(Operation operation, Integer value) {
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
