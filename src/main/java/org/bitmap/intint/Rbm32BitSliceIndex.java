package org.bitmap.intint;

import org.bitmap.core.BitSliceIndex;
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
        return this.ebm;
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

    @Override
    public void serialize(ByteBuffer buffer) throws IOException {
        buffer.putInt(this.minValue);
        buffer.putInt(this.maxValue);
        buffer.putInt(this.sliceSize);
        buffer.put(this.runOptimized ? (byte) 1 : (byte) 0);
        this.ebm.serialize(buffer);
        for (RoaringBitmap rb : this.slices) {
            rb.serialize(buffer);
        }
    }

    @Override
    public void deserialize(ByteBuffer buffer) throws IOException {
        this.clear();

        this.minValue = buffer.getInt();
        this.maxValue = buffer.getInt();
        this.sliceSize = buffer.getInt();
        this.runOptimized = buffer.get() == (byte) 1;

        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(buffer);
        this.ebm = ebm;

        buffer.position(buffer.position() + ebm.serializedSizeInBytes());
        int bitDepth = buffer.getInt();
        RoaringBitmap[] slices = new RoaringBitmap[bitDepth];
        for (int i = 0; i < bitDepth; i++) {
            RoaringBitmap rb = new RoaringBitmap();
            rb.deserialize(buffer);
            slices[i] = rb;
            buffer.position(buffer.position() + rb.serializedSizeInBytes());
        }
        this.slices = slices;
    }

    @Override
    public void serialize(DataOutput output) throws IOException {
        output.writeInt(this.minValue);
        output.writeInt(this.maxValue);
        output.writeInt(this.sliceSize);
        output.writeInt(this.runOptimized ? (byte) 1 : (byte) 0);

        this.ebm.serialize(output);
        for (RoaringBitmap rb : this.slices) {
            rb.serialize(output);
        }
    }

    @Override
    public void deserialize(DataInput in) throws IOException {
        this.clear();

        this.minValue = in.readInt();
        this.maxValue = in.readInt();
        this.sliceSize = in.readInt();
        this.runOptimized = in.readInt() == (byte) 1;

        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(in);
        this.ebm = ebm;

        RoaringBitmap[] slices = new RoaringBitmap[this.sliceSize];
        for (int i = 0; i < this.sliceSize; i++) {
            RoaringBitmap rb = new RoaringBitmap();
            rb.deserialize(in);
            slices[i] = rb;
        }
        this.slices = slices;
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
}
