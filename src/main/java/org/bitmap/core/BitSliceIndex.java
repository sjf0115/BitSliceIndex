package org.bitmap.core;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

public interface BitSliceIndex {
    // 基础操作
    int sliceSize();
    long getLongCardinality();
    boolean isEmpty();
    BitSliceIndex clone();
    // 插入操作
    void put(int key, int value);
    void putAll(BitSliceIndex otherBsi);
    // 删除操作
    void clear();
    int remove(int key);
    // 精确查询操作
    boolean containsKey(int key);
    boolean containsValue(int value);
    int get(int key);
    RoaringBitmap keys();
    Collection<Integer> values();
    // 极值查询操作
    int maxValue();
    int maxValue(RoaringBitmap rbm);
    int minValue();
    int minValue(RoaringBitmap rbm);
    // 范围查询操作
    RoaringBitmap eq(int value);
    RoaringBitmap neq(int value);
    RoaringBitmap le(int value);
    RoaringBitmap lt(int value);
    RoaringBitmap ge(int value);
    RoaringBitmap gt(int value);
    RoaringBitmap between(int lower, int upper);
    Long sum(RoaringBitmap rbm);
    // 序列化操作
    int serializedSizeInBytes();
    void serialize(ByteBuffer buffer) throws IOException;
    void deserialize(ByteBuffer buffer) throws IOException;
    void serialize(DataOutput output) throws IOException;
    void deserialize(DataInput in) throws IOException;
    byte[] serialize() throws IOException;
    void deserialize(byte[] bytes) throws IOException;
    // 优化操作
    void runOptimize();
}
