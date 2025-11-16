package org.bitmap.core;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

public interface BitSliceIndex<K,V> {
    // 基础操作
    int sliceSize();
    long getLongCardinality();
    boolean isEmpty();
    BitSliceIndex clone();
    // 插入操作
    void put(K key, V value);
    void putAll(BitSliceIndex otherBsi);
    // 删除操作
    void clear();
    V remove(K key);
    // 精确查询操作
    boolean containsKey(K key);
    boolean containsValue(V value);
    V get(K key);
    RoaringBitmap keys();
    Collection<Integer> values();
    // 极值查询操作
    V maxValue();
    V maxValue(RoaringBitmap rbm);
    V minValue();
    V minValue(RoaringBitmap rbm);
    // 范围查询操作
    RoaringBitmap eq(V value);
    RoaringBitmap neq(V value);
    RoaringBitmap le(V value);
    RoaringBitmap lt(V value);
    RoaringBitmap ge(V value);
    RoaringBitmap gt(V value);
    RoaringBitmap between(V lower, V upper);
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
