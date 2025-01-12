package org.bitmap.core;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

public interface BitSliceIndex {
    int sliceSize();
    long getLongCardinality();
    void clear();
    boolean isEmpty();
    void put(int key, int value);
    void putAll(BitSliceIndex otherBsi);
    int get(int key);
    boolean containsKey(int key);
    boolean containsValue(int value);
    int remove(int key);
    RoaringBitmap keys();
    Collection<Integer> values();
    int maxValue();
    int minValue();

    void serialize(ByteBuffer buffer) throws IOException;
    void deserialize(ByteBuffer buffer) throws IOException;

    void serialize(DataOutput output) throws IOException;
    void deserialize(DataInput in) throws IOException;
//    byte[] serialize() throws IOException;
//    void deserialize(byte[] bytes) throws IOException;
}
