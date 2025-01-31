package org.bitmap;


import org.bitmap.core.BitSliceIndex;
import org.bitmap.intint.Rbm32BitSliceIndex;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Rbm32BitSliceIndex 测试
 */
public class Rbm32BitSliceIndexTest {
    private Map<Integer, Integer> initMap = new HashMap<>();
    private Rbm32BitSliceIndex bsi;

    @Before
    public void init() {
        bsi = new Rbm32BitSliceIndex();
        // 用户ID(user_id)、积分(score)
        initMap.put(1, 48);
        initMap.put(2, 80);
        initMap.put(3, 75);
        initMap.put(4, 19);
        initMap.put(5, 1);
        initMap.put(6, 57);
        initMap.put(7, 63);
        initMap.put(8, 22);
        initMap.put(9, 96);
        initMap.put(10, 34);
        for (int key : initMap.keySet()) {
            bsi.put(key, initMap.get(key));
        }
    }

    @Test
    public void sliceTest() {
        int sliceSize = bsi.sliceSize();
        System.out.println("SliceSize: " + sliceSize);
        assert(sliceSize == 7);
    }

    @Test
    public void getLongCardinalityTest() {
        long cardinality = bsi.getLongCardinality();
        System.out.println("Cardinality: " + cardinality);
        assert(cardinality == 10);
    }

    @Test
    public void isEmptyTest() {
        boolean isEmpty = bsi.isEmpty();
        System.out.println("IsEmpty: " + isEmpty);
        assert(isEmpty == false);
    }

    @Test
    public void putTest() {
        bsi.put(11, 12);
    }

    @Test
    public void getTest() {
        int value = bsi.get(10);
        System.out.println("Value: " + value);
        assert(value == 34);
    }

    @Test
    public void containsKeyExistTest() {
        boolean isExist = bsi.containsKey(10);
        System.out.println("isExist: " + isExist);
        assert(isExist == true);
    }

    @Test
    public void containsKeyNoExistTest() {
        boolean isExist = bsi.containsKey(11);
        System.out.println("isExist: " + isExist);
        assert(isExist == false);
    }

    @Test
    public void containsValueExistTest() {
//        boolean isExist = bsi.containsValue(57);
//        System.out.println("isExist: " + isExist);
//        assert(isExist == true);
    }

    @Test
    public void containsValueNoExistTest() {
        boolean isExist = bsi.containsKey(81);
        System.out.println("isExist: " + isExist);
        assert(isExist == false);
    }

    @Test
    public void removeTest() {
        boolean isExist = bsi.containsKey(4);
        System.out.println("isExist: " + isExist);
        assert(isExist == true);
        int removeValue = bsi.remove(4);
        System.out.println("RemoveValue: " + removeValue);
        assert(removeValue == 19);
        isExist = bsi.containsKey(4);
        System.out.println("isExist: " + isExist);
        assert(isExist == false);
    }

    @Test
    public void keysTest() {
        RoaringBitmap bitmap = bsi.keys();
        int[] keys = bitmap.stream().toArray();
        for (int key : keys) {
            System.out.println("Key: " + key);
        }
        assertEquals(bitmap.getCardinality(),10);
        assertArrayEquals(bitmap.toArray(), new int[]{1,2,3,4,5,6,7,8,9,10});
    }

    @Test
    public void maxValueTest() {
        int maxValue = bsi.maxValue();
        System.out.println("MaxValue: " + maxValue);
        assert(maxValue == 96);
    }

    @Test
    public void minValueTest() {
        int minValue = bsi.minValue();
        System.out.println("MinValue: " + minValue);
        assert(minValue == 1);
    }

    @Test
    public void eqTest() {
        RoaringBitmap eqBitmap = bsi.eq(57);
        // 6
        for (int key : eqBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(eqBitmap.getLongCardinality(),1);
        eqBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }

    @Test
    public void neqTest() {
        RoaringBitmap neqBitmap = bsi.neq(57);
        // 1,2,3,4,5,7,8,9,10
        for (int key : neqBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(neqBitmap.getLongCardinality(),9);
        neqBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }


    @Test
    public void leTest() {
        RoaringBitmap leBitmap = bsi.le(57);
        // 1,4,5,6,8,10
        for (int key : leBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(leBitmap.getLongCardinality(),6);
        leBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }

    @Test
    public void ltTest() {
        RoaringBitmap ltBitmap = bsi.lt(57);
        // 1,4,5,8,10
        for (int key : ltBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(ltBitmap.getLongCardinality(),5);
        ltBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }

    @Test
    public void geTest() {
        RoaringBitmap geBitmap = bsi.ge(57);
        // 2,3,6,7,9
        for (int key : geBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(geBitmap.getLongCardinality(),5);
        geBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }

    @Test
    public void gtTest() {
        RoaringBitmap gtBitmap = bsi.gt(57);
        // 2,3,7,9
        for (int key : gtBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(gtBitmap.getLongCardinality(),4);
        gtBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }

    @Test
    public void betweenTest() {
        RoaringBitmap betweenBitmap = bsi.between(57, 83);
        // 2,3,6,7
        for (int key : betweenBitmap.toArray()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }
        assertEquals(bsi.getLongCardinality(),10);
        assertEquals(betweenBitmap.getLongCardinality(),4);
        betweenBitmap.stream().forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
    }

    @Test
    public void sumTest() {
        RoaringBitmap rbm = RoaringBitmap.bitmapOf(3,6,8,9);
        long sum = bsi.sum(rbm);
        System.out.println("Sum: " + sum);
        assertEquals(250L, sum);
    }

    @Test
    public void cloneTest() {
        BitSliceIndex cloneBsi = bsi.clone();
        bsi.put(11, 38);

        initMap.put(11, 38);
        assertEquals(11, bsi.getLongCardinality());
        IntStream.range(1, 11).forEach(
                key -> {
                    int value = bsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
        for (int key : bsi.keys()) {
            System.out.println("key: " + key + ", value: " + bsi.get(key));
        }

        assertEquals(10, cloneBsi.getLongCardinality());
        IntStream.range(1, 10).forEach(
                key -> {
                    int value = cloneBsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
        for (int key : cloneBsi.keys()) {
            System.out.println("key: " + key + ", value: " + cloneBsi.get(key));
        }
    }

    @Test
    public void serializedSizeInBytesTest() {
        int bytes = bsi.serializedSizeInBytes();
        System.out.println("bytes: " + bytes);
        assertEquals(223, bytes);
    }

//    @Test
//    public void deserializeTest() throws IOException {
//        String base64 = "AAAAAQAAAGAAAAAHAAAAADowAAABAAAAAAAJABAAAAABAAIAAwAEAAUABgAHAAgACQAKADowAAABAAAAAAAEABAAAAADAAQABQAGAAcAOjAAAAEAAAAAAAQAEAAAAAMABAAHAAgACgA6MAAAAQAAAAAAAQAQAAAABwAIADowAAABAAAAAAACABAAAAADAAYABwA6MAAAAQAAAAAABQAQAAAAAQACAAQABgAHAAgAOjAAAAEAAAAAAAQAEAAAAAEABgAHAAkACgA6MAAAAQAAAAAAAgAQAAAAAgADAAkA";
//        byte[] bytes = Base64.getDecoder().decode(base64);
//        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
//        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//        bsi.deserialize(dataInputStream);
//        int[] keys = bsi.keys().stream().toArray();
//        for (int key : keys) {
//            System.out.println("Key: " + key);
//        }
//    }

    @Test
    public void serializeTest() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        bsi.serialize(dataOutputStream);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        String base64 = Base64.getEncoder().encodeToString(bytes);
        System.out.println(bytes.length);
        System.out.println(base64);
    }

    @Test
    public void serializeByteBufferTest() throws IOException {
        // 序列化
        ByteBuffer buffer = ByteBuffer.allocate(bsi.serializedSizeInBytes());
        bsi.serialize(buffer);

        // 反序列化
        byte[] bytes = buffer.array();
        Rbm32BitSliceIndex newBsi = new Rbm32BitSliceIndex();
        newBsi.deserialize(ByteBuffer.wrap(bytes));

        assertEquals(10, newBsi.getLongCardinality());
        IntStream.range(1, 10).forEach(
                key -> {
                    int value = newBsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
        for (int key : newBsi.keys()) {
            System.out.println("key: " + key + ", value: " + newBsi.get(key));
        }
    }

    @Test
    public void serializeStreamTest() throws IOException {
        // 序列化
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bsi.serialize(dos);
        byte[] bytes = bos.toByteArray();

        // 序列化
        Rbm32BitSliceIndex newBsi = new Rbm32BitSliceIndex();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);
        newBsi.deserialize(dis);

        assertEquals(10, newBsi.getLongCardinality());
        IntStream.range(1, 10).forEach(
                key -> {
                    int value = newBsi.get(key);
                    int targetValue = initMap.get(key);
                    assertEquals(value, targetValue);
                });
        for (int key : newBsi.keys()) {
            System.out.println("key: " + key + ", value: " + newBsi.get(key));
        }
    }
}
