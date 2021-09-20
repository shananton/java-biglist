package com.github.shananton.biglist;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GrowingBigListTests {
    private final Random rand = new Random(928591);

    @ParameterizedTest
    @MethodSource("provideLinearReadsArguments")
    void linearReads(long inRamLimit, int count) throws IOException {
        var l = new GrowingBigList(inRamLimit);
        for (long i = 0; i < count; ++i) {
            l.add(i);
            assertEquals(i, l.get((int) i));
        }
        for (int i = count - 1; i >= 0; --i) {
            assertEquals(i, l.get(i));
        }
    }

    @Test
    void randomReads() throws IOException {
        var l = new GrowingBigList(4096);
        var count = 100000;
        for (long i = 0; i < count; ++i) {
            l.add(i);
            assertEquals(i, l.get((int) i));
        }
        for (int it = 0; it < count; ++it) {
            var i = rand.nextInt(count);
            assertEquals(i, l.get(i));
        }
    }

    @Test
    void inserts() throws IOException {
        var count = 15000;
        var insCount = 100;
        var l = new GrowingBigList(4096);
        for (int i = 0; i < count; ++i) {
            l.add((long) i);
        }
        for (int it = 0; it < insCount; ++it) {
            var i = rand.nextInt(count);
            l.add(i, -1L);
        }
        long cur = 0;
        for (var x : l) {
            if (x != -1) {
                assertEquals(cur++, x);
            }
        }
        assertEquals(count, cur);
    }

    @Test
    void simpleRemoves() throws IOException {
        var l = new GrowingBigList(2);
        l.addAll(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L));
        l.remove(1);
        l.remove(3);
        l.remove(3);
        assertEquals(List.of(1L, 3L, 4L, 7L), l.stream().toList());
    }

    @Test
    void removes() throws IOException {
        var count = 15000;
        var l = new GrowingBigList(4096);
        for (int i = 0; i < count; ++i) {
            l.add((long) i);
        }
        var halfSize = l.size() / 2;
        for (int it = 0, i = 1; it < halfSize; ++it, ++i) {
            l.remove(i);
        }
        var cur = 0;
        for (var x : l) {
            assertEquals(cur, x);
            cur += 2;
        }
    }

    private static Stream<Arguments> provideLinearReadsArguments() {
        var inRamLimits = new long[]{512, 1000, 1024, 4095, 4096, 4097, 2 << 15, 2 << 20};
        var counts = new int[]{0, 1, 10, 100, 1000, 10000, 100000, 1000000};
        Stream.Builder<Arguments> sb = Stream.builder();
        for (var inRamLimit : inRamLimits) {
            for (var count : counts) {
                sb.add(Arguments.of(inRamLimit, count));
            }
        }
        sb.add(Arguments.of(2 << 20, 10000000));
        sb.add(Arguments.of(2 << 20, 100000000));
        return sb.build();
    }
}
