package com.example.gsb.map;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LockFreeOpenAddressingMapTest {

    // 1) 插入与查找：冲突时走探测序列，更新不重复
    @Test
    void insertAndGetWithProbing() {
        LockFreeOpenAddressingMap<Integer, String> map = new LockFreeOpenAddressingMap<>(4, k -> k * 31 + 7);
        for (int i = 0; i < 100; i++) {
            map.put(i, "v" + i);
        }
        for (int i = 0; i < 100; i++) {
            assertThat(map.get(i)).isEqualTo("v" + i);
        }
        assertThat(map.size()).isEqualTo(100);
        assertThat(map.get(1000)).isNull();
        assertThat(map.containsKey(42)).isTrue();

        map.put(42, "updated");
        assertThat(map.get(42)).isEqualTo("updated");
        assertThat(map.size()).isEqualTo(100);
        assertThat(map.keys()).hasSize(100);
    }

    // 2) 删除与墓碑：删除后读不到，且不破坏同探测链上后续键的查找
    @Test
    void deleteLeavesTombstoneAndKeepsProbeChain() {
        LockFreeOpenAddressingMap<Integer, String> map =
                new LockFreeOpenAddressingMap<>(8, k -> k & 0x7fffffff);
        // 构造冲突：4、12、20 在容量 8 时落到同一起始槽（线性探测链）
        for (int k : new int[]{4, 12, 20, 28}) {
            map.put(k, "v" + k);
        }
        assertThat(map.remove(12)).isEqualTo("v12");
        assertThat(map.remove(12)).isNull();
        assertThat(map.get(12)).isNull();
        // 墓碑后面的键仍可找到 —— 若删除直接置空，这些查找会被截断
        assertThat(map.get(4)).isEqualTo("v4");
        assertThat(map.get(20)).isEqualTo("v20");
        assertThat(map.get(28)).isEqualTo("v28");
        assertThat(map.size()).isEqualTo(3);
    }

    @Test
    void deleteAllAndReinsert() {
        LockFreeOpenAddressingMap<String, String> map = new LockFreeOpenAddressingMap<>(4, String::hashCode);
        map.put("a", "1");
        map.put("b", "2");
        map.remove("a");
        map.remove("b");
        assertThat(map.size()).isZero();
        map.put("a", "3");
        assertThat(map.get("a")).isEqualTo("3");
    }

    // 3) 扩容与迁移：超过负载阈值后容量翻倍，数据不丢
    @Test
    void resizeMigratesAllEntries() {
        LockFreeOpenAddressingMap<Integer, Integer> map = new LockFreeOpenAddressingMap<>(4, k -> k);
        for (int i = 0; i < 500; i++) {
            map.put(i, i);
        }
        assertThat(map.capacity()).isGreaterThanOrEqualTo(512);
        for (int i = 0; i < 500; i++) {
            assertThat(map.get(i)).isEqualTo(i);
        }
        assertThat(map.size()).isEqualTo(500);
        assertThat(map.loadFactor()).isLessThanOrEqualTo(LockFreeOpenAddressingMap.RESIZE_LOAD_FACTOR + 1e-9 + 1.0 / map.capacity());
    }

    // 4) 并发插入不丢数据：总数校验
    @Test
    void concurrentInsertsNoLoss() throws Exception {
        int threads = 8;
        int perThread = 5_000;
        LockFreeOpenAddressingMap<Integer, Integer> map =
                new LockFreeOpenAddressingMap<>(16, k -> k);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            int base = t * perThread;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        map.put(base + i, base + i);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(2, TimeUnit.MINUTES)).isTrue();
        pool.shutdown();

        int expected = threads * perThread;
        assertThat(map.size())
                .as("并发插入后元素总数必须等于 %d（丢数据即 CAS 竞争处理错误）", expected)
                .isEqualTo(expected);
        for (int i = 0; i < expected; i++) {
            assertThat(map.get(i)).as("key %d 必须可读", i).isEqualTo(i);
        }
    }

    // 4b) 迁移期间并发读写混合：读永远不能读到错误/丢失的数据
    @Test
    void concurrentReadWriteDuringMigration() throws Exception {
        LockFreeOpenAddressingMap<Integer, Integer> map =
                new LockFreeOpenAddressingMap<>(8, k -> k);
        int writers = 4, readers = 4;
        int count = 20_000;
        ExecutorService writerPool = Executors.newFixedThreadPool(writers);
        ExecutorService readerPool = Executors.newFixedThreadPool(readers);
        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
        AtomicInteger errors = new AtomicInteger();
        for (int r = 0; r < readers; r++) {
            readerPool.submit(() -> {
                while (running.get()) {
                    int key = (int) (Math.random() * (long) writers * count);
                    Integer v = map.get(key);
                    if (v != null && !v.equals(key)) {
                        errors.incrementAndGet();
                    }
                }
            });
        }
        for (int w = 0; w < writers; w++) {
            int base = w;
            writerPool.submit(() -> {
                for (int i = 0; i < count; i++) {
                    int key = i * writers + base;
                    map.put(key, key);
                }
            });
        }
        writerPool.shutdown();
        assertThat(writerPool.awaitTermination(2, TimeUnit.MINUTES)).isTrue();
        running.set(false);
        readerPool.shutdown();
        assertThat(readerPool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
        assertThat(errors).hasValue(0);
        assertThat(map.size()).isEqualTo(writers * count);
    }

    // 4c) 同键并发 put / remove 线性化
    @Test
    void concurrentPutSameKey() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int round = 0; round < 20; round++) {
            LockFreeOpenAddressingMap<Integer, Integer> map =
                    new LockFreeOpenAddressingMap<>(4, k -> k);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        map.put(7, 7);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(1, TimeUnit.MINUTES)).isTrue();
            assertThat(map.size()).as("同键并发插入只能有一个活副本").isEqualTo(1);
        }
        pool.shutdown();
    }

    // 5) 墓碑整理：大量删除后触发 compaction，墓碑被清除且不丢活数据
    @Test
    void compactionRemovesTombstonesWithoutLoss() {
        LockFreeOpenAddressingMap<Integer, Integer> map =
                new LockFreeOpenAddressingMap<>(16, k -> k);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        int capacityBefore = map.capacity();
        long compBefore = map.compactionCount();

        // 删除 60 个键：墓碑占已占用槽位（活+墓碑）比例达到 60%，触发整理
        for (int i = 0; i < 60; i++) {
            map.remove(i);
        }

        assertThat(map.compactionCount())
                .as("墓碑比例超过阈值后必须至少整理一次")
                .isGreaterThan(compBefore);
        assertThat(map.size()).isEqualTo(40);
        for (int i = 60; i < 100; i++) {
            assertThat(map.get(i)).as("整理不能丢活数据 key=%d", i).isEqualTo(i);
        }
        for (int i = 0; i < 60; i++) {
            assertThat(map.get(i)).as("整理后不能读到已删除项 key=%d", i).isNull();
        }
        // 同容量整理：不应触发扩容
        assertThat(map.capacity()).isEqualTo(capacityBefore);
    }

    // 5b) 整理期间并发增删读：最终一致、不丢数据、不复活已删除键
    @Test
    void concurrentOpsDuringCompaction() throws Exception {
        LockFreeOpenAddressingMap<Integer, Integer> map =
                new LockFreeOpenAddressingMap<>(16, k -> k);
        int range = 20_000;
        for (int i = 0; i < range; i++) {
            map.put(i, i);
        }
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ConcurrentMap<Integer, Boolean> deleted = new ConcurrentHashMap<>();
        AtomicInteger errors = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int base = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = base; i < range; i += threads) {
                        if ((i & 1) == 0) {
                            map.remove(i);
                            deleted.put(i, Boolean.TRUE);
                        }
                    }
                    // 持续读校验：已删除键不可复活，存在键值必须正确
                    for (int round = 0; round < 3; round++) {
                        for (int i = base; i < range; i += threads) {
                            Integer v = map.get(i);
                            if ((i & 1) == 0) {
                                if (deleted.containsKey(i) && v != null) {
                                    errors.incrementAndGet();
                                }
                            } else if (v == null || !v.equals(i)) {
                                errors.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(2, TimeUnit.MINUTES)).isTrue();
        pool.shutdown();

        assertThat(errors.get()).as("整理期间读到已删除项或错误值").isZero();
        assertThat(map.size()).isEqualTo(range / 2);
        for (int i = 1; i < range; i += 2) {
            assertThat(map.get(i)).isEqualTo(i);
        }
        for (int i = 0; i < range; i += 2) {
            assertThat(map.get(i)).isNull();
        }
    }

    // 6) null 键支持
    @Test
    void nullKeySupported() {
        LockFreeOpenAddressingMap<String, String> map = new LockFreeOpenAddressingMap<>();
        map.put(null, "n");
        map.put("x", "y");
        assertThat(map.get(null)).isEqualTo("n");
        assertThat(map.get("x")).isEqualTo("y");
        map.put(null, "n2");
        assertThat(map.get(null)).isEqualTo("n2");
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.remove(null)).isEqualTo("n2");
        assertThat(map.get(null)).isNull();
        assertThat(map.size()).isEqualTo(1);
    }

    // 6b) 自定义哈希质量直接影响探测长度：均匀哈希远优于常数哈希
    @Test
    void hashQualityAffectsProbeLength() {
        LockFreeOpenAddressingMap<Integer, Integer> good =
                new LockFreeOpenAddressingMap<>(256, k -> k);
        LockFreeOpenAddressingMap<Integer, Integer> bad =
                new LockFreeOpenAddressingMap<>(256, k -> 42);
        int n = 100;
        for (int i = 0; i < n; i++) {
            good.put(i, i);
            bad.put(i, i);
        }
        // 预热探测统计
        for (int i = 0; i < n; i++) {
            good.get(i);
            bad.get(i);
        }
        assertThat(good.averageProbeLength()).isLessThan(2.0);
        assertThat(bad.averageProbeLength()).isGreaterThan(good.averageProbeLength() * 5);
        System.out.printf("均匀哈希平均探测长度=%.2f, 常数哈希=%.2f%n",
                good.averageProbeLength(), bad.averageProbeLength());
    }

    @Test
    void rejectsNullHashFunction() {
        assertThatThrownBy(() -> new LockFreeOpenAddressingMap<Integer, Integer>(16, null))
                .isInstanceOf(NullPointerException.class);
    }

    // 7) 统计指标
    @Test
    void statsAreReported() {
        LockFreeOpenAddressingMap<Integer, Integer> map =
                new LockFreeOpenAddressingMap<>(16, k -> k);
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 10; i++) {
            map.get(i);
        }
        LockFreeOpenAddressingMap.MapStats stats = map.stats();
        assertThat(stats.size()).isEqualTo(10);
        assertThat(stats.capacity()).isEqualTo(16);
        assertThat(stats.loadFactor()).isCloseTo(0.625, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(stats.averageProbeLength()).isGreaterThanOrEqualTo(1.0);
        assertThat(stats.compactions()).isZero();

        map.remove(0);
        LockFreeOpenAddressingMap.MapStats after = map.stats();
        assertThat(after.size()).isEqualTo(9);
    }

    @Test
    void keysSnapshotReflectsContent() {
        LockFreeOpenAddressingMap<String, Integer> map = new LockFreeOpenAddressingMap<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            map.put("k" + i, i);
            expected.add("k" + i);
        }
        Set<String> keys = new HashSet<>(map.keys());
        assertThat(keys).containsExactlyInAnyOrderElementsOf(expected);
    }
}
