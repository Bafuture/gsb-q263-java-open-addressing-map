package com.example.gsb.map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.within;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LockFreeOpenAddressingMapTest {

  // ----------------------------------------------------------------
  // 1. 插入 / 查找
  // ----------------------------------------------------------------

  @Test
  void putAndGetBasic() {
    LockFreeOpenAddressingMap<String, Integer> map = new LockFreeOpenAddressingMap<>();
    assertThat(map.put("a", 1)).isNull();
    assertThat(map.put("b", 2)).isNull();
    assertThat(map.put("a", 10)).isEqualTo(1); // 覆盖返回旧值

    assertThat(map.get("a")).isEqualTo(10);
    assertThat(map.get("b")).isEqualTo(2);
    assertThat(map.get("missing")).isNull();
    assertThat(map.containsKey("a")).isTrue();
    assertThat(map.containsKey("missing")).isFalse();
    assertThat(map.size()).isEqualTo(2);
  }

  @Test
  void nullKeyIsSupported() {
    LockFreeOpenAddressingMap<String, Integer> map = new LockFreeOpenAddressingMap<>();
    assertThat(map.put(null, 42)).isNull();
    assertThat(map.get(null)).isEqualTo(42);
    assertThat(map.put(null, 43)).isEqualTo(42);
    assertThat(map.get(null)).isEqualTo(43);
    assertThat(map.remove(null)).isEqualTo(43);
    assertThat(map.get(null)).isNull();
    assertThat(map.size()).isZero();
  }

  @Test
  void nullValueIsRejected() {
    LockFreeOpenAddressingMap<String, Integer> map = new LockFreeOpenAddressingMap<>();
    assertThatNullPointerException().isThrownBy(() -> map.put("k", null));
  }

  @Test
  void customHashFunctionIsUsed() {
    // 自定义哈希：所有键哈希相同 -> 全部冲突，验证正确性不依赖哈希质量。
    LockFreeOpenAddressingMap<Integer, String> map =
        new LockFreeOpenAddressingMap<>(64, 0.7, k -> 7);
    for (int i = 0; i < 20; i++) {
      map.put(i, "v" + i);
    }
    for (int i = 0; i < 20; i++) {
      assertThat(map.get(i)).isEqualTo("v" + i);
    }
    assertThat(map.size()).isEqualTo(20);
    // 全部冲突时平均探测长度应明显大于 1（哈希质量差 -> 探测变长）。
    assertThat(map.averageProbeLength()).isGreaterThan(1.5);
  }

  // ----------------------------------------------------------------
  // 2. 删除与墓碑
  // ----------------------------------------------------------------

  @Test
  void deleteKeepsProbeChainIntact() {
    // 常量哈希让 k1..k5 全部落在同一探测链上。
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, k -> 0);
    for (int i = 0; i < 5; i++) {
      map.put(i, i * 100);
    }
    // 删除链中间的键：若直接置空会截断探测链，后面的键将丢失。
    assertThat(map.remove(2)).isEqualTo(200);
    assertThat(map.get(2)).isNull();
    assertThat(map.get(0)).isEqualTo(0);
    assertThat(map.get(1)).isEqualTo(100);
    assertThat(map.get(3)).isEqualTo(300); // 墓碑之后的键仍可找到
    assertThat(map.get(4)).isEqualTo(400);
    assertThat(map.size()).isEqualTo(4);
    assertThat(map.tombstoneCount()).isEqualTo(1);

    // 墓碑复活：重新 put 已删除的键。
    assertThat(map.put(2, 222)).isNull();
    assertThat(map.get(2)).isEqualTo(222);
    assertThat(map.size()).isEqualTo(5);
    assertThat(map.tombstoneCount()).isZero();

    // 重复删除返回 null。
    assertThat(map.remove(2)).isEqualTo(222);
    assertThat(map.remove(2)).isNull();
  }

  // ----------------------------------------------------------------
  // 3. 扩容与迁移
  // ----------------------------------------------------------------

  @Test
  void resizeMigratesAllEntries() {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    int n = 10_000;
    for (int i = 0; i < n; i++) {
      map.put(i, i * 2);
    }
    assertThat(map.size()).isEqualTo(n);
    assertThat(map.capacity()).isGreaterThanOrEqualTo(16384);
    assertThat(map.resizeCount()).isGreaterThan(0);
    for (int i = 0; i < n; i++) {
      assertThat(map.get(i)).isEqualTo(i * 2);
    }
    // 迁移完成后旧键更新仍正常。
    assertThat(map.put(5, 55)).isEqualTo(10);
    assertThat(map.get(5)).isEqualTo(55);
  }

  @Test
  void readsAndWritesWorkDuringMigration() throws Exception {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    int n = 50_000;
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    Thread writer = new Thread(() -> {
      try {
        for (int i = 0; i < n; i++) {
          map.put(i, i);
        }
      } catch (Throwable t) {
        errors.add(t);
      }
    });
    // 读线程在迁移期间持续读：只允许读到 null（还没插入）或正确值，绝不许读到错值。
    List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 4; r++) {
      Thread reader = new Thread(() -> {
        try {
          for (int i = 0; i < n; i++) {
            Integer v = map.get(i);
            if (v != null && v != i) {
              errors.add(new AssertionError("wrong value for " + i + ": " + v));
            }
          }
        } catch (Throwable t) {
          errors.add(t);
        }
      });
      readers.add(reader);
    }
    writer.start();
    readers.forEach(Thread::start);
    writer.join();
    for (Thread r : readers) {
      r.join();
    }
    assertThat(errors).isEmpty();
    assertThat(map.size()).isEqualTo(n);
    for (int i = 0; i < n; i++) {
      assertThat(map.get(i)).isEqualTo(i);
    }
  }

  // ----------------------------------------------------------------
  // 4. 并发插入不丢数据（总数校验）
  // ----------------------------------------------------------------

  @Test
  void concurrentInsertsLoseNothing() throws Exception {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    int threads = 8;
    int perThread = 10_000;

    runConcurrently(threads, tid -> {
      for (int i = 0; i < perThread; i++) {
        int key = tid * perThread + i;
        map.put(key, key);
      }
    });

    int total = threads * perThread;
    assertThat(map.size()).isEqualTo(total); // 总数校验：一条都不能丢
    for (int i = 0; i < total; i++) {
      assertThat(map.get(i)).isEqualTo(i);
    }
  }

  @Test
  void concurrentInsertAndDeleteStayConsistent() throws Exception {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    int threads = 8;
    int perThread = 5_000;

    // 每个线程操作互不相交的键区间：插入全部，再删掉奇数键。
    runConcurrently(threads, tid -> {
      int base = tid * perThread;
      for (int i = 0; i < perThread; i++) {
        map.put(base + i, base + i);
      }
      for (int i = 1; i < perThread; i += 2) {
        map.remove(base + i);
      }
    });

    int expected = threads * perThread / 2;
    assertThat(map.size()).isEqualTo(expected);
    for (int tid = 0; tid < threads; tid++) {
      for (int i = 0; i < perThread; i++) {
        int key = tid * perThread + i;
        if (i % 2 == 0) {
          assertThat(map.get(key)).isEqualTo(key);
        } else {
          assertThat(map.get(key)).isNull(); // 已删除项绝不可被读到
        }
      }
    }
  }

  // ----------------------------------------------------------------
  // 5. 墓碑整理
  // ----------------------------------------------------------------

  @Test
  void tombstoneCleanupIsTriggeredAndKeepsData() {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    // 容量 16、负载因子 0.7：插入 10 个不触发扩容。
    for (int i = 0; i < 10; i++) {
      map.put(i, i);
    }
    assertThat(map.capacity()).isEqualTo(16);
    // 删除 8 个：墓碑数超过容量 × 0.4，触发整理。
    for (int i = 0; i < 8; i++) {
      assertThat(map.remove(i)).isEqualTo(i);
    }
    assertThat(map.cleanupCount()).isGreaterThanOrEqualTo(1);
    // 整理后：活键还在，已删键读不到，墓碑被清掉。
    assertThat(map.get(8)).isEqualTo(8);
    assertThat(map.get(9)).isEqualTo(9);
    for (int i = 0; i < 8; i++) {
      assertThat(map.get(i)).isNull();
    }
    assertThat(map.size()).isEqualTo(2);
    assertThat(map.tombstoneCount()).isLessThan(8);
  }

  @Test
  void cleanupUnderConcurrencyLosesNothing() throws Exception {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    int threads = 4;
    int perThread = 2_000;

    // 每个线程反复 插入->删除 自己的键，制造大量墓碑触发整理。
    runConcurrently(threads, tid -> {
      int base = tid * perThread;
      for (int round = 0; round < 5; round++) {
        for (int i = 0; i < perThread; i++) {
          map.put(base + i, round);
        }
        for (int i = 0; i < perThread; i++) {
          map.remove(base + i);
        }
      }
      for (int i = 0; i < perThread; i++) {
        map.put(base + i, tid);
      }
    });

    assertThat(map.cleanupCount()).isGreaterThanOrEqualTo(1);
    assertThat(map.size()).isEqualTo(threads * perThread);
    for (int tid = 0; tid < threads; tid++) {
      for (int i = 0; i < perThread; i++) {
        assertThat(map.get(tid * perThread + i)).isEqualTo(tid);
      }
    }
  }

  // ----------------------------------------------------------------
  // 6. 统计
  // ----------------------------------------------------------------

  @Test
  void statsAreReported() {
    LockFreeOpenAddressingMap<Integer, Integer> map =
        new LockFreeOpenAddressingMap<>(16, 0.7, Object::hashCode);
    for (int i = 0; i < 100; i++) {
      map.put(i, i);
    }
    for (int i = 0; i < 50; i++) {
      map.get(i);
    }
    for (int i = 0; i < 90; i++) {
      map.remove(i);
    }

    LockFreeOpenAddressingMap.Stats s = map.stats();
    assertThat(s.size()).isEqualTo(10);
    assertThat(map.size()).isEqualTo(10);
    assertThat(s.capacity()).isGreaterThanOrEqualTo(16);
    assertThat(s.loadFactor()).isCloseTo((double) s.size() / s.capacity(), within(1e-9));
    assertThat(s.averageProbeLength()).isGreaterThanOrEqualTo(1.0);
    assertThat(s.cleanupCount()).isEqualTo(map.cleanupCount());
    assertThat(s.cleanupCount() + s.resizeCount()).isGreaterThan(0);
  }

  // ----------------------------------------------------------------
  // 工具
  // ----------------------------------------------------------------

  @FunctionalInterface
  private interface ConcurrentTask {
    void run(int threadId) throws Exception;
  }

  private static void runConcurrently(int threads, ConcurrentTask task) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      final int tid = i;
      futures.add(pool.submit(() -> {
        task.run(tid);
        return null;
      }));
    }
    pool.shutdown();
    assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
    for (Future<?> f : futures) {
      f.get(); // 传播工作线程里的异常
    }
  }
}
