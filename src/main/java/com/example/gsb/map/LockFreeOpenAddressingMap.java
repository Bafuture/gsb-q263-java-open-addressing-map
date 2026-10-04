package com.example.gsb.map;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToIntFunction;

/**
 * 基于开放寻址 + CAS 的高并发哈希表。
 *
 * <p>核心设计：
 * <ul>
 *   <li>单数组开放寻址，线性探测解决冲突；槽位通过 {@link AtomicReferenceArray}
 *       的 CAS 完成插入 / 更新 / 删除，无锁。</li>
 *   <li>删除采用墓碑（tombstone）：键保留在槽位中、值置为 {@code TOMB} 哨兵。
 *       不能直接置空（null），因为开放寻址的查找以“遇到空槽”为终止条件，
 *       置空会截断探测链，使后面因冲突而顺延的键永远找不到。墓碑保证探测
 *       链连续：查找跳过墓碑继续前进，插入遇到同键墓碑可原地“复活”。</li>
 *   <li>扩容 / 墓碑整理采用协作式迁移：负载因子超阈值或墓碑比例过高时，
 *       创建新表并逐槽位用 CAS 把旧槽冻结为 {@code PRIME} 哨兵后搬运；
 *       任何线程读到 {@code PRIME} 都会先帮忙搬运（helpTransfer），再切换到
 *       新表重试，因此迁移期间读写都能正确进行。</li>
 *   <li>支持 null 键（哈希固定为 0）与自定义哈希函数。哈希质量直接决定探测
 *       长度：低熵或聚集的哈希会让键挤在一起形成一次聚集（primary
 *       clustering），线性探测的平均探测长度随负载因子 α 以
 *       ½(1+1/(1-α)) 增长，哈希分布越不均匀，实际探测长度越偏离该下界。</li>
 * </ul>
 *
 * <p>注意：值不允许为 null（与 {@link java.util.concurrent.ConcurrentHashMap} 一致）。
 */
public class LockFreeOpenAddressingMap<K, V> {

  private static final int MIN_CAPACITY = 16;
  private static final double DEFAULT_LOAD_FACTOR = 0.7;
  /** 墓碑数超过容量 × 该比例时触发整理。 */
  private static final double TOMBSTONE_RATIO = 0.4;
  /** 协作迁移时每个线程一次领取的槽位步长。 */
  private static final int TRANSFER_STRIDE = 16;

  /** 墓碑哨兵：Entry.value == TOMB 表示该键已删除，键本身保留以维持探测链。 */
  private static final Object TOMB = new Object();
  /** 冻结哨兵：迁移期间槽位被置为 PRIME，读写线程遇到它会先协助迁移再重试。 */
  private static final Object PRIME = new Object();

  /** 槽位内容：null（空） | Entry（含墓碑） | PRIME（已冻结搬走）。 */
  private static final class Entry {
    final Object key;   // 允许 null（null 键）
    final Object value; // TOMB 表示墓碑
    Entry(Object key, Object value) {
      this.key = key;
      this.value = value;
    }
  }

  /** 一张定长哈希表；扩容 / 整理时整体替换。 */
  private static final class Table {
    final int capacity;
    final AtomicReferenceArray<Object> slots;
    final LongAdder tombstones = new LongAdder();
    final AtomicInteger transferIndex = new AtomicInteger();
    final AtomicInteger primed = new AtomicInteger();
    volatile Table next;

    Table(int capacity) {
      this.capacity = capacity;
      this.slots = new AtomicReferenceArray<>(capacity);
    }
  }

  private final double loadFactor;
  private final ToIntFunction<? super K> hasher;
  private final Object transferLock = new Object();

  private volatile Table table;

  private final LongAdder size = new LongAdder();
  private final LongAdder probeSlots = new LongAdder();
  private final LongAdder probeOps = new LongAdder();
  private final LongAdder cleanups = new LongAdder();
  private final LongAdder resizes = new LongAdder();

  public LockFreeOpenAddressingMap() {
    this(MIN_CAPACITY, DEFAULT_LOAD_FACTOR, Object::hashCode);
  }

  public LockFreeOpenAddressingMap(int initialCapacity, double loadFactor,
                                   ToIntFunction<? super K> hasher) {
    if (initialCapacity < 1) {
      throw new IllegalArgumentException("initialCapacity must be >= 1");
    }
    if (loadFactor <= 0.0 || loadFactor >= 1.0) {
      throw new IllegalArgumentException("loadFactor must be in (0, 1)");
    }
    this.loadFactor = loadFactor;
    this.hasher = Objects.requireNonNull(hasher, "hasher");
    this.table = new Table(nextPow2(Math.max(MIN_CAPACITY, initialCapacity)));
  }

  // ------------------------------------------------------------------
  // 哈希
  // ------------------------------------------------------------------

  private int hashOf(Object key) {
    int h = (key == null) ? 0 : hasher.applyAsInt(cast(key));
    // 扰动函数：把高位信息混入低位，缓解低质量哈希的低位聚集。
    h ^= h >>> 16;
    return h;
  }

  @SuppressWarnings("unchecked")
  private K cast(Object key) {
    return (K) key;
  }

  // ------------------------------------------------------------------
  // 查询
  // ------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  public V get(Object key) {
    int h = hashOf(key);
    long probes = 0;
    Table t = table;
    outer:
    while (true) {
      int mask = t.capacity - 1;
      int idx = h & mask;
      for (int scanned = 0; scanned < t.capacity; scanned++) {
        Object s = t.slots.get(idx);
        probes++;
        if (s == PRIME) {
          helpTransfer(t);
          t = awaitSwap(t);
          continue outer;
        }
        if (s == null) {
          recordProbes(probes);
          return null;
        }
        Entry e = (Entry) s;
        if (Objects.equals(e.key, key)) {
          recordProbes(probes);
          return e.value == TOMB ? null : (V) e.value;
        }
        idx = (idx + 1) & mask;
      }
      // 整表无空槽且未命中：键不存在。
      recordProbes(probes);
      return null;
    }
  }

  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  // ------------------------------------------------------------------
  // 插入 / 更新
  // ------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  public V put(K key, V value) {
    Objects.requireNonNull(value, "value must not be null");
    int h = hashOf(key);
    long probes = 0;
    Table t = table;
    outer:
    while (true) {
      int mask = t.capacity - 1;
      int idx = h & mask;
      int scanned = 0;
      while (scanned < t.capacity) {
        Object s = t.slots.get(idx);
        probes++;
        if (s == PRIME) {
          helpTransfer(t);
          t = awaitSwap(t);
          continue outer;
        }
        if (s == null) {
          Entry e = new Entry(key, value);
          if (t.slots.compareAndSet(idx, null, e)) {
            size.increment();
            recordProbes(probes);
            checkCapacity(t);
            return null;
          }
          continue; // CAS 竞争失败，重读同一槽位
        }
        Entry e = (Entry) s;
        if (Objects.equals(e.key, key)) {
          Object old = e.value;
          Entry ne = new Entry(e.key, value);
          if (t.slots.compareAndSet(idx, e, ne)) {
            if (old == TOMB) { // 墓碑复活：逻辑上是一次新插入
              size.increment();
              t.tombstones.decrement();
              recordProbes(probes);
              checkCapacity(t);
              return null;
            }
            recordProbes(probes);
            return (V) old;
          }
          continue;
        }
        idx = (idx + 1) & mask;
        scanned++;
      }
      // 整表无空槽：强制扩容后重试（正常触发路径之外的兜底）。
      startTransfer(t, t.capacity * 2, false);
      t = awaitSwap(t);
    }
  }

  // ------------------------------------------------------------------
  // 删除（墓碑）
  // ------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  public V remove(Object key) {
    int h = hashOf(key);
    long probes = 0;
    Table t = table;
    outer:
    while (true) {
      int mask = t.capacity - 1;
      int idx = h & mask;
      int scanned = 0;
      while (scanned < t.capacity) {
        Object s = t.slots.get(idx);
        probes++;
        if (s == PRIME) {
          helpTransfer(t);
          t = awaitSwap(t);
          continue outer;
        }
        if (s == null) {
          recordProbes(probes);
          return null;
        }
        Entry e = (Entry) s;
        if (Objects.equals(e.key, key)) {
          Object old = e.value;
          if (old == TOMB) {
            recordProbes(probes);
            return null;
          }
          Entry tomb = new Entry(e.key, TOMB);
          if (t.slots.compareAndSet(idx, e, tomb)) {
            size.decrement();
            t.tombstones.increment();
            recordProbes(probes);
            checkCapacity(t);
            return (V) old;
          }
          continue;
        }
        idx = (idx + 1) & mask;
        scanned++;
      }
      recordProbes(probes);
      return null;
    }
  }

  // ------------------------------------------------------------------
  // 扩容 / 墓碑整理（协作式迁移）
  // ------------------------------------------------------------------

  private void checkCapacity(Table t) {
    if (t.next != null) {
      helpTransfer(t);
      return;
    }
    if (t != table) {
      return;
    }
    long sz = size.sum();
    int cap = t.capacity;
    if (sz > (long) (cap * loadFactor)) {
      startTransfer(t, cap * 2, false);
      return;
    }
    long tomb = t.tombstones.sum();
    if (tomb > (long) (cap * TOMBSTONE_RATIO) || sz + tomb > (long) (cap * loadFactor)) {
      // 墓碑比例过高：重建一张只装活键的新表（可能同容量或缩容）。
      startTransfer(t, capacityFor(sz), true);
    }
  }

  private int capacityFor(long sz) {
    long needed = (long) (sz / loadFactor) + 2;
    return nextPow2(Math.max(MIN_CAPACITY, needed));
  }

  private void startTransfer(Table t, int newCapacity, boolean cleanup) {
    if (t.next != null || t != table) {
      helpTransfer(t);
      return;
    }
    synchronized (transferLock) {
      if (table == t && t.next == null) {
        t.next = new Table(nextPow2(Math.max(MIN_CAPACITY, newCapacity)));
        if (cleanup) {
          cleanups.increment();
        } else {
          resizes.increment();
        }
      }
    }
    helpTransfer(t);
  }

  /** 所有线程协作搬运：按步长认领槽位区间，逐槽冻结并复制。 */
  private void helpTransfer(Table t) {
    Table nxt = t.next;
    if (nxt == null) {
      return;
    }
    int cap = t.capacity;
    while (true) {
      int i = t.transferIndex.getAndAdd(TRANSFER_STRIDE);
      if (i >= cap) {
        return;
      }
      int end = Math.min(i + TRANSFER_STRIDE, cap);
      for (int j = i; j < end; j++) {
        copySlot(t, nxt, j);
      }
    }
  }

  /**
   * 复制单个槽位：先把活键值对写入新表，再 CAS 冻结旧槽。
   * 冻结失败说明有并发写改了该槽：若键被删除则从新表撤掉刚才的复制，
   * 若是值被更新则循环重试重新复制最新值。
   */
  private void copySlot(Table old, Table nxt, int j) {
    while (true) {
      Object s = old.slots.get(j);
      if (s == PRIME) {
        return;
      }
      if (s == null) {
        if (old.slots.compareAndSet(j, null, PRIME)) {
          primed(old, nxt);
          return;
        }
        continue;
      }
      Entry e = (Entry) s;
      if (e.value == TOMB) {
        if (old.slots.compareAndSet(j, e, PRIME)) {
          primed(old, nxt);
          return;
        }
        continue;
      }
      putInto(nxt, e.key, e.value);
      if (old.slots.compareAndSet(j, e, PRIME)) {
        primed(old, nxt);
        return;
      }
      Object cur = old.slots.get(j);
      if (cur instanceof Entry && ((Entry) cur).value == TOMB) {
        removeFrom(nxt, e.key); // 并发删除：撤销刚才的复制
      }
    }
  }

  private void primed(Table old, Table nxt) {
    if (old.primed.incrementAndGet() == old.capacity) {
      // 最后一个槽位冻结完成：原子切换当前表，之后所有操作走新表。
      synchronized (transferLock) {
        if (table == old) {
          table = nxt;
        }
      }
    }
  }

  /** 等待旧表迁移完成并返回当前表。 */
  private Table awaitSwap(Table old) {
    Table t;
    while ((t = table) == old) {
      helpTransfer(old);
      Thread.onSpinWait();
    }
    return t;
  }

  /** 迁移专用：往新表写入（新表此时只有搬运线程在写，直接覆盖式 put）。 */
  private void putInto(Table t, Object key, Object value) {
    int h = hashOf(key);
    while (true) {
      int mask = t.capacity - 1;
      int idx = h & mask;
      for (int scanned = 0; scanned < t.capacity; scanned++) {
        Object s = t.slots.get(idx);
        if (s == PRIME && t.next != null) { // 防御：新表自身也在迁移
          helpTransfer(t);
          t = awaitSwap(t);
          break;
        }
        if (s == null) {
          if (t.slots.compareAndSet(idx, null, new Entry(key, value))) {
            return;
          }
          scanned--; // 重读同一槽位
          continue;
        }
        Entry e = (Entry) s;
        if (Objects.equals(e.key, key)) {
          if (t.slots.compareAndSet(idx, e, new Entry(e.key, value))) {
            return;
          }
          scanned--;
          continue;
        }
        idx = (idx + 1) & mask;
      }
      if (t.next == null) {
        return; // 不会发生（新表容量足够），防御性返回
      }
    }
  }

  /** 迁移专用：从新表撤销一个键（并发删除导致），置墓碑保持探测链。 */
  private void removeFrom(Table t, Object key) {
    int h = hashOf(key);
    int mask = t.capacity - 1;
    int idx = h & mask;
    for (int scanned = 0; scanned < t.capacity; scanned++) {
      Object s = t.slots.get(idx);
      if (s == null) {
        return;
      }
      if (s instanceof Entry) {
        Entry e = (Entry) s;
        if (Objects.equals(e.key, key)) {
          if (e.value != TOMB) {
            t.slots.compareAndSet(idx, e, new Entry(e.key, TOMB));
            t.tombstones.increment();
          }
          return;
        }
      }
      idx = (idx + 1) & mask;
    }
  }

  // ------------------------------------------------------------------
  // 统计
  // ------------------------------------------------------------------

  private void recordProbes(long probes) {
    probeSlots.add(probes);
    probeOps.increment();
  }

  public int size() {
    return (int) Math.min(Integer.MAX_VALUE, size.sum());
  }

  public int capacity() {
    return table.capacity;
  }

  public double loadFactor() {
    return (double) size.sum() / table.capacity;
  }

  public double averageProbeLength() {
    long ops = probeOps.sum();
    return ops == 0 ? 0.0 : (double) probeSlots.sum() / ops;
  }

  public long tombstoneCount() {
    return table.tombstones.sum();
  }

  public long cleanupCount() {
    return cleanups.sum();
  }

  public long resizeCount() {
    return resizes.sum();
  }

  public Stats stats() {
    return new Stats(size(), capacity(), loadFactor(), averageProbeLength(),
        tombstoneCount(), cleanupCount(), resizeCount());
  }

  /** 统计快照。 */
  public record Stats(long size, int capacity, double loadFactor,
                      double averageProbeLength, long tombstones,
                      long cleanupCount, long resizeCount) {
  }

  private static int nextPow2(long n) {
    int cap = 1;
    while (cap < n) {
      cap <<= 1;
    }
    return cap;
  }
}
