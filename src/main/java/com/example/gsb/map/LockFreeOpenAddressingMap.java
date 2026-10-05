package com.example.gsb.map;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 基于开放寻址 + CAS 的无锁哈希表。
 *
 * <p>核心设计：
 * <ul>
 *   <li>每个槽位是 {@link AtomicReferenceArray} 的一个元素，状态为
 *       {@code null}(空) / {@link Node}(活条目) / {@code TOMBSTONE}(墓碑) / {@link Forwarding}(迁移标记)。</li>
 *   <li>插入只发生在空槽上，通过 CAS 完成，冲突时按线性探测序列向后找空位。
 *       因为插入从不复用墓碑槽，所以同一键不可能出现两个活副本（无需额外的去重逻辑）。</li>
 *   <li>删除把活条目 CAS 成墓碑。不能直接置空：开放寻址的查找沿探测序列前进，
 *       遇到空槽即判定“键不存在”。若把中间槽置空，序列被截断，
 *       排在其后、因冲突而顺延的键将永远找不到。墓碑表示“这里曾经有值，请继续探测”，
 *       查找会跳过它继续前进。</li>
 *   <li>负载因子超过阈值或墓碑比例过高时触发重建（扩容 / 同容量整理），
 *       采用“旧表 + 新表”转发式迁移：写入线程先帮助把旧表逐段迁移到新表再操作，
 *       读操作无锁、遇到转发标记自动跳到新表继续，因此迁移期间读写都正确。</li>
 *   <li>支持 null 键（内部规范化为哨兵对象）与自定义 {@link HashFunction}。</li>
 * </ul>
 */
public class LockFreeOpenAddressingMap<K, V> {

    /** 触发扩容的负载因子阈值（活条目数 / 容量）。 */
    static final double RESIZE_LOAD_FACTOR = 0.70;
    /** 触发墓碑整理的墓碑比例阈值（墓碑数 / 容量）。 */
    static final double TOMBSTONE_RATIO = 0.50;
    /** 每次帮助迁移的槽位段长度。 */
    private static final int STRIDE = 32;

    private static final Object TOMBSTONE = new Object();

    private final HashFunction<? super K> hashFunction;
    private final AtomicReference<Table> tableRef;

    /** 累计探测步数与探测次数（用于统计平均探测长度），单调递增，允许轻微竞态误差。 */
    private final AtomicLong probeSteps = new AtomicLong();
    private final AtomicLong probeOps = new AtomicLong();
    /** 墓碑整理（compaction）触发次数。 */
    private final AtomicInteger compactions = new AtomicInteger();

    public LockFreeOpenAddressingMap() {
        this(16, HashFunction.defaultHash());
    }

    public LockFreeOpenAddressingMap(int initialCapacity, HashFunction<? super K> hashFunction) {
        if (initialCapacity < 2) {
            throw new IllegalArgumentException("capacity must be >= 2");
        }
        this.hashFunction = Objects.requireNonNull(hashFunction, "hashFunction");
        this.tableRef = new AtomicReference<>(new Table(Integer.highestOneBit(initialCapacity - 1) << 1));
    }

    // ------------------------------------------------------------------
    // 内部结构
    // ------------------------------------------------------------------

    /** 活条目。value 用 VarHandle 保证并发替换的可见性与 CAS 语义。 */
    static final class Node {
        final Object key;
        volatile Object value;

        Node(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        private static final VarHandle VALUE;
        static {
            try {
                VALUE = MethodHandles.lookup().findVarHandle(Node.class, "value", Object.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }

    /** 槽位被迁移到新表后的转发标记。 */
    static final class Forwarding {
        final Table next;

        Forwarding(Table next) {
            this.next = next;
        }
    }

    /** 一张表：槽位数组 + 计数器 + 迁移游标 + 指向新表的引用。 */
    static final class Table {
        final AtomicReferenceArray<Object> slots;
        final int mask;
        /** 活条目数（迁移完成后等于新表元素数）。 */
        final AtomicInteger live = new AtomicInteger();
        /** 墓碑数。 */
        final AtomicInteger tombs = new AtomicInteger();
        /** 迁移游标：小于它的槽位已被认领迁移。 */
        final AtomicInteger cursor = new AtomicInteger();
        /** 已迁移完成的槽位数，等于容量时整体切换到新表。 */
        final AtomicInteger migrated = new AtomicInteger();
        volatile Table forward;

        private static final VarHandle FORWARD;
        static {
            try {
                FORWARD = MethodHandles.lookup().findVarHandle(Table.class, "forward", Table.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        Table(int capacity) {
            this.slots = new AtomicReferenceArray<>(capacity);
            this.mask = capacity - 1;
        }

        int capacity() {
            return slots.length();
        }
    }

    /** 统计快照。 */
    public record MapStats(int size, int capacity, double loadFactor,
                           double averageProbeLength, long compactions) {
    }

    // ------------------------------------------------------------------
    // 哈希与探测
    // ------------------------------------------------------------------

    private Object maskKey(K key) {
        return key == null ? NullKey.INSTANCE : key;
    }

    private int spread(Object key) {
        int h = hashFunction.hash(unmask(key));
        // 高位扰动，缓解弱哈希函数低位雷同的问题；并保证非负。
        return (h ^ (h >>> 16)) & 0x7fffffff;
    }

    @SuppressWarnings("unchecked")
    private K unmask(Object key) {
        return key == NullKey.INSTANCE ? null : (K) key;
    }

    private enum NullKey { INSTANCE }

    private void countProbe(int steps) {
        probeSteps.addAndGet(steps);
        probeOps.incrementAndGet();
    }

    // ------------------------------------------------------------------
    // 读
    // ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public V get(K key) {
        Object k = maskKey(key);
        Table t = tableRef.get();
        int idx = spread(k) & t.mask;
        int steps = 0;
        while (true) {
            Object s = t.slots.get(idx);
            steps++;
            if (s == null) {          // 空槽：探测序列结束，键不存在
                countProbe(steps);
                return null;
            }
            if (s == TOMBSTONE) {     // 墓碑：曾有条目，继续探测
                idx = (idx + 1) & t.mask;
                continue;
            }
            if (s instanceof Forwarding f) {  // 迁移中：跳到新表重新探测
                t = f.next;
                idx = spread(k) & t.mask;
                steps = 0;
                continue;
            }
            Node n = (Node) s;
            if (n.key.equals(k)) {
                countProbe(steps);
                return (V) n.value;
            }
            idx = (idx + 1) & t.mask;
        }
    }

    public boolean containsKey(K key) {
        return get(key) != null;
    }

    // ------------------------------------------------------------------
    // 写
    // ------------------------------------------------------------------

    /**
     * 插入或更新。若旧表正在迁移，先帮助迁移完成再写入新表，
     * 因此新插入永远落在当前最新表上，迁移期间写入不丢数据。
     */
    public void put(K key, V value) {
        Object k = maskKey(key);
        Node node = new Node(k, value);
        Table t = tableRef.get();
        while (true) {
            if (t.forward != null) {          // 有正在进行的迁移：先帮忙搬完
                helpTransfer(t);
                t = tableRef.get();
                continue;
            }
            int idx = spread(k) & t.mask;
            int steps = 0;
            boolean done = false;
            while (true) {
                Object s = t.slots.get(idx);
                steps++;
                if (s == null) {
                    if (t.slots.compareAndSet(idx, null, node)) {
                        countProbe(steps);
                        int live = t.live.incrementAndGet();
                        maybeResize(t, live);
                        done = true;
                        break;
                    }
                    continue;                 // CAS 失败：重读该槽
                }
                if (s == TOMBSTONE) {         // 墓碑不可复用，继续探测（保证同键唯一）
                    idx = (idx + 1) & t.mask;
                    continue;
                }
                if (s instanceof Forwarding) {
                    break;                    // 表开始迁移：回到外层帮助迁移
                }
                Node n = (Node) s;
                if (n.key.equals(k)) {        // 更新已存在的键
                    Node.VALUE.setVolatile(n, value);
                    countProbe(steps);
                    return;
                }
                idx = (idx + 1) & t.mask;
            }
            if (done) {
                return;
            }
        }
    }

    /**
     * 删除。把活条目 CAS 成墓碑（而不是置空），保证不截断探测序列。
     * 删除后若墓碑比例过高则触发整理。
     */
    @SuppressWarnings("unchecked")
    public V remove(K key) {
        Object k = maskKey(key);
        Table t = tableRef.get();
        while (true) {
            if (t.forward != null) {
                helpTransfer(t);
                t = tableRef.get();
                continue;
            }
            int idx = spread(k) & t.mask;
            int steps = 0;
            while (true) {
                Object s = t.slots.get(idx);
                steps++;
                if (s == null) {
                    countProbe(steps);
                    return null;
                }
                if (s == TOMBSTONE) {
                    idx = (idx + 1) & t.mask;
                    continue;
                }
                if (s instanceof Forwarding) {
                    break;                    // 回到外层帮助迁移后重试
                }
                Node n = (Node) s;
                if (n.key.equals(k)) {
                    V old = (V) n.value;
                    if (t.slots.compareAndSet(idx, n, TOMBSTONE)) {
                        countProbe(steps);
                        t.live.decrementAndGet();
                        int tombs = t.tombs.incrementAndGet();
                        maybeCompact(t, tombs);
                        return old;
                    }
                    continue;                 // CAS 失败：重读该槽
                }
                idx = (idx + 1) & t.mask;
            }
        }
    }

    public void clear() {
        tableRef.set(new Table(16));
    }

    // ------------------------------------------------------------------
    // 扩容与墓碑整理（共用转发式迁移）
    // ------------------------------------------------------------------

    private void maybeResize(Table t, int live) {
        if (live > (int) (t.capacity() * RESIZE_LOAD_FACTOR) && t.forward == null) {
            Table next = new Table(t.capacity() * 2);
            if (tryStartMigration(t, next)) {
                helpTransfer(t);
            }
        }
    }

    private void maybeCompact(Table t, int tombs) {
        int live = t.live.get();
        if (tombs >= 4 && (long) tombs * 2 >= (long) live + tombs && t.forward == null) {
            // 同容量重建：丢弃墓碑，只迁移活条目。
            Table next = new Table(t.capacity());
            if (tryStartMigration(t, next)) {
                compactions.incrementAndGet();
                helpTransfer(t);
            }
        }
    }

    private boolean tryStartMigration(Table t, Table next) {
        // 通过 CAS 设置 forward 保证只启动一次迁移，失败说明别的线程已先行。
        return tableRef.get() == t && Table.FORWARD.compareAndSet(t, null, next);
    }

    /**
     * 帮助把旧表逐段迁移到新表。所有写线程在迁移期间都会先进入这里，
     * 因此迁移由并发写操作共同推动完成；读操作无需参与，直接跟随转发标记。
     */
    private void helpTransfer(Table old) {
        Table next = old.forward;
        int capacity = old.capacity();
        while (true) {
            int start = old.cursor.getAndAdd(STRIDE);
            if (start >= capacity) {
                break;                        // 所有段都已被认领
            }
            int end = Math.min(start + STRIDE, capacity);
            for (int i = start; i < end; i++) {
                migrateSlot(old, next, i);
            }
            if (old.migrated.addAndGet(end - start) == capacity) {
                // 最后一个完成迁移的线程负责整体切换；切换后旧表只作为转发跳板存在。
                tableRef.compareAndSet(old, next);
            }
        }
    }

    /**
     * 迁移单个槽位：活条目发布到新表后，把旧槽 CAS 成转发标记。
     * 关键竞态：节点发布到新表之后、旧槽被置为转发标记之前，
     * 可能被并发删除 CAS 成墓碑。此时必须把已发布到新表的节点一并撤回，
     * 否则已删除的键会在新表“复活”。
     */
    private void migrateSlot(Table old, Table next, int i) {
        Node publishedNode = null;
        int publishedIdx = -1;
        while (true) {
            Object s = old.slots.get(i);
            if (s == null || s == TOMBSTONE) {
                if (old.slots.compareAndSet(i, s, new Forwarding(next))) {
                    if (s == TOMBSTONE && publishedNode != null
                            && next.slots.compareAndSet(publishedIdx, publishedNode, TOMBSTONE)) {
                        next.live.decrementAndGet();
                        next.tombs.incrementAndGet();
                    }
                    return;                    // 空槽与墓碑直接丢弃（墓碑整理即发生在这里）
                }
                continue;
            }
            if (s instanceof Forwarding) {
                return;                        // 该槽已完成迁移
            }
            Node n = (Node) s;
            publishedIdx = publish(next, n);   // 先把活条目放进新表
            publishedNode = n;
            if (old.slots.compareAndSet(i, n, new Forwarding(next))) {
                next.live.incrementAndGet();
                return;
            }
            // CAS 失败说明槽位状态变了（被并发删除或已转发），重读后重新处理。
        }
    }

    /**
     * 把节点发布到新表（putIfAbsent 语义），返回落点下标。
     * 新表此时不可能有同键活条目（旧表已停止写入），同键命中只是防御性检查。
     */
    private int publish(Table next, Node node) {
        int idx = spread(node.key) & next.mask;
        while (true) {
            Object s = next.slots.get(idx);
            if (s == null) {
                if (next.slots.compareAndSet(idx, null, node)) {
                    return idx;
                }
                continue;
            }
            if (s == TOMBSTONE) {
                idx = (idx + 1) & next.mask;
                continue;
            }
            Node existing = (Node) s;
            if (existing.key.equals(node.key)) {
                return idx;                    // 防御：不覆盖已存在的同键条目
            }
            idx = (idx + 1) & next.mask;
        }
    }

    // ------------------------------------------------------------------
    // 统计
    // ------------------------------------------------------------------

    public int size() {
        return tableRef.get().live.get();
    }

    public int capacity() {
        return tableRef.get().capacity();
    }

    public double loadFactor() {
        Table t = tableRef.get();
        return (double) t.live.get() / t.capacity();
    }

    /** 平均探测长度 = 累计探测步数 / 探测次数。 */
    public double averageProbeLength() {
        long ops = probeOps.get();
        return ops == 0 ? 0.0 : (double) probeSteps.get() / ops;
    }

    public long compactionCount() {
        return compactions.get();
    }

    public MapStats stats() {
        return new MapStats(size(), capacity(), loadFactor(), averageProbeLength(), compactionCount());
    }

    /** 当前所有键的快照（逐槽扫描，弱一致），主要用于测试与调试。 */
    @SuppressWarnings("unchecked")
    public List<K> keys() {
        Table t = tableRef.get();
        List<K> result = new ArrayList<>();
        for (int i = 0; i < t.capacity(); i++) {
            Object s = t.slots.get(i);
            if (s instanceof Node n) {
                result.add((K) unmask(n.key));
            }
        }
        return result;
    }
}
