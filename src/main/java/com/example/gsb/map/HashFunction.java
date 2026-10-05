package com.example.gsb.map;

/**
 * 键的哈希函数。允许传入自定义实现以观察哈希质量对探测长度的影响：
 * 哈希分布越均匀，键在数组上越分散，冲突越少，平均探测长度越接近 1；
 * 哈希质量差（如常数哈希）会让大量键挤在同一探测序列上，
 * 平均探测长度随之线性增长，开放寻址退化为顺序扫描。
 */
@FunctionalInterface
public interface HashFunction<K> {

    HashFunction<Object> DEFAULT = key -> key == null ? 0 : key.hashCode();

    int hash(K key);

    @SuppressWarnings("unchecked")
    static <K> HashFunction<K> defaultHash() {
        return (HashFunction<K>) DEFAULT;
    }
}
