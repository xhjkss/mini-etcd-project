package com.xhj.etcd.kernel.etcd.store.mvcc;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * KeyValueStore
 *
 * @author XJks
 * @description 当前阶段最小 MVCC KV 状态机，覆盖 Put/Get/Range/Delete/DeleteRange/Compact。
 *
 * <p>该类不是通用并发容器，设计前提是由 EtcdNode 的 event-loop 串行访问。</p>
 *
 * <p>
 * TODO:
 *  KeyValueStore 与 KeyValueRecord 的 MVCC 字段关系总览：
 *  1) KeyValueStore.currentRevision：全局写时钟，所有 key 共用；只有真实写变更才推进。
 *  2) KeyValueRecord.modRevision：该条记录写入时对应的全局 revision（取值来自 currentRevision）。
 *  3) KeyValueRecord.createRevision：该 key 当前存活代第一次写入时的全局 revision。
 *  4) KeyValueRecord.version：该 key 在当前存活代内的变更次数（单 key 计数，不是全局计数）。
 *  5) delete tombstone 会结束旧存活代；后续 put 开启新存活代，createRevision 重置、version 从 1 开始。
 * </p>
 */
public class KeyValueStore {

    /**
     * FNV-1a 64 位哈希初始值（offset basis）。
     *
     * <p>
     * TODO:
     *  FNV = Fowler-Noll-Vo，是一种“快、实现简单、结果稳定”的非加密哈希。
     *  这里用它来做 KV 状态一致性对账（HashKv），不是做安全签名或防篡改。
     * </p>
     */
    private static final long FNV_64_OFFSET_BASIS = 1469598103934665603L;

    /**
     * FNV-1a 64 位哈希乘数（prime）。
     */
    private static final long FNV_64_PRIME = 1099511628211L;

    /**
     * key -> 历史记录（按 modRevision 升序）。
     */
    private final NavigableMap<String, List<KeyValueRecord>> historyByKey = new TreeMap<>();

    /**
     * 当前 revision。
     *
     * <p>
     * TODO:
     *  这是“全局 revision”，不是某个 key 的本地版本号。
     *  对外可理解为状态机提交写操作的全局单调时钟；KeyValueRecord.modRevision/createRevision 都基于该时钟取值。
     * </p>
     */
    private long currentRevision;

    /**
     * 历史压缩边界 revision。
     *
     * <p>
     * TODO:
     *  该值表示“已经被 compact 的历史下界”：
     *  1) requestedRevision < compactRevision 的历史读必须返回 compacted 错误；
     *  2) compactRevision 只会单调递增，不会回退；
     *  3) compact 不推进 currentRevision，只改变可读取历史窗口。
     * </p>
     */
    private long compactRevision;

    /**
     * 获取当前状态机 revision。
     */
    public long currentRevision() {
        return currentRevision;
    }

    /**
     * 获取当前 compact revision。
     */
    public long compactRevision() {
        return compactRevision;
    }

    /**
     * 计算指定 revision 下的状态机哈希。
     *
     * <p>哈希只覆盖该 revision 下的可见 key/value 状态，不把当前 revision 号本身混入哈希值，避免
     * “状态相同但写时钟不同”时产生不必要的哈希差异。</p>
     *
     * <p>
     * TODO:
     *  小白可按下面理解该哈希流程：
     *  1) 先用固定初始值 FNV_64_OFFSET_BASIS 作为起点；
     *  2) 再按“确定顺序”把每个可见记录的字段喂进哈希（key、value、createRevision、modRevision、version、leaseId）；
     *  3) 每喂入一个字节都执行 FNV-1a 步骤：hash = (hash XOR byte) * prime；
     *  4) 最终得到 long 值，作为该 revision 下状态机视图摘要。
     * <p>
     * 为什么能用于多节点对账：
     * - 顺序稳定：historyByKey 是 TreeMap，遍历顺序按 key 字典序固定；
     * - 编码稳定：字符串统一 UTF-8，数字统一按固定字节序展开；
     * - 输入一致时输出一致，便于快速发现状态分歧。
     * </p>
     *
     * @param requestedRevision 目标 revision，0 表示当前最新 revision
     * @return 状态机哈希
     */
    public long computeKvStateHash(long requestedRevision) {
        long revision = resolveReadRevision(requestedRevision);
        long hash = FNV_64_OFFSET_BASIS;
        for (Map.Entry<String, List<KeyValueRecord>> entry : historyByKey.entrySet()) {
            KeyValueRecord visibleRecord = getVisibleRecordByRevision(entry.getKey(), revision);
            if (visibleRecord == null) {
                continue;
            }
            hash = fnvUpdateString(hash, visibleRecord.getKey());
            hash = fnvUpdateString(hash, visibleRecord.getValue());
            hash = fnvUpdateLong(hash, visibleRecord.getCreateRevision());
            hash = fnvUpdateLong(hash, visibleRecord.getModRevision());
            hash = fnvUpdateLong(hash, visibleRecord.getVersion());
            hash = fnvUpdateLong(hash, visibleRecord.getLeaseId());
        }
        return hash;
    }

    /**
     * 统计指定 revision 下的可见 key 数量。
     *
     * @param requestedRevision 目标 revision，0 表示当前最新 revision
     * @return 可见 key 数量
     */
    public int countVisibleKeys(long requestedRevision) {
        long revision = resolveReadRevision(requestedRevision);
        int count = 0;
        for (Map.Entry<String, List<KeyValueRecord>> entry : historyByKey.entrySet()) {
            if (getVisibleRecordByRevision(entry.getKey(), revision) != null) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * 写入 key-value。
     *
     * <p>每次写入都会推进 revision，并按 MVCC 规则更新 createRevision 和 version。</p>
     */
    public KeyValueRecord put(String key, String value) {
        return put(key, value, 0L);
    }

    /**
     * 写入 key-value，并可选绑定 lease。
     *
     * <p>每次写入都会推进 revision，并按 MVCC 规则更新 createRevision 和 version。</p>
     *
     * @param key     待写入 key
     * @param value   待写入 value
     * @param leaseId 绑定的 leaseId，0 表示不绑定
     * @return 写入后的可见记录
     */
    public KeyValueRecord put(String key, String value, long leaseId) {
        // 1) 先做 key 校验，避免空 key 进入状态机破坏范围语义。
        validateKey(key);

        // 2) 写入动作统一推进全局 revision。
        long revision = nextRevision();
        // 3) 读取 revision-1 的可见记录，用于计算 createRevision/version。
        KeyValueRecord previousRecord = getVisibleRecordByRevision(key, revision - 1L);

        KeyValueRecord record = new KeyValueRecord();
        record.setKey(key);
        record.setValue(value);
        record.setModRevision(revision);
        record.setDeleted(false);
        record.setLeaseId(Math.max(0L, leaseId));

        if (previousRecord == null) {
            /**
             * TODO:
             *  previousRecord==null 表示“该 key 在 revision-1 时不可见”（首次写入或上一个存活代已被 tombstone 结束）。
             *  此时需要开启新的存活代：
             *  1) createRevision = 当前写入 revision；
             *  2) version = 1（单 key 版本计数重新起步，不等于全局 revision）。
             */
            record.setCreateRevision(revision);
            record.setVersion(1L);
        } else {
            /**
             * TODO:
             *  previousRecord!=null 表示仍在同一存活代内更新：
             *  1) createRevision 继承上一可见版本；
             *  2) version 在该存活代内递增。
             */
            record.setCreateRevision(previousRecord.getCreateRevision());
            record.setVersion(previousRecord.getVersion() + 1L);
        }

        // 4) 追加新版本记录，保持单 key 历史按 revision 有序。
        appendRecord(record);
        return copyVisibleRecord(record);
    }

    /**
     * 按 revision 读取指定 key。
     *
     * <p>requestedRevision=0 表示读取当前最新 revision。</p>
     */
    public KeyValueRecord get(String key, long requestedRevision) {
        // 1) 校验 key，避免非法 key 导致读取语义不明确。
        validateKey(key);
        // 2) 解析目标 revision。0 表示最新，其他值必须落在有效区间内。
        long revision = resolveReadRevision(requestedRevision);
        // 3) 返回该 revision 下可见版本（已删除或不存在返回 null）。
        return copyVisibleRecord(getVisibleRecordByRevision(key, revision));
    }

    /**
     * 范围读取。
     *
     * <p>支持单 key、区间和前缀三种读取语义，并支持 limit、keysOnly、countOnly。</p>
     *
     * <p>
     * TODO:
     *  endKeyExclusive 采用“左闭右开”语义：
     *  1) startKey 是包含边界（>= startKey）；
     *  2) endKeyExclusive 是排除边界（< endKeyExclusive）；
     *  3) 当 endKeyExclusive 为空时，range 退化为单 key 读取。
     *  这种设计可统一表达单点、区间和前缀三种查询。
     * </p>
     *
     * <p>案例说明：</p>
     * <p>1) 单 key 读取：startKey="user1"，endKeyExclusive=null，prefixMatch=false -> 只读 user1。</p>
     * <p>2) 区间读取：startKey="a"，endKeyExclusive="d"，prefixMatch=false -> 读取 ["a","d")，即 a<=k<d。</p>
     * <p>3) 前缀读取：startKey="app/"，prefixMatch=true -> 读取所有以 app/ 开头的 key。</p>
     * <p>4) keysOnly=true -> 只返回 key，不返回 value。</p>
     * <p>5) countOnly=true -> 只关心匹配数量，返回列表可为空。</p>
     */
    public List<KeyValueRecord> range(String startKey,
                                      String endKeyExclusive,
                                      boolean prefixMatch,
                                      int limit,
                                      boolean keysOnly,
                                      boolean countOnly,
                                      long requestedRevision) {
        // 1) 统一参数校验和 revision 解析。
        validateKey(startKey);
        long revision = resolveReadRevision(requestedRevision);
        // 2) 计算实际 rangeEnd。
        //    prefix=false 时直接使用调用方传入的 rangeEnd；
        //    prefix=true 时把“前缀查询”转换成左闭右开区间查询。
        String resolvedEndKeyExclusive = resolveEndKeyExclusive(startKey, endKeyExclusive, prefixMatch);

        List<KeyValueRecord> records = new ArrayList<>();
        // 3) rangeEnd 为空时退化为单 key 读取。
        if (resolvedEndKeyExclusive == null || resolvedEndKeyExclusive.length() == 0) {
            KeyValueRecord record = getVisibleRecordByRevision(startKey, revision);
            if (record != null) {
                records.add(copyForRangeResult(record, keysOnly));
            }
            // countOnly=true 时只关心计数，不返回明细。
            return countOnly ? new ArrayList<>() : records;
        }

        // 4) 区间读取直接使用有序 map 的子区间视图，避免全量扫描+手工排序。
        //    subMap(key,true,actualRangeEnd,false) 等价于读取 [key, actualRangeEnd)。
        NavigableMap<String, List<KeyValueRecord>> rangeView = historyByKey.subMap(startKey, true, resolvedEndKeyExclusive, false);
        for (Map.Entry<String, List<KeyValueRecord>> entry : rangeView.entrySet()) {
            String storedKey = entry.getKey();
            KeyValueRecord record = getVisibleRecordByRevision(storedKey, revision);
            if (record != null) {
                records.add(copyForRangeResult(record, keysOnly));
            }
        }

        // 5) 最后应用 limit 和 countOnly 语义。
        if (limit > 0 && records.size() > limit) {
            records = new ArrayList<>(records.subList(0, limit));
        }
        return countOnly ? new ArrayList<>() : records;
    }

    /**
     * 按范围读取历史记录（包含 tombstone），用于 Watch 事件回放。
     *
     * <p>该方法不会套用 get/range 的“只看当前可见值”逻辑，而是直接返回 revision 区间内的历史版本，
     * 供 Watch 在创建时做历史回放和在实时推送前补齐缺失事件。</p>
     *
     * <p>处理思路：</p>
     * <ol>
     *     <li>先把单 key / 前缀 / range 三种访问方式统一成一个左闭右开区间。</li>
     *     <li>再在每个 key 的历史版本列表里过滤 revision 窗口。</li>
     *     <li>最后按 revision 升序、key 升序做稳定排序，保证多节点回放顺序一致。</li>
     * </ol>
     *
     * <p>Watch 创建时会先拿到当前全局 revision，再把客户端给的 startRevision 与这个全局点对齐。
     * 这里返回的就是“从 startRevision 到 currentRevision 之间真正发生过的变更历史”。</p>
     *
     * @param startKey               起始 key
     * @param endKeyExclusive        结束 key（左闭右开）
     * @param prefixMatch            是否按前缀匹配
     * @param startRevisionInclusive 起始 revision（含）
     * @param endRevisionInclusive   结束 revision（含）
     * @param limit                  最大返回条数，0 表示不限制
     * @return 按 modRevision 升序、key 升序稳定排序的历史记录
     */
    public List<KeyValueRecord> rangeHistoryRecordsForWatch(String startKey, String endKeyExclusive, boolean prefixMatch, long startRevisionInclusive, long endRevisionInclusive, int limit) {
        validateKey(startKey);
        if (endRevisionInclusive < 1L) {
            return new ArrayList<>();
        }
        long normalizedStartRevisionInclusive = Math.max(1L, startRevisionInclusive);
        if (normalizedStartRevisionInclusive > endRevisionInclusive) {
            return new ArrayList<>();
        }

        String resolvedEndKeyExclusive = resolveEndKeyExclusive(startKey, endKeyExclusive, prefixMatch);
        List<KeyValueRecord> records = new ArrayList<>();
        if (resolvedEndKeyExclusive == null || resolvedEndKeyExclusive.length() == 0) {
            // 单 key 场景：只读取 startKey 自身对应的历史链。
            // 这个分支通常对应 watch 的单 key 订阅。
            appendHistoryRecordsInRevisionWindow(historyByKey.get(startKey),
                    normalizedStartRevisionInclusive,
                    endRevisionInclusive,
                    records);
        } else {
            // 区间/前缀场景：先在有序 key 空间里拿到目标视图，再逐个 key 过滤 revision 窗口。
            // 这个分支通常对应 watch 的 prefix 订阅，或者 range 订阅。
            NavigableMap<String, List<KeyValueRecord>> rangeView = historyByKey.subMap(startKey, true, resolvedEndKeyExclusive, false);
            for (Map.Entry<String, List<KeyValueRecord>> entry : rangeView.entrySet()) {
                appendHistoryRecordsInRevisionWindow(entry.getValue(),
                        normalizedStartRevisionInclusive,
                        endRevisionInclusive,
                        records);
            }
        }

        records.sort((left, right) -> {
            int revisionCompare = Long.compare(left.getModRevision(), right.getModRevision());
            if (revisionCompare != 0) {
                return revisionCompare;
            }
            return left.getKey().compareTo(right.getKey());
        });
        // TODO: Watch 的历史回放和实时事件都依赖这个顺序稳定性，因此这里显式排序，而不是依赖底层 Map 遍历顺序。
        // 这样做的目的，是让多个节点在同一 revision 区间上回放出完全一致的事件顺序。
        if (limit > 0 && records.size() > limit) {
            return new ArrayList<>(records.subList(0, limit));
        }
        return records;
    }

    /**
     * 删除单个 key。
     */
    public KeyValueDeleteResult delete(String key) {
        validateKey(key);
        return deleteRange(key, null, false, false);
    }

    /**
     * 删除多个指定 key。
     *
     * <p>该方法用于 lease revoke / lease expire 场景，保证一组 key 在同一个 revision 内被删除。</p>
     *
     * @param keys 待删除 key 列表
     * @return 删除结果
     */
    public KeyValueDeleteResult deleteKeys(List<String> keys) {
        List<KeyValueRecord> toDelete = new ArrayList<>();
        if (keys == null || keys.isEmpty()) {
            KeyValueDeleteResult result = new KeyValueDeleteResult();
            result.setDeletedCount(0);
            result.setRevision(currentRevision);
            return result;
        }

        List<String> orderedKeys = new ArrayList<>(keys);
        orderedKeys.sort(String::compareTo);
        for (String key : orderedKeys) {
            validateKey(key);
            KeyValueRecord record = getVisibleRecordByRevision(key, currentRevision);
            if (record != null) {
                toDelete.add(record);
            }
        }
        return deleteVisibleRecords(toDelete, false);
    }

    /**
     * 范围删除。
     *
     * <p>删除语义采用墓碑记录，不直接移除历史，便于历史 revision 读取和后续 compact 演进。</p>
     *
     * <p>
     * TODO:
     *  endKeyExclusive 在 deleteRange 中与 range 保持完全一致：
     *  删除区间是 [startKey, endKeyExclusive)。
     *  统一边界语义可以保证“查到什么就删什么”的行为一致性，减少读删边界不一致问题。
     * </p>
     *
     * <p>案例说明：</p>
     * <p>1) 单 key 删除：startKey="user1"，endKeyExclusive=null，prefixMatch=false -> 仅删除 user1（若可见）。</p>
     * <p>2) 区间删除：startKey="a"，endKeyExclusive="d"，prefixMatch=false -> 删除 ["a","d") 内可见 key。</p>
     * <p>3) 前缀删除：startKey="app/"，prefixMatch=true -> 删除所有以 app/ 开头的可见 key。</p>
     * <p>4) prevKv=true -> 返回删除前可见值；prevKv=false -> 不返回明细。</p>
     */
    public KeyValueDeleteResult deleteRange(String startKey, String endKeyExclusive, boolean prefixMatch, boolean prevKv) {
        // 1) 参数校验并解析范围边界。
        validateKey(startKey);
        String resolvedEndKeyExclusive = resolveEndKeyExclusive(startKey, endKeyExclusive, prefixMatch);

        // 2) 先收集当前 revision 下可见的待删除记录。
        //    注意：这里只收集“可见版本”，历史旧版本和墓碑不会被作为待删目标。
        List<KeyValueRecord> toDelete = new ArrayList<>();
        if (resolvedEndKeyExclusive == null || resolvedEndKeyExclusive.length() == 0) {
            KeyValueRecord record = getVisibleRecordByRevision(startKey, currentRevision);
            if (record != null) {
                toDelete.add(record);
            }
        } else {
            // 区间删除与 range 同语义：删除 [key, actualRangeEnd) 区间内的可见 key。
            NavigableMap<String, List<KeyValueRecord>> rangeView = historyByKey.subMap(startKey, true, resolvedEndKeyExclusive, false);
            for (Map.Entry<String, List<KeyValueRecord>> entry : rangeView.entrySet()) {
                String storedKey = entry.getKey();
                KeyValueRecord record = getVisibleRecordByRevision(storedKey, currentRevision);
                if (record != null) {
                    toDelete.add(record);
                }
            }
        }

        KeyValueDeleteResult result = new KeyValueDeleteResult();
        // 3) 无匹配记录时不推进 revision，直接返回当前 revision。
        if (toDelete.isEmpty()) {
            /**
             * TODO:
             *  deleteRange 没有命中可见 key 时，状态机不产生变更，因此不能推进全局 revision。
             *  这样可保证“只有真实变更才占用 revision”。
             */
            result.setDeletedCount(0);
            result.setRevision(currentRevision);
            return result;
        }

        // 4) 至少删除一条时推进 revision，并为每条记录追加墓碑版本。
        //    这里不会物理删除历史记录，只会追加 deleted=true 的新版本。
        long revision = nextRevision();
        for (KeyValueRecord previous : toDelete) {
            appendDeletedRecord(previous, revision);
            if (prevKv) {
                // 4.1) prevKv=true 时回传删除前可见值。
                result.getPreviousRecords().add(copyVisibleRecord(previous));
            }
        }
        result.setDeletedCount(toDelete.size());
        result.setRevision(revision);
        return result;
    }

    /**
     * 删除已收集的可见记录列表。
     *
     * <p>该方法把 delete / deleteRange / lease revoke 共用的删除逻辑收敛到一个实现。</p>
     *
     * @param toDelete 待删除的可见记录
     * @param prevKv   是否返回删除前记录
     * @return 删除结果
     */
    private KeyValueDeleteResult deleteVisibleRecords(List<KeyValueRecord> toDelete, boolean prevKv) {
        KeyValueDeleteResult result = new KeyValueDeleteResult();
        if (toDelete == null || toDelete.isEmpty()) {
            result.setDeletedCount(0);
            result.setRevision(currentRevision);
            return result;
        }

        long revision = nextRevision();
        for (KeyValueRecord previous : toDelete) {
            appendDeletedRecord(previous, revision);
            if (prevKv) {
                result.getPreviousRecords().add(copyVisibleRecord(previous));
            }
        }
        result.setDeletedCount(toDelete.size());
        result.setRevision(revision);
        return result;
    }

    /**
     * 压缩指定 revision 及其之前的历史版本。
     *
     * <p>
     * TODO:
     *  compact 的核心语义是“收缩历史窗口”而不是“产生新写版本”：
     *  1) compact 不会推进 currentRevision；
     *  2) compact 后 requestedRevision < compactRevision 的历史读必须报 compacted；
     *  3) 对每个 key 保留 <= revision 的最后可见版本作为边界（若边界是 tombstone 则该 key 可整体清理）。
     * </p>
     *
     * @param revision 压缩边界 revision
     * @return 历史记录删除条数
     */
    public int compact(long revision) {
        if (revision <= 0L) {
            throw new IllegalArgumentException("compact revision must be positive");
        }
        if (revision > currentRevision) {
            throw new IllegalArgumentException("compact revision must not be greater than current revision, revision=" + revision + ", current=" + currentRevision);
        }
        if (revision <= compactRevision) {
            throw new IllegalArgumentException("required revision has been compacted, requested=" + revision + ", compactRevision=" + compactRevision);
        }

        int removedRecordCount = 0;
        List<String> keyList = new ArrayList<>(historyByKey.keySet());
        for (String key : keyList) {
            List<KeyValueRecord> records = historyByKey.get(key);
            if (records == null || records.isEmpty()) {
                continue;
            }
            List<KeyValueRecord> compactedRecords = compactRecordsToRevision(records, revision);
            removedRecordCount += records.size() - compactedRecords.size();
            if (compactedRecords.isEmpty()) {
                historyByKey.remove(key);
            } else {
                historyByKey.put(key, compactedRecords);
            }
        }

        compactRevision = revision;
        return removedRecordCount;
    }

    /**
     * 构建状态机快照。
     *
     * <p>快照数据使用与运行态一致的 historyByKey 结构；单 key 历史顺序由写入路径保证。</p>
     */
    public KeyValueStoreSnapshot createSnapshot() {
        // 1) 固定快照 revision，作为恢复后的读边界基线。
        KeyValueStoreSnapshot snapshot = new KeyValueStoreSnapshot();
        snapshot.setRevision(currentRevision);
        snapshot.setCompactRevision(compactRevision);

        // 2) 按 key 深拷贝历史记录，避免运行态对象泄漏到快照对象中。
        for (Map.Entry<String, List<KeyValueRecord>> entry : historyByKey.entrySet()) {
            List<KeyValueRecord> copiedRecords = new ArrayList<>();
            List<KeyValueRecord> records = entry.getValue();
            if (records != null) {
                for (KeyValueRecord record : records) {
                    copiedRecords.add(copyRecord(record));
                }
            }
            snapshot.getHistoryByKey().put(entry.getKey(), copiedRecords);
        }
        return snapshot;
    }

    /**
     * 从快照恢复状态机。
     *
     * <p>恢复时会先清空当前状态，再用快照重建 historyByKey，最后重建 currentRevision。</p>
     */
    public void restoreSnapshot(KeyValueStoreSnapshot snapshot) {
        // 1) 先清空运行态，再做恢复，避免脏数据残留。
        historyByKey.clear();
        currentRevision = 0L;
        compactRevision = 0L;
        if (snapshot == null) {
            return;
        }

        // 2) 按 key 维度恢复历史记录，并逐 key 做排序兜底。
        NavigableMap<String, List<KeyValueRecord>> snapshotHistoryByKey = snapshot.getHistoryByKey();
        if (snapshotHistoryByKey != null) {
            for (Map.Entry<String, List<KeyValueRecord>> entry : snapshotHistoryByKey.entrySet()) {
                String key = entry.getKey();
                if (key == null) {
                    continue;
                }
                List<KeyValueRecord> records = entry.getValue();
                List<KeyValueRecord> restoredRecords = new ArrayList<>();
                if (records != null) {
                    for (KeyValueRecord record : records) {
                        if (record == null) {
                            continue;
                        }
                        KeyValueRecord restoredRecord = copyRecord(record);
                        if (restoredRecord.getKey() == null) {
                            restoredRecord.setKey(key);
                        }
                        restoredRecords.add(restoredRecord);
                    }
                }
                historyByKey.put(key, restoredRecords);
            }
        }

        // 3) revision 以快照声明值和历史最大 modRevision 的较大值为准。
        currentRevision = Math.max(snapshot.getRevision(), findMaxRevision());
        compactRevision = Math.max(0L, snapshot.getCompactRevision());
    }

    /**
     * 推进全局 revision。
     */
    private long nextRevision() {
        currentRevision += 1L;
        return currentRevision;
    }

    /**
     * 更新 FNV-1a 哈希（字节数组）。
     *
     * <p>
     * 先写入长度，再写入内容，避免不同输入产生同一字节拼接歧义。
     * 例如：["ab","c"] 与 ["a","bc"]，如果不写长度可能被错误视为同一串输入。
     * </p>
     *
     * <p>这里与 fnvUpdateLong / fnvUpdateString 构成最小工具集，避免过度拆分为多层小函数。</p>
     */
    private long fnvUpdateBytes(long hash, byte[] bytes) {
        if (bytes == null) {
            int nullMarker = -1;
            hash ^= ((nullMarker >>> 24) & 0xffL);
            hash *= FNV_64_PRIME;
            hash ^= ((nullMarker >>> 16) & 0xffL);
            hash *= FNV_64_PRIME;
            hash ^= ((nullMarker >>> 8) & 0xffL);
            hash *= FNV_64_PRIME;
            hash ^= (nullMarker & 0xffL);
            hash *= FNV_64_PRIME;
            return hash;
        }

        int length = bytes.length;
        hash ^= ((length >>> 24) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((length >>> 16) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((length >>> 8) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= (length & 0xffL);
        hash *= FNV_64_PRIME;

        for (byte value : bytes) {
            hash ^= (value & 0xffL);
            hash *= FNV_64_PRIME;
        }
        return hash;
    }

    /**
     * 更新 FNV-1a 哈希。
     */
    private long fnvUpdateLong(long hash, long value) {
        hash ^= ((value >>> 56) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((value >>> 48) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((value >>> 40) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((value >>> 32) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((value >>> 24) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((value >>> 16) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= ((value >>> 8) & 0xffL);
        hash *= FNV_64_PRIME;
        hash ^= (value & 0xffL);
        hash *= FNV_64_PRIME;
        return hash;
    }

    /**
     * 更新 FNV-1a 哈希。
     */
    private long fnvUpdateString(long hash, String value) {
        return fnvUpdateBytes(hash, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 解析读取 revision。
     */
    private long resolveReadRevision(long requestedRevision) {
        if (requestedRevision < 0L) {
            throw new IllegalArgumentException("revision must not be negative");
        }
        if (requestedRevision == 0L) {
            return currentRevision;
        }
        if (requestedRevision < compactRevision) {
            throw new IllegalArgumentException("required revision has been compacted, requested=" + requestedRevision + ", compactRevision=" + compactRevision);
        }
        if (requestedRevision > currentRevision) {
            throw new IllegalArgumentException("requested revision must not be greater than current revision, requested=" + requestedRevision + ", current=" + currentRevision);
        }
        return requestedRevision;
    }

    /**
     * 获取指定 key 在目标 revision 下可见的记录。
     */
    private KeyValueRecord getVisibleRecordByRevision(String key, long revision) {
        List<KeyValueRecord> records = historyByKey.get(key);
        if (records == null || records.isEmpty()) {
            return null;
        }

        // 按 modRevision 顺序查找最后一个 <= 目标 revision 的版本。
        KeyValueRecord visible = null;
        for (KeyValueRecord record : records) {
            if (record.getModRevision() > revision) {
                break;
            }
            visible = record;
        }
        // 删除墓碑对外不可见。
        if (visible == null || visible.isDeleted()) {
            /**
             * TODO:
             *  这里把 tombstone 视为“该 revision 下 key 不存在”。
             *  这正是 put 时 previousRecord 可能为 null 的关键原因：
             *  tombstone 会结束旧存活代，后续 put 将开启新存活代（createRevision 重置、version 从 1 开始）。
             */
            return null;
        }
        return copyVisibleRecord(visible);
    }

    /**
     * 追加普通记录并维持单 key 历史顺序。
     */
    private void appendRecord(KeyValueRecord record) {
        List<KeyValueRecord> records = historyByKey.get(record.getKey());
        if (records == null) {
            // 首次出现的 key 在这里创建历史列表。
            records = new ArrayList<>();
            historyByKey.put(record.getKey(), records);
        }
        // 每次写入的 modRevision 都来自单调递增全局 revision，因此单 key 历史天然有序。
        records.add(copyRecord(record));
    }

    /**
     * 追加删除墓碑记录。
     */
    private void appendDeletedRecord(KeyValueRecord previousRecord, long revision) {
        KeyValueRecord record = new KeyValueRecord();
        record.setKey(previousRecord.getKey());
        record.setValue(null);
        /**
         * TODO:
         *  删除墓碑仍属于“当前存活代内的一次变更”，因此：
         *  1) createRevision 继承旧值；
         *  2) modRevision 使用当前删除 revision；
         *  3) version 继续 +1。
         *  随后该记录会在可见性判断中被过滤，逻辑上结束当前存活代。
         */
        record.setCreateRevision(previousRecord.getCreateRevision());
        record.setModRevision(revision);
        record.setVersion(previousRecord.getVersion() + 1L);
        record.setDeleted(true);
        record.setLeaseId(previousRecord.getLeaseId());
        appendRecord(record);
    }

    /**
     * 追加 revision 区间内的历史记录（包含 tombstone）。
     */
    private void appendHistoryRecordsInRevisionWindow(List<KeyValueRecord> sourceRecords, long startRevisionInclusive, long endRevisionInclusive, List<KeyValueRecord> target) {
        if (sourceRecords == null || sourceRecords.isEmpty()) {
            return;
        }
        for (KeyValueRecord record : sourceRecords) {
            long modRevision = record.getModRevision();
            if (modRevision < startRevisionInclusive) {
                continue;
            }
            if (modRevision > endRevisionInclusive) {
                break;
            }
            target.add(copyRecord(record));
        }
    }

    /**
     * 解析实际 rangeEnd。
     *
     * <p>作用：把“单 key / 区间 / 前缀”三种调用方式统一成区间边界。</p>
     *
     * <p>
     * TODO:
     *  该方法返回的 endKeyExclusive 用于构造最终区间 [startKey, endKeyExclusive)：
     *  1) prefixMatch=false：直接使用调用方给的 endKeyExclusive；
     *  2) prefixMatch=true：根据 startKey 计算前缀上界，转成区间查询；
     *  3) endKeyExclusive 为空：调用方按单 key 处理。
     * </p>
     *
     * <p>案例说明：</p>
     * <p>1) prefixMatch=false 且 endKeyExclusive="d"：返回 "d"，最终查询 [startKey,"d")。</p>
     * <p>2) prefixMatch=false 且 endKeyExclusive=null：返回 null，调用方按单 key 处理。</p>
     * <p>3) prefixMatch=true 且 startKey="app/"：返回 computePrefixEndKeyExclusive("app/")，最终查询 [ "app/", computePrefixEndKeyExclusive("app/") )。</p>
     */
    private String resolveEndKeyExclusive(String startKey, String endKeyExclusive, boolean prefixMatch) {
        if (prefixMatch) {
            return computePrefixEndKeyExclusive(startKey);
        }
        return endKeyExclusive;
    }

    /**
     * 计算前缀查询/删除的右边界。
     *
     * <p>该方法把“前缀匹配”转换成“左闭右开区间”：</p>
     * <p>prefix = p，右边界 = nextPrefixEnd(p)，则所有满足 p 开头的 key 都会落在 [p, nextPrefixEnd(p))。</p>
     *
     * <p>
     * TODO:
     *  为什么是 prefix="app/" -> prefixEnd="app0"：
     *  1) 字典序比较中，'/' 的编码值小于 '0'，因此所有 "app/xxx" 都满足 "app/xxx" < "app0"；
     *  2) 同时 "app/xxx" 也都满足 >= "app/"；
     *  3) 所以 [ "app/", "app0" ) 正好覆盖 app/ 前缀空间。
     *  这就是 etcd 前缀查询转换成区间查询的核心设计：不做全量 startsWith 扫描，而是在有序 key 空间做连续区间扫描。
     * </p>
     *
     * <p>案例：</p>
     * <p>prefix="app/"，nextPrefixEnd="app0"（示意）。</p>
     * <p>则 "app/a"、"app/b" 命中；"apq/x" 不命中。</p>
     */
    private String computePrefixEndKeyExclusive(String prefixKey) {
        /**
         * TODO:
         *  该算法与 etcd client/v3 的 GetPrefixRangeEnd 设计一致（从后向前找第一个可+1字节并截断尾部）：
         *  1) 前缀查询本质是区间查询 [prefix, prefixEnd)；
         *  2) prefixEnd 必须是“刚好大于所有 prefix* 字符串”的最小上界；
         *  3) 这样就不需要目录树，也能在有序 key 空间上高效前缀扫描。
         *
         *  示例：
         *  prefix="app/" -> prefixEnd="app0"
         *  命中："app/a"、"app/b"（都满足 >= "app/" 且 < "app0"）
         *  不命中："apq/x"（"apq/x" > "app0"）
         */
        if (prefixKey == null || prefixKey.length() == 0) {
            return prefixKey;
        }

        // 把字符串看成“可比较的字符序列”，后续按“从低位到高位进位”的思路求最小上界。
        char[] chars = prefixKey.toCharArray();

        /**
         * TODO:
         *  该 for 循环是“从后向前找可进位位点”的过程（类似十进制加法从个位往前进位）：
         *  1) 从最后一位开始扫描，优先尝试对最低位 +1，得到尽可能小的上界；
         *  2) 如果某一位已经是 MAX_VALUE，说明这位无法再 +1，需要继续向前找可进位位；
         *  3) 找到第一位可进位位后，该位 +1，并把其后的尾部全部截断；
         *  4) 截断尾部的原因：要构造“严格大于 prefixKey 且尽可能小”的 endKeyExclusive。
         *
         *  示例1：
         *  prefixKey = "app/"，最后一位 '/' 可进位为 '0'，返回 "app0"。
         *  示例2：
         *  prefixKey = "ab\\uFFFF"，最后一位不可进位，向前到 'b' 进位为 'c'，返回 "ac"（尾部截断）。
         */
        for (int index = chars.length - 1; index >= 0; index--) {
            if (chars[index] == Character.MAX_VALUE) {
                // 当前位已到上限，无法 +1，继续向更高位寻找可进位位。
                continue;
            }

            // 在首个可进位位上 +1。
            chars[index] = (char) (chars[index] + 1);

            // 返回 [0, index] 前缀（含进位位），截断尾部，得到“最小严格上界”。
            return new String(chars, 0, index + 1);
        }

        // 全部字符都已经到最大值时，沿用当前实现的兜底策略，返回原前缀（调用方会退化为单 key 路径）。
        return prefixKey;
    }

    /**
     * 复制范围读取结果。
     */
    private KeyValueRecord copyForRangeResult(KeyValueRecord record, boolean keysOnly) {
        KeyValueRecord copy = copyVisibleRecord(record);
        if (copy != null && keysOnly) {
            copy.setValue(null);
        }
        return copy;
    }

    /**
     * 压缩单 key 历史列表到指定 revision。
     *
     * <p>
     * TODO:
     *  算法分两段：
     *  1) 先筛出 > revision 的记录原样保留；
     *  2) 再从 <= revision 区间取“最后一条记录”作为边界锚点：
     *     - 边界锚点可见（非 tombstone）时保留；
     *     - 边界锚点为 tombstone 时，该 key 在压缩边界前已经删除，故不保留锚点。
     * </p>
     */
    private List<KeyValueRecord> compactRecordsToRevision(List<KeyValueRecord> records, long revision) {
        List<KeyValueRecord> compactedRecords = new ArrayList<>();
        KeyValueRecord latestRecordBeforeOrAtRevision = null;

        for (KeyValueRecord record : records) {
            if (record.getModRevision() <= revision) {
                latestRecordBeforeOrAtRevision = record;
                continue;
            }
            compactedRecords.add(copyRecord(record));
        }

        if (latestRecordBeforeOrAtRevision != null && !latestRecordBeforeOrAtRevision.isDeleted()) {
            compactedRecords.add(0, copyRecord(latestRecordBeforeOrAtRevision));
        }
        return compactedRecords;
    }

    /**
     * 计算历史记录中的最大 revision。
     */
    private long findMaxRevision() {
        long maxRevision = 0L;
        for (Map.Entry<String, List<KeyValueRecord>> entry : historyByKey.entrySet()) {
            for (KeyValueRecord record : entry.getValue()) {
                maxRevision = Math.max(maxRevision, record.getModRevision());
            }
        }
        return maxRevision;
    }

    /**
     * 校验 key。
     */
    private void validateKey(String key) {
        if (key == null || key.trim().length() == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }
    }

    /**
     * 复制可见记录。
     */
    private KeyValueRecord copyVisibleRecord(KeyValueRecord source) {
        if (source == null || source.isDeleted()) {
            return null;
        }
        KeyValueRecord target = copyRecord(source);
        target.setDeleted(false);
        return target;
    }

    /**
     * 深拷贝记录对象，避免运行态对象被外部修改。
     */
    private KeyValueRecord copyRecord(KeyValueRecord source) {
        if (source == null) {
            return null;
        }
        KeyValueRecord target = new KeyValueRecord();
        target.setKey(source.getKey());
        target.setValue(source.getValue());
        target.setCreateRevision(source.getCreateRevision());
        target.setModRevision(source.getModRevision());
        target.setVersion(source.getVersion());
        target.setDeleted(source.isDeleted());
        target.setLeaseId(source.getLeaseId());
        return target;
    }
}
