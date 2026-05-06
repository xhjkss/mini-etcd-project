package com.xhj.etcd.kernel.raft.event;

import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesResponse;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotRequest;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotResponse;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteRequest;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteResponse;
import com.xhj.etcd.serializer.Serializer;

/**
 * RaftEventCodec
 *
 * @author XJks
 * @description Raft 事件编解码器，负责在具体事件数据对象和 RaftEvent 事件信封之间转换。
 *
 * <p>TODO: RaftEvent 只保存事件类型、事件 ID 和序列化后的事件数据；具体事件数据类型由本类根据调用入口显式指定，不由 RaftEvent 自己判断。</p>
 */
public class RaftEventCodec {

    /**
     * 事件数据序列化器。
     *
     * <p>用于将 Propose、Raft RPC、Advance、CreateSnapshot 等事件数据序列化为字节数组，
     * 也用于从 RaftEvent.eventData 反序列化回具体事件对象。</p>
     */
    private final Serializer serializer;

    public RaftEventCodec(Serializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        this.serializer = serializer;
    }

    // ==================== Encode event ====================

    /**
     * 编码通用 Raft 事件。
     *
     * <p>该方法是所有事件编码入口的统一实现。
     * 需要返回处理结果的事件可以传入 eventId；不需要关联结果的内部事件可以传入 null。</p>
     *
     * @param type      事件类型
     * @param eventId   事件 ID
     * @param eventData 事件数据对象
     * @return Raft 事件信封
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, String eventId, Object eventData) {
        RaftEvent event = new RaftEvent();
        event.setType(type);
        event.setEventId(eventId);
        event.setEventData(serializer.serialize(eventData));
        return event;
    }

    /**
     * 编码 Propose 事件。
     *
     * <p>Propose 通常由上层提交写命令触发，需要保留 eventId，
     * 以便调用方等待 Raft 事件循环返回提案结果。</p>
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, String eventId, RaftProposeEventData eventData) {
        return encodeRaftEvent(type, eventId, (Object) eventData);
    }

    /**
     * 编码 RequestVoteRequest 事件。
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, RequestVoteRequest request) {
        return encodeRaftEvent(type, null, request);
    }

    /**
     * 编码 RequestVoteResponse 事件。
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, RequestVoteResponse response) {
        return encodeRaftEvent(type, null, response);
    }

    /**
     * 编码 AppendEntriesRequest 事件。
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, AppendEntriesRequest request) {
        return encodeRaftEvent(type, null, request);
    }

    /**
     * 编码 AppendEntriesResponse 事件。
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, AppendEntriesResponse response) {
        return encodeRaftEvent(type, null, response);
    }

    /**
     * 编码 InstallSnapshotRequest 事件。
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, InstallSnapshotRequest request) {
        return encodeRaftEvent(type, null, request);
    }

    /**
     * 编码 InstallSnapshotResponse 事件。
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, InstallSnapshotResponse response) {
        return encodeRaftEvent(type, null, response);
    }

    /**
     * 编码 Advance 事件。
     *
     * <p>Advance 事件表示上层已经处理完成某个 RaftReady，
     * RaftNode 收到后可以清理对应的 pending 状态。</p>
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, RaftAdvanceEventData eventData) {
        return encodeRaftEvent(type, null, eventData);
    }

    /**
     * 编码 CreateSnapshot 事件。
     *
     * <p>该事件由上层状态机生成快照后提交给 RaftNode，
     * RaftNode 根据 lastIncludedIndex 压缩对应范围内的日志。</p>
     */
    public RaftEvent encodeRaftEvent(RaftEventType type, RaftCreateSnapshotEventData eventData) {
        return encodeRaftEvent(type, null, eventData);
    }

    // ==================== Decode event ====================

    /**
     * 解码 Propose 事件数据。
     */
    public RaftProposeEventData decodeRaftProposeEventData(RaftEvent event) {
        return decodeEventData(event, RaftProposeEventData.class, new RaftProposeEventData());
    }

    /**
     * 解码 RequestVoteRequest。
     */
    public RequestVoteRequest decodeRequestVoteRequest(RaftEvent event) {
        return decodeEventData(event, RequestVoteRequest.class, new RequestVoteRequest());
    }

    /**
     * 解码 RequestVoteResponse。
     */
    public RequestVoteResponse decodeRequestVoteResponse(RaftEvent event) {
        return decodeEventData(event, RequestVoteResponse.class, new RequestVoteResponse());
    }

    /**
     * 解码 AppendEntriesRequest。
     */
    public AppendEntriesRequest decodeAppendEntriesRequest(RaftEvent event) {
        return decodeEventData(event, AppendEntriesRequest.class, new AppendEntriesRequest());
    }

    /**
     * 解码 AppendEntriesResponse。
     */
    public AppendEntriesResponse decodeAppendEntriesResponse(RaftEvent event) {
        return decodeEventData(event, AppendEntriesResponse.class, new AppendEntriesResponse());
    }

    /**
     * 解码 InstallSnapshotRequest。
     */
    public InstallSnapshotRequest decodeInstallSnapshotRequest(RaftEvent event) {
        return decodeEventData(event, InstallSnapshotRequest.class, new InstallSnapshotRequest());
    }

    /**
     * 解码 InstallSnapshotResponse。
     */
    public InstallSnapshotResponse decodeInstallSnapshotResponse(RaftEvent event) {
        return decodeEventData(event, InstallSnapshotResponse.class, new InstallSnapshotResponse());
    }

    /**
     * 解码 Advance 事件数据。
     *
     * <p>该数据用于通知 RaftNode：上层已经完成对应 RaftReady 的处理，可以推进 Ready 生命周期。</p>
     */
    public RaftAdvanceEventData decodeRaftAdvanceEventData(RaftEvent event) {
        return decodeEventData(event, RaftAdvanceEventData.class, new RaftAdvanceEventData());
    }

    /**
     * 解码 CreateSnapshot 事件数据。
     *
     * <p>该数据包含状态机快照字节和快照覆盖的最后日志索引。</p>
     */
    public RaftCreateSnapshotEventData decodeRaftCreateSnapshotEventData(RaftEvent event) {
        return decodeEventData(event, RaftCreateSnapshotEventData.class, new RaftCreateSnapshotEventData());
    }

    // ==================== Internal helpers ====================

    /**
     * 解码事件数据。
     *
     * <p>TODO: eventData 为空时返回调用方传入的空对象，保持事件循环处理空载荷事件时的容错语义；
     * eventData 不为空时，则按调用方指定的事件数据类型反序列化。</p>
     *
     * @param event          Raft 事件信封
     * @param eventDataClass 事件数据类型
     * @param emptyEventData 空载荷时返回的默认对象
     * @param <T>            事件数据类型
     * @return 事件数据对象
     */
    private <T> T decodeEventData(RaftEvent event, Class<T> eventDataClass, T emptyEventData) {
        if (event == null || event.getEventData() == null) {
            return emptyEventData;
        }
        return serializer.deserialize(event.getEventData(), eventDataClass);
    }
}