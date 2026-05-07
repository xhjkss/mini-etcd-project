package com.xhj.etcd.kernel.raft.event;

import com.xhj.etcd.kernel.raft.core.RaftReady;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesResponse;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotRequest;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotResponse;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteRequest;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteResponse;
import lombok.NoArgsConstructor;

/**
 * RaftEventCodec
 *
 * @author XJks
 * @description Raft 内部事件辅助器，负责创建 RaftEvent，并按事件类型安全读取 data 对象。
 *
 * <p>当前 RaftEvent 只在 JVM 内部事件队列流转，因此不需要序列化事件 data。</p>
 */
@NoArgsConstructor
public class RaftEventCodec {

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
        event.setData(eventData);
        return event;
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, RequestVoteRequest request) {
        return encodeRaftEvent(type, null, request);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, RequestVoteResponse response) {
        return encodeRaftEvent(type, null, response);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, AppendEntriesRequest request) {
        return encodeRaftEvent(type, null, request);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, AppendEntriesResponse response) {
        return encodeRaftEvent(type, null, response);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, InstallSnapshotRequest request) {
        return encodeRaftEvent(type, null, request);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, InstallSnapshotResponse response) {
        return encodeRaftEvent(type, null, response);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, RaftReady ready) {
        return encodeRaftEvent(type, null, ready);
    }

    public RaftEvent encodeRaftEvent(RaftEventType type, RaftCreateSnapshotEventData eventData) {
        return encodeRaftEvent(type, null, eventData);
    }

    // ==================== Decode event data ====================

    public byte[] decodeProposeCommandData(RaftEvent event) {
        return decodeEventData(event, RaftEventType.PROPOSE, byte[].class);
    }

    public RequestVoteRequest decodeRequestVoteRequest(RaftEvent event) {
        return decodeEventData(event, RaftEventType.REQUEST_VOTE, RequestVoteRequest.class);
    }

    public RequestVoteResponse decodeRequestVoteResponse(RaftEvent event) {
        return decodeEventData(event, RaftEventType.REQUEST_VOTE_RESPONSE, RequestVoteResponse.class);
    }

    public AppendEntriesRequest decodeAppendEntriesRequest(RaftEvent event) {
        return decodeEventData(event, RaftEventType.APPEND_ENTRIES, AppendEntriesRequest.class);
    }

    public AppendEntriesResponse decodeAppendEntriesResponse(RaftEvent event) {
        return decodeEventData(event, RaftEventType.APPEND_ENTRIES_RESPONSE, AppendEntriesResponse.class);
    }

    public InstallSnapshotRequest decodeInstallSnapshotRequest(RaftEvent event) {
        return decodeEventData(event, RaftEventType.INSTALL_SNAPSHOT, InstallSnapshotRequest.class);
    }

    public InstallSnapshotResponse decodeInstallSnapshotResponse(RaftEvent event) {
        return decodeEventData(event, RaftEventType.INSTALL_SNAPSHOT_RESPONSE, InstallSnapshotResponse.class);
    }

    public RaftReady decodeRaftReady(RaftEvent event) {
        return decodeEventData(event, RaftEventType.ADVANCE, RaftReady.class);
    }

    public RaftCreateSnapshotEventData decodeRaftCreateSnapshotEventData(RaftEvent event) {
        return decodeEventData(event, RaftEventType.CREATE_SNAPSHOT, RaftCreateSnapshotEventData.class);
    }

    // ==================== Internal helpers ====================

    public <T> T decodeEventData(RaftEvent event, RaftEventType expectedType, Class<T> dataClass) {
        if (event == null) {
            throw new IllegalArgumentException("raft event must not be null");
        }
        if (event.getType() != expectedType) {
            throw new IllegalArgumentException("unexpected raft event type, expected="
                    + expectedType + ", actual=" + event.getType());
        }
        Object data = event.getData();
        if (data == null) {
            return null;
        }
        if (!dataClass.isInstance(data)) {
            throw new IllegalArgumentException("unexpected raft event data type, expected="
                    + dataClass.getName() + ", actual=" + data.getClass().getName());
        }
        return dataClass.cast(data);
    }
}