package com.xhj.etcd.kernel.etcd.event;

/**
 * EtcdEventCodec
 *
 * @author XJks
 * @description EtcdNode 内部事件辅助器，负责创建 EtcdEvent，并按事件类型安全读取 data 对象。
 *
 * <p>当前 EtcdEvent 只在 JVM 内部队列流转，因此不需要序列化和反序列化。
 * 该类保留 Codec 命名，是为了和 EtcdCommandCodec / RaftEventCodec 的“信封 + 类型校验 + 数据读取”模式保持一致。</p>
 */
public class EtcdEventCodec {

    // ==================== Encode event ====================

    /**
     * 构造 Etcd 事件信封。
     *
     * <p>RPC handler 收到 XxxRequest 后，只负责调用该方法生成事件并投递到 etcdEventQueue；
     * 是否需要进入 Raft，由 etcd-event-loop 处理事件时统一判断。</p>
     *
     * @param type    事件类型
     * @param eventId 事件 ID
     * @param data    事件数据对象
     * @return Etcd 事件信封
     */
    public EtcdEvent encodeEtcdEvent(EtcdEventType type, String eventId, Object data) {
        EtcdEvent event = new EtcdEvent();
        event.setType(type);
        event.setEventId(eventId);
        event.setData(data);
        return event;
    }

    // ==================== Decode event data ====================

    /**
     * 读取并校验事件数据对象。
     *
     * <p>Object data 可以减少 XxxEventData 套壳，但不能在业务代码中到处裸强转。
     * 所有事件数据读取都通过该方法完成，集中校验事件类型和 data 类型。</p>
     *
     * @param event        Etcd 事件信封
     * @param expectedType 期望事件类型
     * @param dataClass    期望数据类型
     * @param <T>          事件数据类型
     * @return 类型安全的事件数据对象
     */
    public <T> T decodeEtcdEventData(EtcdEvent event, EtcdEventType expectedType, Class<T> dataClass) {
        assertEtcdEventType(event, expectedType);
        Object data = event.getData();
        if (data == null) {
            return null;
        }
        if (!dataClass.isInstance(data)) {
            throw new IllegalArgumentException("unexpected etcd event data type, expected=" + dataClass.getName() + ", actual=" + data.getClass().getName());
        }
        return dataClass.cast(data);
    }

    // ==================== Type check ====================

    private void assertEtcdEventType(EtcdEvent event, EtcdEventType expectedType) {
        if (event == null) {
            throw new IllegalArgumentException("etcd event must not be null");
        }
        if (event.getType() != expectedType) {
            throw new IllegalArgumentException("unexpected etcd event type, expected=" + expectedType + ", actual=" + event.getType());
        }
    }
}
