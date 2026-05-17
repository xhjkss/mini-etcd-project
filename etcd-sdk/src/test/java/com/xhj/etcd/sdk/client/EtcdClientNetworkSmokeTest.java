package com.xhj.etcd.sdk.client;

import com.xhj.etcd.sdk.client.watch.WatchHandle;
import com.xhj.etcd.sdk.client.watch.WatchListener;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchEventView;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcServer;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * EtcdClientNetworkSmokeTest
 *
 * @author XJks
 * @description SDK 轻量真实网络 smoke 测试，验证 put/get/watch subscribe/cancel 基础链路。
 */
public class EtcdClientNetworkSmokeTest {

    /**
     * 单个 smoke 用例总超时，单位：毫秒。
     */
    private static final long TEST_TIMEOUT_MILLIS = 10000L;

    @Test
    public void shouldSupportPutGetAndWatchOnRealNetwork() throws Exception {
        MiniEtcdCluster cluster = new MiniEtcdCluster();
        cluster.startThreeNodeCluster();

        EtcdClient client = new EtcdClient(cluster.getAllEndpoints());
        WatchHandle handle = null;
        try {
            awaitClusterReady(client, TEST_TIMEOUT_MILLIS);

            String watchKey = "smoke/watch/key";
            String watchValue = "value-1";

            CountDownLatch subscribedLatch = new CountDownLatch(1);
            CountDownLatch notificationLatch = new CountDownLatch(1);
            CountDownLatch canceledLatch = new CountDownLatch(1);
            AtomicReference<Throwable> listenerError = new AtomicReference<>();
            AtomicReference<WatchEventView> eventViewRef = new AtomicReference<>();

            WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
            subscribeRequest.setStartKey(watchKey);
            subscribeRequest.setPrefixMatch(false);
            subscribeRequest.setStartRevision(0L);
            subscribeRequest.setMaxEvents(16);

            handle = client.watch(subscribeRequest, new WatchListener() {
                @Override
                public void onSubscribed(WatchSubscribeResponse response) {
                    subscribedLatch.countDown();
                }

                @Override
                public void onNotification(WatchNotification response) {
                    if (response == null || response.getEvents() == null) {
                        return;
                    }
                    for (WatchEventView eventView : response.getEvents()) {
                        if (eventView != null
                                && eventView.getKeyValue() != null
                                && watchKey.equals(eventView.getKeyValue().getKey())) {
                            eventViewRef.compareAndSet(null, eventView);
                            notificationLatch.countDown();
                            return;
                        }
                    }
                }

                @Override
                public void onCanceled(WatchCancelResponse response) {
                    canceledLatch.countDown();
                }

                @Override
                public void onError(Throwable cause) {
                    listenerError.compareAndSet(null, cause);
                }
            });

            Assert.assertTrue("watch subscribe ack timeout", subscribedLatch.await(5L, TimeUnit.SECONDS));

            PutResponse putResponse = client.put(new PutRequest(watchKey, watchValue));
            Assert.assertNotNull("put response must not be null", putResponse);
            Assert.assertTrue("put revision must be positive", putResponse.getRevision() > 0L);

            GetResponse getResponse = client.get(new GetRequest(watchKey));
            Assert.assertNotNull("get response must not be null", getResponse);
            Assert.assertEquals("get value mismatch", watchValue, getResponse.getValue());

            Assert.assertTrue("watch notification timeout", notificationLatch.await(5L, TimeUnit.SECONDS));
            Assert.assertNull("watch listener should not report error", listenerError.get());
            Assert.assertNotNull("watch event should be captured", eventViewRef.get());
            Assert.assertNotNull("watch event keyValue must not be null", eventViewRef.get().getKeyValue());
            Assert.assertEquals("watch event key mismatch", watchKey, eventViewRef.get().getKeyValue().getKey());

            handle.cancel();
            Assert.assertTrue("watch cancel ack timeout", canceledLatch.await(5L, TimeUnit.SECONDS));
            Assert.assertTrue("watch handle should be closed", handle.isClosed());
            handle = null;
        } finally {
            if (handle != null) {
                handle.close();
            }
            client.close();
            cluster.close();
        }
    }

    /**
     * 等待集群 leader 可用。
     */
    private void awaitClusterReady(EtcdClient client, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                PutResponse response = client.put(new PutRequest("smoke/probe", "ready"));
                if (response != null && response.getRevision() > 0L) {
                    return;
                }
            } catch (Exception exception) {
                lastException = exception;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("cluster is not ready for sdk smoke test", lastException);
    }

    /**
     * MiniEtcdCluster
     *
     * @author XJks
     * @description SDK smoke 场景使用的最小三节点 Etcd+Raft+RPC 运行时。
     */
    private static class MiniEtcdCluster {

        private static final long RPC_TIMEOUT_MILLIS = 5000L;

        private final Serializer serializer = SerializerRegistry.getDefaultSerializer();

        private final Map<String, NodeRuntime> runtimeByNodeId = new LinkedHashMap<>();

        void startThreeNodeCluster() throws Exception {
            if (!runtimeByNodeId.isEmpty()) {
                throw new IllegalStateException("cluster already started");
            }

            List<String> nodeIds = new ArrayList<>();
            nodeIds.add("n1");
            nodeIds.add("n2");
            nodeIds.add("n3");

            for (String nodeId : nodeIds) {
                NodeEndpoint endpoint = new NodeEndpoint(nodeId, "127.0.0.1", findFreePort());
                List<String> peerNodeIds = new ArrayList<>(nodeIds);
                peerNodeIds.remove(nodeId);

                RaftConfig raftConfig = new RaftConfig();
                raftConfig.setElectionTimeoutTicks(10);
                raftConfig.setHeartbeatTimeoutTicks(3);
                raftConfig.setSnapshotTriggerLogCount(50);
                raftConfig.setPeerNodeIds(peerNodeIds);

                runtimeByNodeId.put(nodeId, new NodeRuntime(endpoint, raftConfig));
            }

            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                runtime.start(serializer);
            }
            registerAllEndpoints();
        }

        List<NodeEndpoint> getAllEndpoints() {
            List<NodeEndpoint> endpoints = new ArrayList<>();
            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                endpoints.add(runtime.endpoint);
            }
            return endpoints;
        }

        void close() {
            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                runtime.stop();
            }
        }

        private void registerAllEndpoints() {
            for (NodeRuntime source : runtimeByNodeId.values()) {
                if (source.node == null) {
                    continue;
                }
                for (NodeRuntime target : runtimeByNodeId.values()) {
                    source.node.registerNodeEndpoint(target.endpoint);
                }
            }
        }

        private static int findFreePort() throws Exception {
            ServerSocket socket = new ServerSocket(0);
            try {
                return socket.getLocalPort();
            } finally {
                socket.close();
            }
        }

        private static class NodeRuntime {

            private final NodeEndpoint endpoint;
            private final Storage storage;
            private final RaftConfig raftConfig;

            private RpcClient raftRpcClient;
            private EtcdNode node;
            private NettyRpcServer rpcServer;

            private NodeRuntime(NodeEndpoint endpoint, RaftConfig raftConfig) {
                this.endpoint = endpoint;
                this.raftConfig = raftConfig;
                this.storage = new MemoryStorage();
            }

            private void start(Serializer serializer) {
                raftRpcClient = new NettyRpcClient(serializer, RPC_TIMEOUT_MILLIS);
                node = new EtcdNode(endpoint.getNodeId(), raftConfig, storage, serializer, raftRpcClient);
                rpcServer = new NettyRpcServer(endpoint, serializer);
                rpcServer.registerService(EtcdNode.RPC_SERVICE_NAME,
                        node,
                        EtcdNode.HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_WATCH_SUBSCRIBE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_WATCH_CANCEL_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_RESPONSE_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_RESPONSE_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_RESPONSE_METHOD_NAME);
                rpcServer.start();
                node.start();
            }

            private void stop() {
                if (node != null) {
                    node.stop();
                }
                if (rpcServer != null) {
                    rpcServer.stop();
                }
                if (raftRpcClient != null) {
                    raftRpcClient.shutdown();
                }
            }
        }
    }
}
