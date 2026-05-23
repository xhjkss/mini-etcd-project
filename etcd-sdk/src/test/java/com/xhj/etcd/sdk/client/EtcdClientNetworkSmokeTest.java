package com.xhj.etcd.sdk.client;

import com.xhj.etcd.sdk.client.lease.LeaseHandle;
import com.xhj.etcd.sdk.client.watch.WatchHandle;
import com.xhj.etcd.sdk.client.watch.WatchListener;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
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
import java.util.concurrent.atomic.AtomicLong;
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
            Assert.assertTrue("watchId should be assigned by server after subscribe ACK", handle.getWatchId() > 0L);

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

            handle.close();
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

    @Test
    public void shouldKeepSubscribeBeforeFirstNotificationUnderConcurrentPutInterleaving() throws Exception {
        MiniEtcdCluster cluster = new MiniEtcdCluster();
        cluster.startThreeNodeCluster();

        EtcdClient client = new EtcdClient(cluster.getAllEndpoints());
        try {
            awaitClusterReady(client, TEST_TIMEOUT_MILLIS);

            for (int round = 0; round < 20; round++) {
                String probeKey = "smoke/watch/order/probe/" + round;
                long baseRevision = client.put(new PutRequest(probeKey, "probe-" + round)).getRevision();

                String watchKey = "smoke/watch/order/key/" + round;
                String watchValue = "value-" + round;
                CountDownLatch subscribedLatch = new CountDownLatch(1);
                CountDownLatch notificationLatch = new CountDownLatch(1);
                AtomicLong subscribedNanoTime = new AtomicLong(0L);
                AtomicLong firstNotificationNanoTime = new AtomicLong(0L);
                AtomicReference<Throwable> listenerError = new AtomicReference<>();
                AtomicReference<WatchHandle> watchHandleRef = new AtomicReference<>();

                WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
                subscribeRequest.setStartKey(watchKey);
                subscribeRequest.setPrefixMatch(false);
                subscribeRequest.setStartRevision(baseRevision + 1L);
                subscribeRequest.setMaxEvents(16);

                Thread watchThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            watchHandleRef.set(client.watch(subscribeRequest, new WatchListener() {
                                @Override
                                public void onSubscribed(WatchSubscribeResponse response) {
                                    subscribedNanoTime.compareAndSet(0L, System.nanoTime());
                                    subscribedLatch.countDown();
                                }

                                @Override
                                public void onNotification(WatchNotification response) {
                                    if (response == null || response.getEvents() == null) {
                                        return;
                                    }
                                    for (WatchEventView eventView : response.getEvents()) {
                                        if (eventView == null
                                                || eventView.getKeyValue() == null
                                                || !watchKey.equals(eventView.getKeyValue().getKey())) {
                                            continue;
                                        }
                                        firstNotificationNanoTime.compareAndSet(0L, System.nanoTime());
                                        notificationLatch.countDown();
                                        return;
                                    }
                                }

                                @Override
                                public void onCanceled(WatchCancelResponse response) {
                                }

                                @Override
                                public void onError(Throwable cause) {
                                    listenerError.compareAndSet(null, cause);
                                }
                            }));
                        } catch (Throwable throwable) {
                            listenerError.compareAndSet(null, throwable);
                        }
                    }
                }, "sdk-watch-order-round-" + round);
                watchThread.start();

                Thread.sleep(5L);
                client.put(new PutRequest(watchKey, watchValue));

                watchThread.join(5000L);
                Assert.assertFalse("watch subscribe thread should complete, round=" + round, watchThread.isAlive());
                Assert.assertTrue("watch subscribe ack timeout, round=" + round, subscribedLatch.await(5L, TimeUnit.SECONDS));
                Assert.assertTrue("watch notification timeout, round=" + round, notificationLatch.await(5L, TimeUnit.SECONDS));
                Assert.assertNull("watch listener should not report error, round=" + round, listenerError.get());
                Assert.assertTrue("subscribe nano time should be recorded, round=" + round, subscribedNanoTime.get() > 0L);
                Assert.assertTrue("notification nano time should be recorded, round=" + round, firstNotificationNanoTime.get() > 0L);
                Assert.assertTrue("notification should not happen before subscribed callback, round=" + round,
                        subscribedNanoTime.get() <= firstNotificationNanoTime.get());

                WatchHandle watchHandle = watchHandleRef.get();
                if (watchHandle != null) {
                    Assert.assertTrue("watchId should be assigned by server, round=" + round, watchHandle.getWatchId() > 0L);
                    watchHandle.close();
                }
            }
        } finally {
            client.close();
            cluster.close();
        }
    }

    @Test
    public void shouldSupportLeaseHandleAutoKeepAliveAndStopKeepAliveOnClose() throws Exception {
        MiniEtcdCluster cluster = new MiniEtcdCluster();
        cluster.startThreeNodeCluster();

        EtcdClient client = new EtcdClient(cluster.getAllEndpoints());
        LeaseHandle leaseHandle = null;
        try {
            awaitClusterReady(client, TEST_TIMEOUT_MILLIS);

            LeaseGrantResponse leaseGrantResponse = client.leaseGrant(new LeaseGrantRequest(0L, 3L));
            Assert.assertNotNull("lease grant response must not be null", leaseGrantResponse);
            Assert.assertNotNull("lease grant lease view must not be null", leaseGrantResponse.getLease());
            long leaseId = leaseGrantResponse.getLease().getLeaseId();
            Assert.assertTrue("leaseId must be positive", leaseId > 0L);

            String leaseKey = "smoke/lease/auto/key";
            client.put(new PutRequest(leaseKey, "v1", leaseId));

            leaseHandle = client.startLeaseKeepAlive(new LeaseKeepAliveRequest(leaseId));
            Assert.assertNotNull("lease handle must not be null", leaseHandle);
            Assert.assertFalse("lease handle should be active", leaseHandle.isClosed());
            Assert.assertEquals("lease handle leaseId mismatch", leaseId, leaseHandle.getLeaseId());

            Thread.sleep(4500L);
            LeaseTtlResponse activeLeaseTtlResponse = client.leaseTtl(new LeaseTtlRequest(leaseId));
            Assert.assertNotNull("lease ttl response must not be null while keepAlive running", activeLeaseTtlResponse);
            Assert.assertNotNull("lease ttl lease view must not be null while keepAlive running", activeLeaseTtlResponse.getLease());
            Assert.assertTrue("remaining ttl should be positive while keepAlive running", activeLeaseTtlResponse.getLease().getRemainingSeconds() > 0L);

            leaseHandle.close();
            Assert.assertTrue("lease handle should be closed", leaseHandle.isClosed());

            waitUntilLeaseKeyDeleted(client, leaseKey, 12000L);
            GetResponse afterExpiredGetResponse = client.get(new GetRequest(leaseKey, false));
            Assert.assertNotNull("get response must not be null", afterExpiredGetResponse);
            Assert.assertNull("lease key should be deleted after keepAlive closed and lease expired", afterExpiredGetResponse.getValue());
            leaseHandle = null;
        } finally {
            if (leaseHandle != null) {
                leaseHandle.close();
            }
            client.close();
            cluster.close();
        }
    }

    @Test
    public void shouldClosePreviousLeaseHandleWhenStartingAutoKeepAliveOnSameLeaseId() throws Exception {
        MiniEtcdCluster cluster = new MiniEtcdCluster();
        cluster.startThreeNodeCluster();

        EtcdClient client = new EtcdClient(cluster.getAllEndpoints());
        LeaseHandle firstLeaseHandle = null;
        LeaseHandle secondLeaseHandle = null;
        try {
            awaitClusterReady(client, TEST_TIMEOUT_MILLIS);

            LeaseGrantResponse leaseGrantResponse = client.leaseGrant(new LeaseGrantRequest(0L, 5L));
            Assert.assertNotNull("lease grant response must not be null", leaseGrantResponse);
            Assert.assertNotNull("lease view must not be null", leaseGrantResponse.getLease());
            long leaseId = leaseGrantResponse.getLease().getLeaseId();
            Assert.assertTrue("leaseId must be positive", leaseId > 0L);

            firstLeaseHandle = client.startLeaseKeepAlive(new LeaseKeepAliveRequest(leaseId));
            Assert.assertNotNull("first lease handle must not be null", firstLeaseHandle);
            Assert.assertFalse("first lease handle should be active", firstLeaseHandle.isClosed());

            secondLeaseHandle = client.startLeaseKeepAlive(new LeaseKeepAliveRequest(leaseId));
            Assert.assertNotNull("second lease handle must not be null", secondLeaseHandle);
            Assert.assertFalse("second lease handle should be active", secondLeaseHandle.isClosed());
            Assert.assertTrue("first lease handle should be closed after same leaseId handle replacement", firstLeaseHandle.isClosed());
            Assert.assertEquals("second lease handle leaseId mismatch", leaseId, secondLeaseHandle.getLeaseId());

            secondLeaseHandle.close();
            Assert.assertTrue("second lease handle should be closed", secondLeaseHandle.isClosed());
            secondLeaseHandle = null;
        } finally {
            if (firstLeaseHandle != null) {
                firstLeaseHandle.close();
            }
            if (secondLeaseHandle != null) {
                secondLeaseHandle.close();
            }
            client.close();
            cluster.close();
        }
    }

    @Test
    public void shouldRevokeLeaseOnCloseWhenGrantAndStartLeaseKeepAlive() throws Exception {
        MiniEtcdCluster cluster = new MiniEtcdCluster();
        cluster.startThreeNodeCluster();

        EtcdClient client = new EtcdClient(cluster.getAllEndpoints());
        LeaseHandle leaseHandle = null;
        try {
            awaitClusterReady(client, TEST_TIMEOUT_MILLIS);

            leaseHandle = client.grantAndStartLeaseKeepAlive(new LeaseGrantRequest(0L, 20L));
            Assert.assertNotNull("lease handle must not be null", leaseHandle);
            Assert.assertTrue("leaseId must be positive", leaseHandle.getLeaseId() > 0L);

            String leaseKey = "smoke/lease/grant-close-revoke/key";
            client.put(new PutRequest(leaseKey, "v1", leaseHandle.getLeaseId()));

            leaseHandle.close();
            Assert.assertTrue("lease handle should be closed", leaseHandle.isClosed());

            waitUntilLeaseKeyDeleted(client, leaseKey, 5000L);
            GetResponse getResponse = client.get(new GetRequest(leaseKey, false));
            Assert.assertNotNull("get response must not be null", getResponse);
            Assert.assertNull("lease key should be deleted after grantAndStart handle close(revoke)", getResponse.getValue());
            leaseHandle = null;
        } finally {
            if (leaseHandle != null) {
                leaseHandle.close();
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
     * 等待 lease 绑定 key 被自动删除。
     */
    private void waitUntilLeaseKeyDeleted(EtcdClient client, String key, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            GetResponse getResponse = client.get(new GetRequest(key, false));
            if (getResponse != null && getResponse.getValue() == null) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("lease key is not deleted after timeout, key=" + key);
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
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_GRANT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_KEEP_ALIVE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_REVOKE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_TTL_REQUEST_METHOD_NAME,
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
