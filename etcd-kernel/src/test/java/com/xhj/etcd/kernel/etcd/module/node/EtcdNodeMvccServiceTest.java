package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeMvccServiceTest
 *
 * @author XJks
 * @description EtcdNode MVCC 上层服务语义测试，聚焦读写参数语义、历史版本读取与范围边界语义。
 */
public class EtcdNodeMvccServiceTest {

    private EtcdNode node;

    @Before
    public void setUp() {
        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(10);
        raftConfig.setHeartbeatTimeoutTicks(3);

        node = new EtcdNode("n1", raftConfig, new MemoryStorage());
        node.start();
    }

    @After
    public void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    public void shouldSupportRangeKeysOnlyCountOnlyAndEndKeyExclusive() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("app/a", "1")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("app/b", "2")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("apq/c", "3")).getHeader().isSuccess());

        RangeRequest intervalRequest = new RangeRequest();
        intervalRequest.setStartKey("app/");
        intervalRequest.setEndKeyExclusive("app0");
        EtcdRpcResponse<RangeResponse> intervalResponse = node.handleEtcdRpcRangeRequest(intervalRequest);
        assertTrue(intervalResponse.getHeader().isSuccess());
        assertNotNull(intervalResponse.getBody());
        assertEquals(2, intervalResponse.getBody().getCount());

        RangeRequest keysOnlyRequest = new RangeRequest();
        keysOnlyRequest.setStartKey("app/");
        keysOnlyRequest.setPrefixMatch(true);
        keysOnlyRequest.setKeysOnly(true);
        EtcdRpcResponse<RangeResponse> keysOnlyResponse = node.handleEtcdRpcRangeRequest(keysOnlyRequest);
        assertTrue(keysOnlyResponse.getHeader().isSuccess());
        assertNotNull(keysOnlyResponse.getBody());
        assertEquals(2, keysOnlyResponse.getBody().getItems().size());
        assertNull(keysOnlyResponse.getBody().getItems().get(0).getValue());
        assertNull(keysOnlyResponse.getBody().getItems().get(1).getValue());

        RangeRequest countOnlyRequest = new RangeRequest();
        countOnlyRequest.setStartKey("app/");
        countOnlyRequest.setPrefixMatch(true);
        countOnlyRequest.setCountOnly(true);
        EtcdRpcResponse<RangeResponse> countOnlyResponse = node.handleEtcdRpcRangeRequest(countOnlyRequest);
        assertTrue(countOnlyResponse.getHeader().isSuccess());
        assertNotNull(countOnlyResponse.getBody());
        assertEquals(0, countOnlyResponse.getBody().getItems().size());
        assertEquals(2, countOnlyResponse.getBody().getCount());
    }

    @Test
    public void shouldReadHistoricalRevisionByGetAndRangeOnLocalReadPath() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> firstPut = node.handleEtcdRpcPutRequest(new PutRequest("history/k", "v1"));
        assertTrue(firstPut.getHeader().isSuccess());
        long firstRevision = firstPut.getBody().getRevision();

        EtcdRpcResponse<PutResponse> secondPut = node.handleEtcdRpcPutRequest(new PutRequest("history/k", "v2"));
        assertTrue(secondPut.getHeader().isSuccess());
        assertTrue(secondPut.getBody().getRevision() > firstRevision);

        GetRequest getHistoryRequest = new GetRequest();
        getHistoryRequest.setKey("history/k");
        getHistoryRequest.setRevision(firstRevision);
        getHistoryRequest.setLinearizableRead(false);
        EtcdRpcResponse<GetResponse> getHistoryResponse = node.handleEtcdRpcGetRequest(getHistoryRequest);
        assertTrue(getHistoryResponse.getHeader().isSuccess());
        assertNotNull(getHistoryResponse.getBody());
        assertEquals("v1", getHistoryResponse.getBody().getValue());

        RangeRequest rangeHistoryRequest = new RangeRequest();
        rangeHistoryRequest.setStartKey("history/");
        rangeHistoryRequest.setPrefixMatch(true);
        rangeHistoryRequest.setRevision(firstRevision);
        rangeHistoryRequest.setLinearizableRead(false);
        EtcdRpcResponse<RangeResponse> rangeHistoryResponse = node.handleEtcdRpcRangeRequest(rangeHistoryRequest);
        assertTrue(rangeHistoryResponse.getHeader().isSuccess());
        assertNotNull(rangeHistoryResponse.getBody());
        assertEquals(1, rangeHistoryResponse.getBody().getCount());
        assertEquals("v1", rangeHistoryResponse.getBody().getItems().get(0).getValue());
    }

    @Test
    public void shouldRejectInvalidRevisionOnLocalReadPath() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("invalid/revision", "v1")).getHeader().isSuccess());

        RangeRequest negativeRevisionRangeRequest = new RangeRequest();
        negativeRevisionRangeRequest.setStartKey("invalid/");
        negativeRevisionRangeRequest.setPrefixMatch(true);
        negativeRevisionRangeRequest.setLinearizableRead(false);
        negativeRevisionRangeRequest.setRevision(-1L);
        EtcdRpcResponse<RangeResponse> negativeRevisionRangeResponse = node.handleEtcdRpcRangeRequest(negativeRevisionRangeRequest);
        assertNotNull(negativeRevisionRangeResponse);
        assertNotNull(negativeRevisionRangeResponse.getHeader());
        assertFalse(negativeRevisionRangeResponse.getHeader().isSuccess());

        RangeRequest futureRevisionRangeRequest = new RangeRequest();
        futureRevisionRangeRequest.setStartKey("invalid/");
        futureRevisionRangeRequest.setPrefixMatch(true);
        futureRevisionRangeRequest.setLinearizableRead(false);
        futureRevisionRangeRequest.setRevision(9999L);
        EtcdRpcResponse<RangeResponse> futureRevisionRangeResponse = node.handleEtcdRpcRangeRequest(futureRevisionRangeRequest);
        assertNotNull(futureRevisionRangeResponse);
        assertNotNull(futureRevisionRangeResponse.getHeader());
        assertFalse(futureRevisionRangeResponse.getHeader().isSuccess());

        GetRequest futureRevisionGetRequest = new GetRequest();
        futureRevisionGetRequest.setKey("invalid/revision");
        futureRevisionGetRequest.setLinearizableRead(false);
        futureRevisionGetRequest.setRevision(9999L);
        EtcdRpcResponse<GetResponse> futureRevisionGetResponse = node.handleEtcdRpcGetRequest(futureRevisionGetRequest);
        assertNotNull(futureRevisionGetResponse);
        assertNotNull(futureRevisionGetResponse.getHeader());
        assertFalse(futureRevisionGetResponse.getHeader().isSuccess());
    }

    @Test
    public void shouldRejectInvalidIntervalOnLocalRangeReadPath() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("interval/a", "v1")).getHeader().isSuccess());

        RangeRequest invalidIntervalRequest = new RangeRequest();
        invalidIntervalRequest.setStartKey("interval/z");
        invalidIntervalRequest.setEndKeyExclusive("interval/a");
        invalidIntervalRequest.setLinearizableRead(false);
        EtcdRpcResponse<RangeResponse> invalidIntervalResponse = node.handleEtcdRpcRangeRequest(invalidIntervalRequest);
        assertNotNull(invalidIntervalResponse);
        assertNotNull(invalidIntervalResponse.getHeader());
        assertFalse(invalidIntervalResponse.getHeader().isSuccess());
    }

    @Test
    public void shouldCompactByRpcAndRejectCompactedHistoricalReads() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> firstPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("compact/node/key", "v1"));
        EtcdRpcResponse<PutResponse> secondPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("compact/node/key", "v2"));
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        EtcdRpcResponse<CompactResponse> compactResponse = node.handleEtcdRpcCompactRequest(compactRequest);
        assertNotNull(compactResponse);
        assertNotNull(compactResponse.getHeader());
        assertTrue(compactResponse.getHeader().isSuccess());
        assertNotNull(compactResponse.getBody());
        assertEquals(secondPutResponse.getBody().getRevision(), compactResponse.getBody().getCompactRevision());
        assertEquals(secondPutResponse.getBody().getRevision(), compactResponse.getBody().getCurrentRevision());

        GetRequest compactedGetRequest = new GetRequest();
        compactedGetRequest.setKey("compact/node/key");
        compactedGetRequest.setRevision(firstPutResponse.getBody().getRevision());
        compactedGetRequest.setLinearizableRead(false);
        EtcdRpcResponse<GetResponse> compactedGetResponse = node.handleEtcdRpcGetRequest(compactedGetRequest);
        assertNotNull(compactedGetResponse);
        assertNotNull(compactedGetResponse.getHeader());
        assertFalse(compactedGetResponse.getHeader().isSuccess());
        assertTrue(compactedGetResponse.getHeader().getMessage().contains("compacted"));

        RangeRequest compactedRangeRequest = new RangeRequest();
        compactedRangeRequest.setStartKey("compact/node/");
        compactedRangeRequest.setPrefixMatch(true);
        compactedRangeRequest.setLinearizableRead(false);
        compactedRangeRequest.setRevision(firstPutResponse.getBody().getRevision());
        EtcdRpcResponse<RangeResponse> compactedRangeResponse = node.handleEtcdRpcRangeRequest(compactedRangeRequest);
        assertNotNull(compactedRangeResponse);
        assertNotNull(compactedRangeResponse.getHeader());
        assertFalse(compactedRangeResponse.getHeader().isSuccess());
        assertTrue(compactedRangeResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldRejectCompactRequestWhenRevisionIsAlreadyCompacted() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("compact/node/duplicate", "v1"));
        assertTrue(putResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(putResponse.getBody().getRevision());
        EtcdRpcResponse<CompactResponse> firstCompactResponse = node.handleEtcdRpcCompactRequest(compactRequest);
        assertNotNull(firstCompactResponse);
        assertNotNull(firstCompactResponse.getHeader());
        assertTrue(firstCompactResponse.getHeader().isSuccess());

        EtcdRpcResponse<CompactResponse> secondCompactResponse = node.handleEtcdRpcCompactRequest(compactRequest);
        assertNotNull(secondCompactResponse);
        assertNotNull(secondCompactResponse.getHeader());
        assertFalse(secondCompactResponse.getHeader().isSuccess());
        assertTrue(secondCompactResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldRejectCompactRequestWhenRevisionIsInvalid() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("compact/node/invalid", "v1"));
        assertTrue(putResponse.getHeader().isSuccess());

        CompactRequest nonPositiveRequest = new CompactRequest();
        nonPositiveRequest.setRevision(0L);
        EtcdRpcResponse<CompactResponse> nonPositiveResponse = node.handleEtcdRpcCompactRequest(nonPositiveRequest);
        assertNotNull(nonPositiveResponse);
        assertNotNull(nonPositiveResponse.getHeader());
        assertFalse(nonPositiveResponse.getHeader().isSuccess());

        CompactRequest futureRevisionRequest = new CompactRequest();
        futureRevisionRequest.setRevision(putResponse.getBody().getRevision() + 10L);
        EtcdRpcResponse<CompactResponse> futureRevisionResponse = node.handleEtcdRpcCompactRequest(futureRevisionRequest);
        assertNotNull(futureRevisionResponse);
        assertNotNull(futureRevisionResponse.getHeader());
        assertFalse(futureRevisionResponse.getHeader().isSuccess());
    }

    private void awaitLeader(EtcdNode targetNode, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (targetNode.getRole() == RaftRoleType.LEADER) {
                return;
            }
            Thread.sleep(50L);
        }
        assertEquals(RaftRoleType.LEADER, targetNode.getRole());
    }
}
