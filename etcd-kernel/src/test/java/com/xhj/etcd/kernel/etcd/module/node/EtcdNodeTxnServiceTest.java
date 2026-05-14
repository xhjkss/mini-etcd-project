package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareFieldType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareOperatorType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeTxnServiceTest
 *
 * @author XJks
 * @description EtcdNode Txn 模块语义测试，聚焦 compare、分支选择、原子回滚与响应顺序。
 */
public class EtcdNodeTxnServiceTest {

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
    public void shouldRejectTxnCompareConditionWhenLongCompareDataIsNegative() throws Exception {
        awaitLeader(node, 3000L);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/invalid/negative-version");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VERSION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(-1L);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/invalid/should-not-write", "v")));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertFalse(txnResponse.getHeader().isSuccess());
        assertNull(txnResponse.getBody());

        EtcdRpcResponse<GetResponse> getResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/invalid/should-not-write", false));
        assertNotNull(getResponse);
        assertNotNull(getResponse.getHeader());
        assertTrue(getResponse.getHeader().isSuccess());
        assertNull(getResponse.getBody().getValue());
    }

    @Test
    public void shouldExecuteTxnSuccessBranchAndReturnOrderedTxnOperationResponses() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> seedPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/success/base", "v1"));
        assertTrue(seedPutResponse.getHeader().isSuccess());

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/success/base");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VERSION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(1L);
        txnRequest.getCompareConditions().add(compareCondition);

        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/success/write", "written-by-success")));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.get(new GetRequest("txn/success/write", false)));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertTrue(txnResponse.getHeader().isSuccess());
        assertNotNull(txnResponse.getBody());
        assertTrue(txnResponse.getBody().isSucceeded());
        assertTrue(txnResponse.getBody().getRevision() >= seedPutResponse.getBody().getRevision());
        assertEquals(2, txnResponse.getBody().getResponses().size());
        assertEquals(TxnOperationType.PUT, txnResponse.getBody().getResponses().get(0).getOperationType());
        assertEquals(TxnOperationType.GET, txnResponse.getBody().getResponses().get(1).getOperationType());
        GetResponse readBackResponse = txnResponse.getBody().getResponses().get(1).dataAs(GetResponse.class);
        assertNotNull(readBackResponse);
        assertEquals("written-by-success", readBackResponse.getValue());
    }

    @Test
    public void shouldExecuteTxnFailureBranchWhenCompareConditionNotMatched() throws Exception {
        awaitLeader(node, 3000L);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/failure/base");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VALUE);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData("expect-hit");
        txnRequest.getCompareConditions().add(compareCondition);

        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/failure/success-key", "success-branch-value")));
        txnRequest.getFailureOperations().add(TxnOperationRequest.put(new PutRequest("txn/failure/failure-key", "failure-branch-value")));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertTrue(txnResponse.getHeader().isSuccess());
        assertNotNull(txnResponse.getBody());
        assertFalse(txnResponse.getBody().isSucceeded());
        assertEquals(1, txnResponse.getBody().getResponses().size());
        assertEquals(TxnOperationType.PUT, txnResponse.getBody().getResponses().get(0).getOperationType());

        EtcdRpcResponse<GetResponse> successBranchKeyResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/failure/success-key", false));
        assertNotNull(successBranchKeyResponse);
        assertNotNull(successBranchKeyResponse.getHeader());
        assertTrue(successBranchKeyResponse.getHeader().isSuccess());
        assertNull(successBranchKeyResponse.getBody().getValue());

        EtcdRpcResponse<GetResponse> failureBranchKeyResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/failure/failure-key", false));
        assertNotNull(failureBranchKeyResponse);
        assertNotNull(failureBranchKeyResponse.getHeader());
        assertTrue(failureBranchKeyResponse.getHeader().isSuccess());
        assertEquals("failure-branch-value", failureBranchKeyResponse.getBody().getValue());
    }

    @Test
    public void shouldRollbackWholeTxnWhenBranchOperationFails() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> baselinePutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/rollback/baseline", "baseline"));
        assertTrue(baselinePutResponse.getHeader().isSuccess());
        long baselineRevision = baselinePutResponse.getBody().getRevision();

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/rollback/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VERSION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(0L);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/rollback/key", "should-be-rolled-back")));

        RangeRequest invalidRangeRequest = new RangeRequest();
        invalidRangeRequest.setStartKey("txn/rollback/z");
        invalidRangeRequest.setEndKeyExclusive("txn/rollback/a");
        invalidRangeRequest.setLinearizableRead(false);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.range(invalidRangeRequest));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertFalse(txnResponse.getHeader().isSuccess());
        assertNull(txnResponse.getBody());

        EtcdRpcResponse<GetResponse> rollbackKeyResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/rollback/key", false));
        assertNotNull(rollbackKeyResponse);
        assertNotNull(rollbackKeyResponse.getHeader());
        assertTrue(rollbackKeyResponse.getHeader().isSuccess());
        assertNull(rollbackKeyResponse.getBody().getValue());

        EtcdRpcResponse<PutResponse> probePutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/rollback/probe", "probe"));
        assertTrue(probePutResponse.getHeader().isSuccess());
        assertEquals(baselineRevision + 1L, probePutResponse.getBody().getRevision());
    }

    @Test
    public void shouldRollbackTxnWhenBranchHistoricalReadHitsCompactedBoundary() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> firstPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/compact/source", "v1"));
        EtcdRpcResponse<PutResponse> secondPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/compact/source", "v2"));
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        assertTrue(node.handleEtcdRpcCompactRequest(compactRequest).getHeader().isSuccess());

        EtcdRpcResponse<PutResponse> baselinePutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/compact/baseline", "baseline"));
        assertTrue(baselinePutResponse.getHeader().isSuccess());
        long baselineRevision = baselinePutResponse.getBody().getRevision();

        TxnRequest txnRequest = new TxnRequest();
        txnRequest.getCompareConditions().add(TxnCompareCondition.version(
                "txn/compact/guard",
                TxnCompareOperatorType.EQUAL,
                0L));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/compact/rollback-target", "temp")));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.get(new GetRequest(
                "txn/compact/source",
                firstPutResponse.getBody().getRevision(),
                false)));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertFalse(txnResponse.getHeader().isSuccess());
        assertTrue(txnResponse.getHeader().getMessage().contains("compacted"));
        assertNull(txnResponse.getBody());

        EtcdRpcResponse<GetResponse> rolledBackKeyResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/compact/rollback-target", false));
        assertNotNull(rolledBackKeyResponse);
        assertNotNull(rolledBackKeyResponse.getHeader());
        assertTrue(rolledBackKeyResponse.getHeader().isSuccess());
        assertNull(rolledBackKeyResponse.getBody().getValue());

        EtcdRpcResponse<PutResponse> probePutResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/compact/probe", "probe"));
        assertTrue(probePutResponse.getHeader().isSuccess());
        assertEquals(baselineRevision + 1L, probePutResponse.getBody().getRevision());
    }

    @Test
    public void shouldSupportAllTxnOperationTypesInSingleBranch() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("txn/all/range/1", "r1")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("txn/all/range/2", "r2")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("txn/all/delete-single", "d1")).getHeader().isSuccess());

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/all/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.MOD_REVISION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(0L);
        txnRequest.getCompareConditions().add(compareCondition);

        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/all/put", "put-value")));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.delete(new DeleteRequest("txn/all/delete-single")));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.get(new GetRequest("txn/all/put", false)));

        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("txn/all/range/");
        rangeRequest.setPrefixMatch(true);
        rangeRequest.setLinearizableRead(false);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.range(rangeRequest));

        DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
        deleteRangeRequest.setStartKey("txn/all/range/");
        deleteRangeRequest.setPrefixMatch(true);
        deleteRangeRequest.setPrevKv(true);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.deleteRange(deleteRangeRequest));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertTrue(txnResponse.getHeader().isSuccess());
        assertNotNull(txnResponse.getBody());
        assertTrue(txnResponse.getBody().isSucceeded());
        assertEquals(5, txnResponse.getBody().getResponses().size());

        List<TxnOperationResponse> responseOps = txnResponse.getBody().getResponses();
        assertEquals(TxnOperationType.PUT, responseOps.get(0).getOperationType());
        assertEquals(TxnOperationType.DELETE, responseOps.get(1).getOperationType());
        assertEquals(TxnOperationType.GET, responseOps.get(2).getOperationType());
        assertEquals(TxnOperationType.RANGE, responseOps.get(3).getOperationType());
        assertEquals(TxnOperationType.DELETE_RANGE, responseOps.get(4).getOperationType());
        GetResponse getResponse = responseOps.get(2).dataAs(GetResponse.class);
        RangeResponse rangeResponse = responseOps.get(3).dataAs(RangeResponse.class);
        DeleteRangeResponse deleteRangeResponse = responseOps.get(4).dataAs(DeleteRangeResponse.class);
        assertNotNull(getResponse);
        assertNotNull(rangeResponse);
        assertNotNull(deleteRangeResponse);
        assertEquals("put-value", getResponse.getValue());
        assertEquals(2, rangeResponse.getCount());
        assertEquals(2, deleteRangeResponse.getDeletedCount());

        EtcdRpcResponse<GetResponse> deletedSingleResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/all/delete-single", false));
        assertNotNull(deletedSingleResponse);
        assertNotNull(deletedSingleResponse.getHeader());
        assertTrue(deletedSingleResponse.getHeader().isSuccess());
        assertNull(deletedSingleResponse.getBody().getValue());
    }

    @Test
    public void shouldSupportTxnCompareOnCreateRevisionField() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("txn/compare/create-revision", "v1"));
        assertTrue(putResponse.getHeader().isSuccess());
        long createRevision = putResponse.getBody().getRevision();

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/compare/create-revision");
        compareCondition.setCompareFieldType(TxnCompareFieldType.CREATE_REVISION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(createRevision);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/compare/create-revision/result", "ok")));

        EtcdRpcResponse<TxnResponse> txnResponse = node.handleEtcdRpcTxnRequest(txnRequest);
        assertNotNull(txnResponse);
        assertNotNull(txnResponse.getHeader());
        assertTrue(txnResponse.getHeader().isSuccess());
        assertNotNull(txnResponse.getBody());
        assertTrue(txnResponse.getBody().isSucceeded());

        EtcdRpcResponse<GetResponse> getResponse = node.handleEtcdRpcGetRequest(new GetRequest("txn/compare/create-revision/result", false));
        assertNotNull(getResponse);
        assertNotNull(getResponse.getHeader());
        assertTrue(getResponse.getHeader().isSuccess());
        assertEquals("ok", getResponse.getBody().getValue());
    }

    @Test
    public void shouldSupportTxnCompareOperatorTypeNotEqualAndGreaterAndLess() throws Exception {
        awaitLeader(node, 3000L);
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("txn/compare/operators", "v2")).getHeader().isSuccess());

        TxnRequest notEqualTxnRequest = new TxnRequest();
        notEqualTxnRequest.getCompareConditions().add(TxnCompareCondition.value(
                "txn/compare/operators",
                TxnCompareOperatorType.NOT_EQUAL,
                "v1"));
        notEqualTxnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/compare/operators/not-equal", "ok")));
        EtcdRpcResponse<TxnResponse> notEqualTxnResponse = node.handleEtcdRpcTxnRequest(notEqualTxnRequest);
        assertNotNull(notEqualTxnResponse);
        assertNotNull(notEqualTxnResponse.getHeader());
        assertTrue(notEqualTxnResponse.getHeader().isSuccess());
        assertNotNull(notEqualTxnResponse.getBody());
        assertTrue(notEqualTxnResponse.getBody().isSucceeded());

        TxnRequest valueGreaterTxnRequest = new TxnRequest();
        valueGreaterTxnRequest.getCompareConditions().add(TxnCompareCondition.value(
                "txn/compare/operators",
                TxnCompareOperatorType.GREATER,
                "v1"));
        valueGreaterTxnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/compare/operators/value-greater", "ok")));
        EtcdRpcResponse<TxnResponse> valueGreaterTxnResponse = node.handleEtcdRpcTxnRequest(valueGreaterTxnRequest);
        assertNotNull(valueGreaterTxnResponse);
        assertNotNull(valueGreaterTxnResponse.getHeader());
        assertTrue(valueGreaterTxnResponse.getHeader().isSuccess());
        assertNotNull(valueGreaterTxnResponse.getBody());
        assertTrue(valueGreaterTxnResponse.getBody().isSucceeded());

        TxnRequest versionLessTxnRequest = new TxnRequest();
        versionLessTxnRequest.getCompareConditions().add(TxnCompareCondition.version(
                "txn/compare/operators",
                TxnCompareOperatorType.LESS,
                5L));
        versionLessTxnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/compare/operators/version-less", "ok")));
        EtcdRpcResponse<TxnResponse> versionLessTxnResponse = node.handleEtcdRpcTxnRequest(versionLessTxnRequest);
        assertNotNull(versionLessTxnResponse);
        assertNotNull(versionLessTxnResponse.getHeader());
        assertTrue(versionLessTxnResponse.getHeader().isSuccess());
        assertNotNull(versionLessTxnResponse.getBody());
        assertTrue(versionLessTxnResponse.getBody().isSucceeded());
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
