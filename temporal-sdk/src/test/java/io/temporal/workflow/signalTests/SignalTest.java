/*
 * Copyright (C) 2022 Temporal Technologies, Inc. All Rights Reserved.
 *
 * Copyright (C) 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this material except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.temporal.workflow.signalTests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.QueryRejectCondition;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.client.*;
import io.temporal.testing.internal.SDKTestOptions;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import io.temporal.workflow.CompletablePromise;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.shared.TestWorkflows.QueryableWorkflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalTest {

  private static final Logger log = LoggerFactory.getLogger(SignalTest.class);

  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      SDKTestWorkflowRule.newBuilder()
          .setWorkflowTypes(TestSignalWorkflowImpl.class)
          .setTestTimeoutSeconds(15)
          .build();

  @Test
  public void testSignal() {
    String workflowId = UUID.randomUUID().toString();
    WorkflowClient workflowClient = testWorkflowRule.getWorkflowClient();
    WorkflowOptions options =
        SDKTestOptions.newWorkflowOptionsWithTimeouts(testWorkflowRule.getTaskQueue()).toBuilder()
            .setWorkflowId(workflowId)
            .build();
    QueryableWorkflow client = workflowClient.newWorkflowStub(QueryableWorkflow.class, options);
    // To execute workflow client.execute() would do. But we want to start workflow and immediately
    // return.
    WorkflowExecution execution = WorkflowClient.start(client::execute);

    testWorkflowRule.sleep(Duration.ofSeconds(1));
    assertEquals(workflowId, execution.getWorkflowId());
    // Calls query multiple times to check at the end of the method that if it doesn't leak threads
    assertEquals("initial", client.getState());
    testWorkflowRule.sleep(Duration.ofSeconds(1));

    client.mySignal("Hello ");
    testWorkflowRule.sleep(Duration.ofSeconds(1));

    // Test client created using WorkflowExecution
    QueryableWorkflow client2 =
        workflowClient.newWorkflowStub(
            QueryableWorkflow.class, execution.getWorkflowId(), Optional.of(execution.getRunId()));
    assertEquals("Hello ", client2.getState());

    testWorkflowRule.sleep(Duration.ofMillis(500));
    client2.mySignal("World!");
    testWorkflowRule.sleep(Duration.ofMillis(500));
    assertEquals("World!", client2.getState());
    assertEquals(
        "Hello World!",
        workflowClient.newUntypedWorkflowStub(execution, Optional.empty()).getResult(String.class));
    client2.execute();
  }

  @Test
  public void testSignalWithStart() {
    String workflowId = UUID.randomUUID().toString();
    WorkflowClient workflowClient = testWorkflowRule.getWorkflowClient();
    WorkflowOptions options =
        SDKTestOptions.newWorkflowOptionsWithTimeouts(testWorkflowRule.getTaskQueue()).toBuilder()
            .setWorkflowId(workflowId)
            .build();

    // SignalWithStart starts a workflow and delivers the signal to it.
    QueryableWorkflow wf1 = workflowClient.newWorkflowStub(QueryableWorkflow.class, options);
    SignalWithStartWorkflowOperation signalOp =
        SignalWithStartWorkflowOperation.newBuilder(wf1::mySignal, "Hello ").build();
    WorkflowExecution execution = WorkflowClient.startWithOperation(wf1::execute, signalOp);
    testWorkflowRule.sleep(Duration.ofSeconds(1));

    // Test client created using WorkflowExecution
    QueryableWorkflow wf2 = workflowClient.newWorkflowStub(QueryableWorkflow.class, options);
    // SignalWithStart delivers the signal to the already running workflow.
    SignalWithStartWorkflowOperation signalOp2 =
        SignalWithStartWorkflowOperation.newBuilder(wf2::mySignal, "World!").build();
    WorkflowExecution execution2 = WorkflowClient.startWithOperation(wf2::execute, signalOp2);
    assertEquals(execution, execution2);

    testWorkflowRule.sleep(Duration.ofMillis(500));
    assertEquals("World!", wf2.getState());
    assertEquals(
        "Hello World!",
        workflowClient.newUntypedWorkflowStub(execution, Optional.empty()).getResult(String.class));

    // Check if that it starts closed workflow (AllowDuplicate is default IdReusePolicy)
    QueryableWorkflow wf3 = workflowClient.newWorkflowStub(QueryableWorkflow.class, options);
    SignalWithStartWorkflowOperation signalOp3 =
        SignalWithStartWorkflowOperation.newBuilder(wf3::mySignal, "Hello ").build();
    WorkflowExecution execution3 = WorkflowClient.startWithOperation(wf3::execute, signalOp3);
    assertEquals(execution.getWorkflowId(), execution3.getWorkflowId());
    wf3.mySignal("World!");
    WorkflowStub untyped = WorkflowStub.fromTyped(wf3);
    String result = untyped.getResult(String.class);
    assertEquals("Hello World!", result);

    // Make sure that cannot start if closed and RejectDuplicate policy
    QueryableWorkflow client4 =
        workflowClient.newWorkflowStub(
            QueryableWorkflow.class,
            options.toBuilder()
                .setWorkflowIdReusePolicy(
                    WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
                .build());
    BatchRequest batch4 = workflowClient.newSignalWithStartRequest();
    batch4.add(client4::mySignal, "Hello ");
    batch4.add(client4::execute);
    try {
      workflowClient.signalWithStart(batch4);
      fail("DuplicateWorkflowException expected");
    } catch (WorkflowExecutionAlreadyStarted e) {
      assertEquals(execution3.getRunId(), e.getExecution().getRunId());
    }
  }

  @Test
  public void testSignalUntyped() {
    WorkflowClient workflowClient = testWorkflowRule.getWorkflowClient();
    String workflowType = QueryableWorkflow.class.getSimpleName();
    WorkflowStub workflowStub =
        workflowClient.newUntypedWorkflowStub(
            workflowType,
            SDKTestOptions.newWorkflowOptionsWithTimeouts(testWorkflowRule.getTaskQueue()));

    WorkflowExecution execution = workflowStub.start();
    SDKTestWorkflowRule.waitForOKQuery(workflowStub);

    assertEquals("initial", workflowStub.query("getState", String.class));

    workflowStub.signal("testSignal", "Hello ");
    while (!"Hello ".equals(workflowStub.query("getState", String.class))) {}
    assertEquals("Hello ", workflowStub.query("getState", String.class));

    workflowStub.signal("testSignal", "World!");
    while (!"World!".equals(workflowStub.query("getState", String.class))) {}
    assertEquals("World!", workflowStub.query("getState", String.class));

    assertEquals(
        "Hello World!",
        workflowClient
            .newUntypedWorkflowStub(execution, Optional.of(workflowType))
            .getResult(String.class));
    assertEquals("Hello World!", workflowStub.getResult(String.class));
    assertEquals("World!", workflowStub.query("getState", String.class));

    WorkflowClient client =
        WorkflowClient.newInstance(
            testWorkflowRule.getWorkflowServiceStubs(),
            WorkflowClientOptions.newBuilder()
                .setNamespace(SDKTestWorkflowRule.NAMESPACE)
                .setQueryRejectCondition(QueryRejectCondition.QUERY_REJECT_CONDITION_NOT_OPEN)
                .build());
    WorkflowStub workflowStubNotOptionRejectCondition =
        client.newUntypedWorkflowStub(execution, Optional.of(workflowType));
    try {
      workflowStubNotOptionRejectCondition.query("getState", String.class);
      fail("unreachable");
    } catch (WorkflowQueryConditionallyRejectedException e) {
      assertEquals(
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
          e.getWorkflowExecutionStatus());
    }
  }

  public static class TestSignalWorkflowImpl implements QueryableWorkflow {
    String state = "initial";
    List<String> signals = new ArrayList<>();
    CompletablePromise<Void> promise = Workflow.newPromise();

    @Override
    public String execute() {
      promise.get();
      return signals.get(0) + signals.get(1);
    }

    @Override
    public String getState() {
      return state;
    }

    @Override
    public void mySignal(String value) {
      log.info("TestSignalWorkflowImpl.mySignal value=" + value);
      state = value;
      signals.add(value);
      if (signals.size() == 2) {
        promise.complete(null);
      }
    }
  }
}
