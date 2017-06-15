/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.camunda.bpm.engine.test.history;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.history.FinishedReportResult;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.camunda.bpm.engine.test.RequiredHistoryLevel;
import org.camunda.bpm.engine.test.dmn.businessruletask.TestPojo;
import org.camunda.bpm.engine.test.util.ProcessEngineTestRule;
import org.camunda.bpm.engine.test.util.ProvidedProcessEngineRule;
import org.camunda.bpm.engine.variable.Variables;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

@RequiredHistoryLevel(ProcessEngineConfiguration.HISTORY_FULL)
public class HistoricFinishedProcessInstanceReportTest {
  public ProcessEngineRule engineRule = new ProvidedProcessEngineRule();
  public ProcessEngineTestRule testRule = new ProcessEngineTestRule(engineRule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(testRule).around(engineRule);

  protected ProcessEngineConfiguration processEngineConfiguration;
  protected HistoryService historyService;

  protected static final String PROCESS_DEFINITION_KEY = "HISTORIC_TASK_INST";
  protected static final String OTHER_PROCESS_DEFINITION_KEY = "OTHER_HISTORIC_TASK_INST";
  protected static final String ANOTHER_PROCESS_DEFINITION_KEY = "ANOTHER_HISTORIC_TASK_INST";
  protected static final String LAST_PROCESS_DEFINITION_KEY = "LAST_HISTORIC_TASK_INST";

  @Before
  public void setUp() {
    historyService = engineRule.getHistoryService();
    processEngineConfiguration = engineRule.getProcessEngineConfiguration();

    testRule.deploy(createProcessWithUserTask(PROCESS_DEFINITION_KEY));
    testRule.deploy(createProcessWithUserTask(OTHER_PROCESS_DEFINITION_KEY));
    testRule.deploy(createProcessWithUserTask(ANOTHER_PROCESS_DEFINITION_KEY));
    testRule.deploy(createProcessWithUserTask(LAST_PROCESS_DEFINITION_KEY));
  }

  @After
  public void cleanUp() {
    List<Task> list = engineRule.getTaskService().createTaskQuery().list();
    for (Task task : list) {
      engineRule.getTaskService().deleteTask(task.getId(), true);
    }

    List<HistoricProcessInstance> historicProcessInstances = engineRule.getHistoryService().createHistoricProcessInstanceQuery().list();
    for (HistoricProcessInstance historicProcessInstance : historicProcessInstances) {
      engineRule.getHistoryService().deleteHistoricProcessInstance(historicProcessInstance.getId());
    }
  }

  protected BpmnModelInstance createProcessWithUserTask(String key) {
    return Bpmn.createExecutableProcess(key)
        .startEvent()
        .userTask(key + "_task1")
          .name(key + " Task 1")
        .endEvent()
        .done();
  }

  protected void prepareProcessInstances(String key, int daysInThePast, Integer historyTimeToLive, int instanceCount, boolean varEnabled) {
    List<ProcessDefinition> processDefinitions = engineRule.getRepositoryService().createProcessDefinitionQuery().processDefinitionKey(key).list();
    assertEquals(1, processDefinitions.size());
    engineRule.getRepositoryService().updateProcessDefinitionHistoryTimeToLive(processDefinitions.get(0).getId(), historyTimeToLive);

    Date oldCurrentTime = ClockUtil.getCurrentTime();
    ClockUtil.setCurrentTime(DateUtils.addDays(new Date(), daysInThePast));

    List<String> processInstanceIds = new ArrayList<String>();
    if (varEnabled) {
      Map<String, Object> variables = Variables.createVariables().putValue("pojo", new TestPojo("okay", 13.37));
      for (int i = 0; i < instanceCount; i++) {
        ProcessInstance processInstance = engineRule.getRuntimeService().startProcessInstanceByKey(key, variables);
        processInstanceIds.add(processInstance.getId());
      }
    } else {
      for (int i = 0; i < instanceCount; i++) {
        ProcessInstance processInstance = engineRule.getRuntimeService().startProcessInstanceByKey(key);
        processInstanceIds.add(processInstance.getId());
      }
    }
    engineRule.getRuntimeService().deleteProcessInstances(processInstanceIds, null, true, true);

    ClockUtil.setCurrentTime(oldCurrentTime);
  }

  @Test
  public void testComplex() {
    // given
    prepareProcessInstances(PROCESS_DEFINITION_KEY, 0, 5, 10, true);
    prepareProcessInstances(PROCESS_DEFINITION_KEY, -6, 5, 10, true);
    prepareProcessInstances(OTHER_PROCESS_DEFINITION_KEY, -6, 5, 10, false);
    prepareProcessInstances(ANOTHER_PROCESS_DEFINITION_KEY, -6, null, 10, true);
    prepareProcessInstances(LAST_PROCESS_DEFINITION_KEY, -6, 0, 10, true);

    engineRule.getRepositoryService().deleteProcessDefinition(
        engineRule.getRepositoryService().createProcessDefinitionQuery().processDefinitionKey(OTHER_PROCESS_DEFINITION_KEY).singleResult().getId(), false);

    engineRule.getProcessEngineConfiguration().getCommandExecutorTxRequired().execute(new Command<Object>() {
      @Override
      public Object execute(CommandContext commandContext) {
        // when
        List<FinishedReportResult> reportResults = historyService.createHistoricFinishedProcessInstanceReport().count(commandContext);

        // then
        assertEquals(3, reportResults.size());
        for (FinishedReportResult result : reportResults) {
          if (result.getProcessDefinitionKey().equals(PROCESS_DEFINITION_KEY)) {
            checkResultNumbers(result, 10, 20);
          } else if (result.getProcessDefinitionKey().equals(ANOTHER_PROCESS_DEFINITION_KEY)) {
            checkResultNumbers(result, 0, 10);
          } else if (result.getProcessDefinitionKey().equals(LAST_PROCESS_DEFINITION_KEY)) {
            checkResultNumbers(result, 10, 10);
          }
        }

        return null;
      }
    });
  }

  private void checkResultNumbers(FinishedReportResult result, int expectedCleanable, int expectedFinished) {
    assertEquals(expectedCleanable, result.getCleanableProcessInstanceCount().longValue());
    assertEquals(expectedFinished, result.getFinishedProcessInstanceCount().longValue());
  }

  @Test
  public void testAllCleanable() {
    // given
    prepareProcessInstances(PROCESS_DEFINITION_KEY, -6, 5, 10, true);

    engineRule.getProcessEngineConfiguration().getCommandExecutorTxRequired().execute(new Command<Object>() {
      @Override
      public Object execute(CommandContext commandContext) {
        // when
        List<FinishedReportResult> reportResults = historyService.createHistoricFinishedProcessInstanceReport().count(commandContext);

        // then
        assertEquals(1, reportResults.size());
        assertEquals(10, reportResults.get(0).getCleanableProcessInstanceCount().longValue());
        assertEquals(10, reportResults.get(0).getFinishedProcessInstanceCount().longValue());

        return null;
      }
    });
  }

  @Test
  public void testPartCleanable() {
    // given
    prepareProcessInstances(PROCESS_DEFINITION_KEY, -6, 5, 5, true);
    prepareProcessInstances(PROCESS_DEFINITION_KEY, 0, 5, 5, true);

    engineRule.getProcessEngineConfiguration().getCommandExecutorTxRequired().execute(new Command<Object>() {
      @Override
      public Object execute(CommandContext commandContext) {
        // when
        List<FinishedReportResult> reportResults = historyService.createHistoricFinishedProcessInstanceReport().count(commandContext);

        // then
        assertEquals(1, reportResults.size());
        assertEquals(5, reportResults.get(0).getCleanableProcessInstanceCount().longValue());
        assertEquals(10, reportResults.get(0).getFinishedProcessInstanceCount().longValue());

        return null;
      }
    });
  }

  @Test
  public void testZeroTTL() {
    // given
    prepareProcessInstances(PROCESS_DEFINITION_KEY, -6, 0, 5, true);
    prepareProcessInstances(PROCESS_DEFINITION_KEY, 0, 0, 5, true);

    engineRule.getProcessEngineConfiguration().getCommandExecutorTxRequired().execute(new Command<Object>() {
      @Override
      public Object execute(CommandContext commandContext) {
        // when
        List<FinishedReportResult> reportResults = historyService.createHistoricFinishedProcessInstanceReport().count(commandContext);

        // then
        assertEquals(1, reportResults.size());
        assertEquals(10, reportResults.get(0).getCleanableProcessInstanceCount().longValue());
        assertEquals(10, reportResults.get(0).getFinishedProcessInstanceCount().longValue());

        return null;
      }
    });
  }

  @Test
  public void testNullTTL() {
    // given
    prepareProcessInstances(PROCESS_DEFINITION_KEY, -6, null, 5, true);
    prepareProcessInstances(PROCESS_DEFINITION_KEY, 0, null, 5, true);

    engineRule.getProcessEngineConfiguration().getCommandExecutorTxRequired().execute(new Command<Object>() {
      @Override
      public Object execute(CommandContext commandContext) {
        // when
        List<FinishedReportResult> reportResults = historyService.createHistoricFinishedProcessInstanceReport().count(commandContext);

        // then
        assertEquals(1, reportResults.size());
        assertEquals(0, reportResults.get(0).getCleanableProcessInstanceCount().longValue());
        assertEquals(10, reportResults.get(0).getFinishedProcessInstanceCount().longValue());

        return null;
      }
    });
  }
}
