package com.test.jbpm;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.kie.server.api.model.definition.QueryDefinition;
import org.kie.server.api.model.definition.QueryFilterSpec;
import org.kie.server.api.model.instance.TaskInstance;

import org.kie.server.api.model.instance.TaskInstanceList;
import org.kie.server.api.util.QueryFilterSpecBuilder;
import org.kie.server.client.KieServicesClient;
import org.kie.server.client.KieServicesConfiguration;
import org.kie.server.client.KieServicesFactory;
import org.kie.server.client.QueryServicesClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryTest.class);

    private static final String LOGIN = "tao";
    private static final String PASSWORD = "password1!";
    private static final String SERVER_URL = "http://localhost:8080/kie-server/services/rest/server";

    private static final String dataSourceJNDIname="java:jboss/datasources/ExampleDS";
    private static QueryDefinition queryDefinition;
    private static QueryServicesClient queryClient;

    @Before
    public void initObjects() {
        // setup the client
        KieServicesConfiguration conf = KieServicesFactory.newRestConfiguration(SERVER_URL, LOGIN, PASSWORD);
        KieServicesClient client = KieServicesFactory.newKieServicesClient(conf);
        // get the query services client
        queryClient = client.getServicesClient(QueryServicesClient.class);
    }


    @Test
    public void TestGetAllTasks() {

        queryDefinition = QueryDefinition.builder().name("getAllTasks")
                .expression("select * from Task t")
                .source(dataSourceJNDIname)
                .target("TASK").build();

        try {
            queryClient.unregisterQuery("getAllTasks");
        } catch (Exception e){
            logger.info("unregister query is fail.");
        }

        logger.info("register getAllTasks...");
        queryClient.registerQuery(queryDefinition);

        // executes the query
        List<TaskInstance> taskList = queryClient.query("getAllTasks", QueryServicesClient.QUERY_MAP_TASK, 0, 100, TaskInstance.class);
        for (TaskInstance taskInstance : taskList) {
            logger.info(taskInstance.toString());
        }
    }

    @Test
    public void registGetAllTaskInstancesVarProductSale(){
        queryDefinition = QueryDefinition.builder().name("getAllTaskInstancesVarProductSale")
                .expression("select ti.*,  c.country, c.productCode, c.quantity, c.price, c.saleDate " +
                        "from AuditTaskImpl ti " +
                        "    inner join (select mv.map_var_id, mv.taskid from MappedVariable mv) mv " +
                        "      on (mv.taskid = ti.taskId) " +
                        "    inner join ProductSale c " +
                        "      on (c.id = mv.map_var_id)")
                .source(dataSourceJNDIname)
                .target("CUSTOM").build();

        queryClient.registerQuery(queryDefinition);
    }

    @Test
    public void queryGetAllTaskInstancesVarProductSale(){
        QueryFilterSpec spec = new QueryFilterSpecBuilder().equalsTo("PRODUCTCODE","aaa").get();

        List<TaskInstance> tasks = queryClient.query("getAllTaskInstancesVarProductSale",
                QueryServicesClient.QUERY_MAP_TASK_WITH_CUSTOM_VARS,
                spec,
                0,
                10,
                TaskInstance.class);
        System.out.println(tasks);

    }

    @Test
    public void registGetMyTaskInstancesWithCustomVariables(){
        queryDefinition = QueryDefinition.builder().name("getMyTaskInstancesWithCustomVariables")
                .expression("select ti.*,  c.country, c.productCode, c.quantity, c.price, c.saleDate, oe.id oeid " +
                        "from AuditTaskImpl ti " +
                        "    inner join (select mv.map_var_id, mv.taskid from MappedVariable mv) mv " +
                        "      on (mv.taskid = ti.taskId) " +
                        "    inner join ProductSale c " +
                        "      on (c.id = mv.map_var_id), " +
                        "  PeopleAssignments_PotOwners po, OrganizationalEntity oe " +
                        "    where ti.taskId = po.task_id and po.entity_id = oe.id")
                .source(dataSourceJNDIname)
                .target("CUSTOM").build();

        queryClient.registerQuery(queryDefinition);
    }

    @Test
    public void unregisterGetMyTaskInstancesWithCustomVariables(){
        queryClient.unregisterQuery("getMyTaskInstancesWithCustomVariables");
    }

    @Test
    public void queryGetMyTaskInstancesWithCustomVariables(){
        QueryFilterSpec spec = new QueryFilterSpecBuilder().equalsTo("PRODUCTCODE","aaa").get();

        List<TaskInstanceList> tasks = queryClient.query("getMyTaskInstancesWithCustomVariables",
                "UserTasksWithCustomVariables",
                spec,
                0,
                1000,
                TaskInstanceList.class);
        System.out.println(tasks);
    }

    @Test
    public void registGetAllTaskInstancesWithCustomVariables(){
        queryDefinition = QueryDefinition.builder().name("getAllTaskInstancesWithCustomVariables")
                .expression("select ti.*,  c.country, c.productCode, c.quantity, c.price, c.saleDate " +
                        "from AuditTaskImpl ti " +
                        "    inner join (select mv.map_var_id, mv.taskid from MappedVariable mv) mv " +
                        "      on (mv.taskid = ti.taskId) " +
                        "    inner join ProductSale c " +
                        "      on (c.id = mv.map_var_id)")
                .source(dataSourceJNDIname)
                .target("CUSTOM").build();

        queryClient.registerQuery(queryDefinition);
    }

    @Test
    public void unregisterGetAllTaskInstancesWithCustomVariables(){
        queryClient.unregisterQuery("getAllTaskInstancesWithCustomVariables");
    }


    @Test
    public void queryGetAllTaskInstancesWithCustomVariables(){
        QueryFilterSpec spec = new QueryFilterSpecBuilder().equalsTo("PRODUCTCODE","aaa").get();

        List<TaskInstanceList> tasks = queryClient.query("getAllTaskInstancesWithCustomVariables",
                "UserTasksWithCustomVariables",
                spec,
                0,
                1000,
                TaskInstanceList.class);
        System.out.println(tasks);
    }
}
