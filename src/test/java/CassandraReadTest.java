import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import insertion.CassandraUtils;
import models.Employee;
import org.junit.Before;
import org.junit.Test;
import utils.ExampleUtils;

import java.util.List;

import static utils.ExampleUtils.KEYSPACE_NAME;
import static utils.ExampleUtils.getEmployees;
import static utils.ExampleUtils.retrieveEmployees;

public class CassandraReadTest {

    private static volatile CqlSession session;

    @Before
    public void setup() {
        session = ExampleUtils.connect();
    }

    @Test
    public void readCassandraData() {
        session.execute("USE " + KEYSPACE_NAME);
        //List API
        ResultSet employeeList = CassandraUtils.list(session);
        System.out.println(retrieveEmployees(employeeList));
        System.out.println(retrieveEmployees(CassandraUtils.list2(session)));

        ResultSet resultSet = CassandraUtils.retrieveEmployeeByName(session, "qwerty");
        List<Employee.EmployeeBuilder> employeesByName = getEmployees(resultSet);
        System.out.println(employeesByName);

        ResultSet resultSet2 = CassandraUtils.retrieveUserByEmail(session, "user_9@sample.com");
        List<Employee.EmployeeBuilder> employeesBySalary = getEmployees(resultSet2);
        System.out.println(employeesBySalary);
    }
}
