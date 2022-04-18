import com.datastax.oss.driver.api.core.CqlSession;
import insertion.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.ExampleUtils;

import static utils.ExampleUtils.KEYSPACE_NAME;

public class CassandraWriteTest {

    private static CqlSession session;
    private static final String EMPLOYEE_BY_NAME = "employee_by_name";
    private static final String EMPLOYEE_BY_EMAIL = "employee_by_email";

    @Before
    public void setup() {
        session = ExampleUtils.connect();
    }

    @Test
    public void testInsertData() {
        session.execute("USE " + KEYSPACE_NAME);
        CassandraUtils.insert(session, EMPLOYEE_BY_NAME);
        CassandraUtils.batchInsert(session, EMPLOYEE_BY_NAME);

        CassandraUtils.insert(session, EMPLOYEE_BY_EMAIL);
        CassandraUtils.batchInsert(session, EMPLOYEE_BY_EMAIL);
        session.close();
    }

    @After
    public void closeSetup() {
        session.close();
    }

}
