import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.ExampleUtils;

import static utils.ExampleUtils.KEYSPACE_NAME;

/**
 * CREATE KEYSPACE employee_keyspace
 * WITH replication =
 * {'class': 'SimpleStrategy',
 * 'replication_factor': '1'}
 * AND durable_writes = true;
 */


public class CassandraSetupTest {

    private static CqlSession session;

    @Before
    public void setup() {
        session = ExampleUtils.connect();
    }

    @Test
    public void testCreateKeySpace() {
        //Delete any present keyspace
        session.execute("DROP KEYSPACE IF EXISTS employee_keyspace");

        //Creating a keyspace
        session.execute(ExampleUtils.createKeyspaceSimpleStrategy(KEYSPACE_NAME, 2));

        //Using the keyspace
        session.execute("USE " + KEYSPACE_NAME);
        //Create employee  by name table
        session.execute(ExampleUtils.createTableEmployeeByNameStmt());

        //Create employee  by email table
        session.execute(ExampleUtils.createTableEmployeeByEmailStmt());

    }

    @After
    public void closeSetup() {
        session.close();
    }




}
