package insertion;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

public class CassandraUtils {

    public static void insert(CqlSession session, String tableName) {
        PreparedStatement ps2 = session.prepare(
                QueryBuilder
                .insertInto(tableName)
                .value("email", QueryBuilder.bindMarker())
                .value("name", QueryBuilder.bindMarker())
                .value("salary", QueryBuilder.bindMarker())
                .build());
        System.out.println("\n Insert Query - > " + ps2.getQuery());
        session.execute(ps2.bind("clun7@gmail.com", "qwerty", 250000));
    }

    public static void batchInsert(CqlSession session, String tableName) {
        PreparedStatement stmtCreateUser =
                session.prepare(QueryBuilder.insertInto(tableName)
                        .value("email", QueryBuilder.bindMarker())
                        .value("name", QueryBuilder.bindMarker())
                        .value("salary", QueryBuilder.bindMarker())
                        .build());

        // Adding 50 records in the table
        BatchStatementBuilder bb = BatchStatement.builder(DefaultBatchType.LOGGED);
        for (int i = 0; i < 50; i++) {
            bb.addStatement(stmtCreateUser.bind("user_" + i + "@sample.com", "user_" + i, 30000));
        }
        ResultSet execute = session.execute(bb.build());

    }

    public static ResultSet list(CqlSession session) {
        //list api
        PreparedStatement ptStmt = session.prepare(
                QueryBuilder.selectFrom("employee_by_name")
                        .columns("email", "name", "salary")
                        .build());
        System.out.println("\n List Query - > " + ptStmt.getQuery());
        return session.execute(ptStmt.getQuery());
    }

    public static ResultSet list2(CqlSession session) {
        return session.execute("select * from employee_keyspace.employee_by_email");
    }

    public static ResultSet retrieveEmployeeByName(CqlSession session, String name) {
        //get a column based on a condition
        PreparedStatement ptStmt = session.prepare(
                QueryBuilder.selectFrom("employee_by_name")
                .columns("email", "name")
                .whereColumn("name").isEqualTo(QueryBuilder.bindMarker())
                .build());
        System.out.println("\n Retreive By name Query - > " + ptStmt.getQuery());
        return session.execute(ptStmt.bind(name));

    }

    public static ResultSet retrieveUserByEmail(CqlSession session, String email) {
        PreparedStatement ptStmt = session.prepare(
                QueryBuilder.selectFrom("employee_by_email")
                        .columns("email", "name")
                        .whereColumn("email").isEqualTo(QueryBuilder.bindMarker())
                        .build());
        System.out.println("\n Retrieve by email Query 2 - > " + ptStmt.getQuery());
        return session.execute(ptStmt.bind(email));
    }
}
