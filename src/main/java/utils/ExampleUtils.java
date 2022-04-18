package utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import lombok.extern.log4j.Log4j2;
import models.Employee;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class ExampleUtils {
    public static final String KEYSPACE_NAME = "employee_keyspace";


    public static CqlSession connect() {
        CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();
        log.info("[OK] Connected to Keyspace {} on node 127.0.0.1", KEYSPACE_NAME);
        return cqlSession;
    }

    public static SimpleStatement createKeyspaceSimpleStrategy(String keyspaceName, int replicationFactor) {
        return SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists()
                .withSimpleStrategy(replicationFactor)
                .withDurableWrites(true)
                .build();
    }

    public static SimpleStatement createTableEmployeeByNameStmt() {
        return SchemaBuilder.createTable("employee_by_name")
                .ifNotExists()
                .withPartitionKey("name", DataTypes.TEXT)
                .withColumn("name", DataTypes.TEXT)
                .withColumn("email", DataTypes.TEXT)
                .withColumn("salary", DataTypes.INT)
                .build();
    }

    public static SimpleStatement createTableEmployeeByEmailStmt() {
        return SchemaBuilder.createTable("employee_by_email")
                .ifNotExists()
                .withPartitionKey("email", DataTypes.TEXT)
                .withColumn("name", DataTypes.TEXT)
                .withColumn("email", DataTypes.TEXT)
                .withColumn("salary", DataTypes.INT)
                .build();
    }

    public static List<Employee.EmployeeBuilder> retrieveEmployees(ResultSet employeeList) {
        return employeeList.all().stream().map(row -> Employee.builder()
                .name(row.getString("name"))
                .email(row.getString("email"))
                .salary(row.getInt("salary")))
                .collect(Collectors.toList());
    }

    public static  List<Employee.EmployeeBuilder> getEmployees(ResultSet resultSet) {
        return resultSet.all().stream().map(row -> Employee.builder()
                .name(row.getString("name"))
                .email(row.getString("email")))
                .collect(Collectors.toList());
    }


}
