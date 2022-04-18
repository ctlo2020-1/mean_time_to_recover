import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import lombok.extern.log4j.Log4j2;


import java.net.InetSocketAddress;

@Log4j2
public class CassandraConfig {
    // Try-finally for handling the connection
    // right way to find out if its a persistent connection, make a connection, sleep for some time, then close
    // Check if it releases in between
    // what is bulk processor in elastic search

    public static void main(String[] args) {
        log.info("Starting 'ClusterShowMetaData' sample...");

        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {
            log.info("Connected to cluster with Session '{}'",
                    cqlSession.getName());

            Metadata metaData = cqlSession.getMetadata();

            log.info("Listing available Nodes:");
            for (Node host : metaData.getNodes().values()) {
                log.info("+ [{}]: datacenter='{}' and rack='{}'",
                        host.getListenAddress().orElse(null),
                        host.getDatacenter(),
                        host.getRack());
            }

            log.info("Listing available keyspaces:");
            for (KeyspaceMetadata meta : metaData.getKeyspaces().values()) {
                log.info(" [{}]   with replication={}", meta.getName(), meta.getReplication());
            }

            log.info("Successfully connected to Cassandra Server ");
        }
        System.exit(0);
    }
}
