package cs4224.extensions;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import cs4224.utils.Utils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

/**
 * IMPORTANT:
 * This test initialisation assumes that you already have a locally running Cassandra instance and have cqlsh.
 * This test is tested on MacOS platform.
 */
public class InitializationExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static boolean started = false;
    public static CqlSession session;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            started = true;
            context.getRoot().getStore(GLOBAL).put("InitializationExtension", this);

            try {
                session = CqlSession.builder()
                        .withKeyspace(CqlIdentifier.fromCql("wholesale_test"))
                        .build();
            } catch (AllNodesFailedException e) {
                session = CqlSession.builder()
                        .withKeyspace("wholesale_dev_b")
                        .addContactPoint(new InetSocketAddress("192.168.48.189", 9042))
                        .withLocalDatacenter("dc1")
                        .build();
            }

        }
        System.out.println("Initializing tests...");
        createSchema();
        System.out.println("Done initializing!");
    }

    public void createSchema() {
        Utils.executeBashCommand("cqlsh -f src/test/resources/create_table_test.cql");
    }

    @Override
    public void close() {
        System.out.println("Complete all tests!");
    }
}
