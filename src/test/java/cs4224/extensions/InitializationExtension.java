package cs4224.extensions;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import cs4224.utils.Utils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

/**
 * IMPORTANT:
 * This test initialisation assumes that you already have a locally running Cassandra instance and have cqlsh.
 * This test is tested on MacOS platform.
 */
public class InitializationExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static boolean started = false;
    public static CqlSession session;
    public static ExecutorService executorService;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            started = true;
            context.getRoot().getStore(GLOBAL).put("InitializationExtension", this);
            executorService = Executors.newFixedThreadPool(1);
            try {
                session = CqlSession.builder()
                        .withKeyspace(CqlIdentifier.fromCql("wholesale_test"))
                        .build();
            } catch (AllNodesFailedException e) {
                session = CqlSession.builder()
                        .withKeyspace("wholesale_dev_a")
                        .addContactPoint(new InetSocketAddress("192.168.48.189", 9042))
                        .withLocalDatacenter("dc1")
                        .build();
            }

        }
        System.out.println("Initializing tests...");
        createSchema();
        System.out.println("Done initializing!");
    }

    private void createSchema() {
        Utils.executeBashCommand("cqlsh -f src/test/resources/create_table_test.cql");
    }

    @Override
    public void close() {
        session.close();
        executorService.shutdown();
        System.out.println("Complete all tests!");
    }
}
