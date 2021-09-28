package cs4224.extensions;

import cs4224.utils.Utils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

/**
 * IMPORTANT:
 * This test initialisation assumes that you already have a locally running Cassandra instance and have cqlsh.
 */
public class InitializationExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            started = true;
            context.getRoot().getStore(GLOBAL).put("InitializationExtension", this);
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
