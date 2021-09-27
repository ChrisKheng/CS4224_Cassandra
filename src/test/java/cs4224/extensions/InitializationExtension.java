package cs4224.extensions;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class InitializationExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            started = true;
            context.getRoot().getStore(GLOBAL).put("InitializationExtension", this);
        }
        System.out.println("Hello World!");
    }

    @Override
    public void close() {
        System.out.println("Goodbye World!");
    }
}
