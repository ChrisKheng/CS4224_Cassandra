package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public abstract class BaseTransaction {
    private final String[] parameters;
    protected final CqlSession session;

    public BaseTransaction(final CqlSession session, final String[] parameters) {
        this.session = session;
        this.parameters = parameters;
    }

    public abstract void execute(final String[] dataLines);

    // NewOrderTransaction requires reading more lines
    public int getExtraLines() {
        return 0;
    }
}
