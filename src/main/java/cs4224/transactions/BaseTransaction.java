package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public abstract class BaseTransaction {
    protected final CqlSession session;

    public BaseTransaction(final CqlSession session) {
        this.session = session;
    }

    public abstract void execute(final String[] dataLines, final String[] parameters);

    // NewOrderTransaction requires reading more lines
    public int getExtraLines() {
        return 0;
    }
}
