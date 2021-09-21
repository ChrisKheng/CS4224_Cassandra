package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class TopBalanceTransaction extends BaseTransaction {

    public TopBalanceTransaction(CqlSession session, String[] parameters) {
        super(session, parameters);
    }

    @Override
    public void execute(String[] dataLines) {

    }
}
