package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;

public class TopBalanceTransaction extends BaseTransaction {

    public TopBalanceTransaction(CqlSession session) {
        super(session);
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {

    }
}
