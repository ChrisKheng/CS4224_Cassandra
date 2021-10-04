package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

import java.util.List;
import java.util.stream.Collectors;

public class StockLevelTransaction extends BaseTransaction {

    public StockLevelTransaction(CqlSession session) {
        super(session);
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int districtId = Integer.parseInt(parameters[2]);
        final int threshold = Integer.parseInt(parameters[3]);
        final int noOfOrders = Integer.parseInt(parameters[4]);

        final int districtNextOrderId = session.execute(
                "SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = ? AND D_ID = ?",
                warehouseId,
                districtId
        ).one().getInt(CqlIdentifier.fromCql("D_NEXT_O_ID"));

        List<Integer> matchingOrderLineItemIds =
                session.execute(
                        "SELECT OL_I_ID FROM order_line " +
                                "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID >= ? AND OL_O_ID < ?",
                        warehouseId,
                        districtId,
                        districtNextOrderId - noOfOrders,
                        districtNextOrderId)
                .all()
                .stream()
                .map(row -> row.getInt(CqlIdentifier.fromCql("OL_I_ID")))
                .distinct()
                .collect(Collectors.toList());

        long count =
                session.execute(
                        session.prepare(
                        "SELECT S_QUANTITY FROM stock WHERE S_W_ID = ? AND S_I_ID IN ?")
                                .bind()
                                .setInt(0, warehouseId)
                                .setList(1, matchingOrderLineItemIds, Integer.class))
                .all()
                .stream()
                .filter(row -> row.getBigDecimal(CqlIdentifier.fromCql("S_QUANTITY")).intValue() < threshold)
                .count();

        System.out.printf("Number of items with stock quantities below the threshold: %d%n", count);
    }
}
