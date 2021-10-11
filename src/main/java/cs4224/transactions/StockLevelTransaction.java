package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.util.List;
import java.util.stream.Collectors;

public class StockLevelTransaction extends BaseTransaction {

    private final PreparedStatement getNextOrderIdOfDistrictQuery;
    private final PreparedStatement getItemIdsOfOrdersQuery;
    private final PreparedStatement getStockQuantitiesOfItemsQuery;


    public StockLevelTransaction(CqlSession session) {
        super(session);

        getNextOrderIdOfDistrictQuery = session.prepare(
                "SELECT D_NEXT_O_ID " +
                        "FROM district " +
                        "WHERE D_W_ID = :d_w_id AND D_ID = :d_id"
        );

        getItemIdsOfOrdersQuery = session.prepare(
                "SELECT OL_I_ID " +
                        "FROM order_line " +
                        "WHERE OL_W_ID = :ol_w_id AND OL_D_ID = :ol_d_id " +
                        "AND OL_O_ID >= :ol_o_id_min AND OL_O_ID <= :ol_o_id_max"
        );

        getStockQuantitiesOfItemsQuery = session.prepare(
                "SELECT S_QUANTITY " +
                        "FROM stock " +
                        "WHERE S_W_ID = :s_w_id AND S_I_ID IN :s_i_ids"
        );

    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final int warehouseId = Integer.parseInt(parameters[1]);
        final int districtId = Integer.parseInt(parameters[2]);
        final int threshold = Integer.parseInt(parameters[3]);
        final int numberOfOrders = Integer.parseInt(parameters[4]);

        final int districtNextOrderId = session.execute(
                getNextOrderIdOfDistrictQuery
                        .boundStatementBuilder()
                        .setInt("d_w_id", warehouseId)
                        .setInt("d_id", districtId)
                        .build()
        ).one().getInt(CqlIdentifier.fromCql("D_NEXT_O_ID"));

        final List<Integer> matchingOrderLineItemIds = session
                .execute(
                        getItemIdsOfOrdersQuery
                                .boundStatementBuilder()
                                .setInt("ol_w_id", warehouseId)
                                .setInt("ol_d_id", districtId)
                                .setInt("ol_o_id_min", districtNextOrderId - numberOfOrders)
                                .setInt("ol_o_id_max", districtNextOrderId - 1)
                                .build()
                )
                .all()
                .parallelStream()
                .map(row -> row.getInt(CqlIdentifier.fromCql("OL_I_ID")))
                .distinct()
                .collect(Collectors.toList());

        final long count = session
                .execute(
                        getStockQuantitiesOfItemsQuery
                                .boundStatementBuilder()
                                .setInt("s_w_id", warehouseId)
                                .setList("s_i_ids", matchingOrderLineItemIds, Integer.class)
                                .build()
                )
                .all()
                .parallelStream()
                .filter(row -> row.getBigDecimal(CqlIdentifier.fromCql("S_QUANTITY")).intValue() < threshold)
                .count();

        System.out.printf("Number of items with stock quantities below the threshold: %d%n", count);
    }
}
