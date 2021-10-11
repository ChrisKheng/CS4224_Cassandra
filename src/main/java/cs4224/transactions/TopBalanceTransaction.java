package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import cs4224.ParallelExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TopBalanceTransaction extends BaseTransaction {

    private final ExecutorService executorService;
    private final PreparedStatement getBalancesOfCustomersQuery;
    private final PreparedStatement getCustomersQuery;
    private final PreparedStatement getWarehouseQuery;
    private final PreparedStatement getDistrictQuery;

    public TopBalanceTransaction(CqlSession session, ExecutorService executorService) {
        super(session);
        this.executorService = executorService;

        getBalancesOfCustomersQuery = session.prepare(
                "SELECT C_W_ID, C_BALANCE, C_D_ID, C_ID " +
                        "FROM customer_balance " +
                        "WHERE C_W_ID = :c_w_id " +
                        "ORDER BY C_BALANCE DESC " +
                        "LIMIT :n"
        );

        getCustomersQuery = session.prepare(
                "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST " +
                        "FROM customer " +
                        "WHERE C_W_ID = :c_w_id AND C_D_ID = :c_d_id AND C_ID IN :c_ids"
        );

        getWarehouseQuery = session.prepare(
                "SELECT W_ID, W_NAME " +
                        "FROM warehouse " +
                        "WHERE W_ID = :w_id"
        );

        getDistrictQuery = session.prepare(
                "SELECT D_W_ID, D_ID, D_NAME " +
                        "FROM district " +
                        "WHERE D_W_ID = :d_w_id AND D_ID = :d_id"
        );
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final List<Row> topTenCustomers = IntStream.rangeClosed(1, 10).boxed()
                .map(warehouseId ->
                        session.execute(
                                getBalancesOfCustomersQuery
                                    .boundStatementBuilder()
                                    .setInt("c_w_id", warehouseId)
                                    .setInt("n", 10)
                                    .build()
                        ).all()
                )
                .flatMap(List<Row>::stream)
                .sorted(
                        Comparator.comparing(
                                (Row customer) -> customer.getBigDecimal(CqlIdentifier.fromCql("C_BALANCE"))
                        ).reversed()
                )
                .limit(10)
                .collect(Collectors.toList());

        final Map<Integer, Map<Integer, List<Row>>> groupedTopTenCustomers = topTenCustomers.stream().collect(
                Collectors.groupingBy(customer -> customer.getInt(CqlIdentifier.fromCql("C_W_ID")),
                        Collectors.groupingBy(customer -> customer.getInt(CqlIdentifier.fromCql("C_D_ID")))
                )
        );

        final Callable<Object> topTenCustomersNamesMappingTask = () -> groupedTopTenCustomers
                .values()
                .stream()
                .flatMap(groupsOfCustomers -> groupsOfCustomers.values().stream())
                .flatMap(groupOfCustomers ->
                        session.execute(
                                getCustomersQuery
                                        .boundStatementBuilder()
                                        .setInt(
                                                "c_w_id",
                                                groupOfCustomers.get(0).getInt(CqlIdentifier.fromCql("C_W_ID"))
                                        )
                                        .setInt(
                                                "c_d_id",
                                                groupOfCustomers.get(0).getInt(CqlIdentifier.fromCql("C_D_ID"))
                                        )
                                        .setList(
                                                "c_ids",
                                                groupOfCustomers
                                                        .stream()
                                                        .map(customer ->
                                                                customer.getInt(CqlIdentifier.fromCql("C_ID"))
                                                        )
                                                        .collect(Collectors.toList()),
                                                Integer.class
                                        )
                                        .build()
                        ).all().stream()
                )
                .collect(Collectors.toMap(
                        customer -> customer.getInt(CqlIdentifier.fromCql("C_ID")),
                        customer -> String.join(
                                " ",
                                customer.getString(CqlIdentifier.fromCql("C_FIRST")),
                                customer.getString(CqlIdentifier.fromCql("C_MIDDLE")),
                                customer.getString(CqlIdentifier.fromCql("C_LAST"))
                        )
                ));

        final Callable<Object> warehousesNamesMappingTask = () -> groupedTopTenCustomers
                .keySet()
                .stream()
                .map(warehouseId ->
                        session.execute(
                                getWarehouseQuery
                                        .boundStatementBuilder()
                                        .setInt(CqlIdentifier.fromCql("w_id"), warehouseId)
                                        .build()
                        ).one()
                )
                .collect(Collectors.toMap(
                        warehouse -> warehouse.getInt(CqlIdentifier.fromCql("W_ID")),
                        warehouse -> warehouse.getString(CqlIdentifier.fromCql("W_NAME"))
                ));

        final Callable<Object> districtNamesMappingTask = () -> groupedTopTenCustomers
                .keySet()
                .stream()
                .flatMap(warehouseId ->
                        groupedTopTenCustomers
                                .get(warehouseId)
                                .keySet()
                                .stream()
                                .map(districtId ->
                                        session.execute(
                                                getDistrictQuery
                                                        .boundStatementBuilder()
                                                        .setInt(CqlIdentifier.fromCql("d_w_id"), warehouseId)
                                                        .setInt(CqlIdentifier.fromCql("d_id"), districtId)
                                                        .build()
                                        ).one()
                                )
                )
                .collect(Collectors.groupingBy(
                        district -> district.getInt(CqlIdentifier.fromCql("D_W_ID")),
                        Collectors.toMap(
                                district -> district.getInt(CqlIdentifier.fromCql("D_ID")),
                                district -> district.getString(CqlIdentifier.fromCql("D_NAME"))
                        )
                ));

        List<Object> resultsOfTasks = new ParallelExecutor(executorService)
                .addTask(topTenCustomersNamesMappingTask)
                .addTask(warehousesNamesMappingTask)
                .addTask(districtNamesMappingTask)
                .execute();

        final Map<Integer, String> topTenCustomersNamesMapping = (Map<Integer, String>) resultsOfTasks.get(0);
        final Map<Integer, String> warehousesNamesMapping = (Map<Integer, String>) resultsOfTasks.get(1);
        final Map<Integer, Map<Integer, String>> districtNamesMapping = (Map<Integer, Map<Integer, String>>) resultsOfTasks.get(2);

        topTenCustomers.forEach(customer ->
                System.out.printf(
                        "Name of customer: %s%n" +
                                "Balance of customer's outstanding payment: %.2f%n" +
                                "Warehouse name of customer: %s%n" +
                                "District name of customer: %s%n%n",
                        topTenCustomersNamesMapping.get(customer.getInt(CqlIdentifier.fromCql("C_ID"))),
                        customer.getBigDecimal(CqlIdentifier.fromCql("C_BALANCE")),
                        warehousesNamesMapping.get(customer.getInt(CqlIdentifier.fromCql("C_W_ID"))),
                        districtNamesMapping
                                .get(customer.getInt(CqlIdentifier.fromCql("C_W_ID")))
                                .get(customer.getInt(CqlIdentifier.fromCql("C_D_ID")))
                )
        );
    }
}
