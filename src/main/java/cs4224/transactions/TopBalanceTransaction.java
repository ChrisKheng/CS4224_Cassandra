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

public class TopBalanceTransaction extends BaseTransaction {

    private final ExecutorService executorService;
    private final Map<Integer, String> allWarehousesNamesMapping;
    private final PreparedStatement getBalancesOfCustomersQuery;
    private final PreparedStatement getCustomersQuery;
    private final PreparedStatement getDistrictQuery;

    public TopBalanceTransaction(CqlSession session, ExecutorService executorService) {
        super(session);

        this.executorService = executorService;

        this.allWarehousesNamesMapping =
                session.execute("SELECT W_ID, W_NAME FROM warehouse")
                        .all()
                        .stream()
                        .collect(Collectors.toMap(
                                warehouse -> warehouse.getInt(CqlIdentifier.fromCql("W_ID")),
                                warehouse -> warehouse.getString(CqlIdentifier.fromCql("W_NAME"))
                        ));

        this.getBalancesOfCustomersQuery = session.prepare(
                "SELECT C_W_ID, C_BALANCE, C_D_ID, C_ID " +
                        "FROM customer_balance " +
                        "WHERE C_W_ID = :c_w_id " +
                        "ORDER BY C_BALANCE DESC " +
                        "LIMIT :n"
        );

        this.getCustomersQuery = session.prepare(
                "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST " +
                        "FROM customer " +
                        "WHERE C_W_ID = :c_w_id AND C_D_ID = :c_d_id AND C_ID IN :c_ids"
        );

        this.getDistrictQuery = session.prepare(
                "SELECT D_W_ID, D_ID, D_NAME " +
                        "FROM district " +
                        "WHERE D_W_ID = :d_w_id AND D_ID = :d_id"
        );
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        final List<Row> topTenCustomers = this.allWarehousesNamesMapping
                .keySet()
                .stream()
                .map(warehouseId ->
                        session.execute(
                                this.getBalancesOfCustomersQuery
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
                                this.getCustomersQuery
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
                                                this.getDistrictQuery
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

        List<Object> resultsOfTasks = new ParallelExecutor(this.executorService)
                .addTask(topTenCustomersNamesMappingTask)
                .addTask(districtNamesMappingTask)
                .execute();

        final Map<Integer, String> topTenCustomersNamesMapping = (Map<Integer, String>) resultsOfTasks.get(0);
        final Map<Integer, Map<Integer, String>> districtNamesMapping = (Map<Integer, Map<Integer, String>>) resultsOfTasks.get(1);

        topTenCustomers.forEach(customer ->
                System.out.printf(
                        "Name of customer: %s%n" +
                                "Balance of customer's outstanding payment: %.2f%n" +
                                "Warehouse name of customer: %s%n" +
                                "District name of customer: %s%n%n",
                        topTenCustomersNamesMapping.get(customer.getInt(CqlIdentifier.fromCql("C_ID"))),
                        customer.getBigDecimal(CqlIdentifier.fromCql("C_BALANCE")),
                        this.allWarehousesNamesMapping.get(customer.getInt(CqlIdentifier.fromCql("C_W_ID"))),
                        districtNamesMapping
                                .get(customer.getInt(CqlIdentifier.fromCql("C_W_ID")))
                                .get(customer.getInt(CqlIdentifier.fromCql("C_D_ID")))
                )
        );
    }
}
