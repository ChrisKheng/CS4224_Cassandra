package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;

import java.math.BigDecimal;

@Dao
public interface OrderLineDao {

    @Query("SELECT max(OL_QUANTITY) as OL_QUANTITY FROM ${qualifiedTableId} WHERE OL_W_ID = :warehouseId AND " +
            "OL_D_ID = :districtId AND OL_O_ID = :orderId")
    Row getOLQuantity(int warehouseId, int districtId, int orderId);

    @Query("SELECT OL_I_ID FROM ${qualifiedTableId} WHERE OL_W_ID = :warehouseId AND OL_D_ID = :districtId " +
            "AND OL_O_ID = :orderId AND OL_QUANTITY = :orderLineQuantity ALLOW FILTERING")
    ResultSet getOLItemId(int warehouseId, int districtId, int orderId, BigDecimal orderLineQuantity);

    @Query("SELECT sum(OL_AMOUNT) as OL_AMOUNT, sum(OL_QUANTITY) as OL_QUANTITY FROM ${qualifiedTableId} " +
            "where OL_W_ID = :warehouseId ALLOW FILTERING")
    @StatementAttributes(timeout = "PT20S")
    Row getState(int warehouseId);
}
