package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;

@Dao
public interface OrderDao {

    @Query("SELECT O_ID, O_ENTRY_D, O_C_ID FROM ${qualifiedTableId} WHERE O_W_ID = :warehouseId AND O_D_ID = :districtId " +
            "AND O_ID >= :greaterThanId AND O_ID < :lessThanId")
    ResultSet getById(int warehouseId, int districtId, int greaterThanId, int lessThanId);

    @Query("SELECT max(O_ID) as O_ID, sum(O_OL_CNT) as O_OL_CNT FROM ${qualifiedTableId}")
    @StatementAttributes(timeout = "PT10S")
    Row getState();
}
