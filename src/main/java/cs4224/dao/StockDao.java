package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;

@Dao
public interface StockDao {

    @Query("SELECT sum(S_QUANTITY) as S_QUANTITY, sum(S_YTD) as S_YTD, sum(S_ORDER_CNT) as S_ORDER_CNT, " +
            "sum(S_REMOTE_CNT) as S_REMOTE_CNT FROM ${qualifiedTableId} WHERE S_W_ID = :warehouseId")
    @StatementAttributes(timeout = "PT10S")
    Row getState(int warehouseId);
}
