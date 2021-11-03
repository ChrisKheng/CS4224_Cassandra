package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;

import java.math.BigDecimal;

@Dao
public interface DistrictDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE D_W_ID = :warehouseId AND D_ID = :id")
    Row getById(int warehouseId, int id);

    @Query("UPDATE ${qualifiedTableId} SET d_ytd = :updatedYtd WHERE D_W_ID = :warehouseId AND D_ID = :id IF d_ytd = :d_ytd")
    @StatementAttributes(timeout = "PT10S")
    ResultSet updateWhereIdEquals(int warehouseId, int id, BigDecimal updatedYtd, BigDecimal d_ytd);

    @Query("SELECT D_NEXT_O_ID FROM ${qualifiedTableId} WHERE D_W_ID = :warehouseId AND D_ID = :id")
    Row getNextOrderId(int warehouseId, int id);

    @Query("SELECT sum(D_YTD) as D_YTD, sum(D_NEXT_O_ID) as D_NEXT_O_ID FROM ${qualifiedTableId}")
    Row getState();
}
