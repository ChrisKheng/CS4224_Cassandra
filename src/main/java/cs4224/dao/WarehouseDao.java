package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;

import java.math.BigDecimal;

@Dao
public interface WarehouseDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE W_ID = :id")
    Row getById(int id);

    @Query("UPDATE ${qualifiedTableId} SET w_ytd = :updatedYtd WHERE W_ID = :id IF w_ytd = :w_ytd")
    @StatementAttributes(timeout = "PT10S")
    ResultSet updateWhereIdEquals(int id, BigDecimal updatedYtd, BigDecimal w_ytd);

    @Query("SELECT W_ID FROM ${qualifiedTableId}")
    ResultSet getAllWarehouseIDs();

    @Query("SELECT sum(W_YTD) as W_YTD FROM ${qualifiedTableId}")
    Row getState();
}
