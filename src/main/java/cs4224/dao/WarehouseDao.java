package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import cs4224.entities.Warehouse;

import java.math.BigDecimal;

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;

@Dao
public interface WarehouseDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE W_ID = :id")
    Row getById(int id);

    @Update(customWhereClause = "W_ID = :id IF w_ytd = :w_ytd", nullSavingStrategy = DO_NOT_SET)
    @StatementAttributes(timeout = "PT10S")
    void updateWhereIdEquals(Warehouse warehouse, int id, BigDecimal w_ytd);

    @Query("SELECT W_ID FROM ${qualifiedTableId}")
    ResultSet getAllWarehouseIDs();

    @Query("SELECT sum(W_YTD) as W_YTD FROM ${qualifiedTableId}")
    Row getState();
}
