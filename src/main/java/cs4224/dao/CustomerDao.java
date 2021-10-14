package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import cs4224.entities.Customer;

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;

@Dao
public interface CustomerDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE C_W_ID = :warehouseId AND C_D_ID = :districtId AND C_ID = :id")
    Row getById(int warehouseId, int districtId, int id);

    @Update(customWhereClause = "C_W_ID = :warehouseId AND C_D_ID = :districtId AND C_ID = :id IF c_ytd_payment = :c_ytd",
            nullSavingStrategy = DO_NOT_SET)
    @StatementAttributes(timeout = "PT10S")
    void updateWhereIdEquals(Customer customer, int warehouseId, int districtId, int id, float c_ytd);

    @Query("SELECT C_FIRST, C_MIDDLE, C_LAST FROM ${qualifiedTableId} WHERE C_W_ID = :warehouseId AND C_D_ID = :districtId " +
            "AND C_ID = :id")
    Row getNameById(int warehouseId, int districtId, int id);

    @Query("SELECT sum(C_BALANCE) as C_BALANCE, sum(C_YTD_PAYMENT) as C_YTD_PAYMENT, sum(C_PAYMENT_CNT) as C_PAYMENT_CNT, " +
            "sum(C_DELIVERY_CNT) as C_DELIVERY_CNT FROM ${qualifiedTableId}")
    @StatementAttributes(timeout = "PT10S")
    Row getState();
}
