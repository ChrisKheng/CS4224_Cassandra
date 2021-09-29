package cs4224.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import cs4224.entities.Customer;

import static com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy.DO_NOT_SET;

@Dao
public interface CustomerDao {

    @Query("SELECT * FROM ${qualifiedTableId} WHERE C_W_ID = :c_w_id AND C_D_ID = :c_d_id AND C_ID = :c_id")
    Customer getById(@CqlName("c_w_id") int warehouseId, @CqlName("c_d_id") int districtId, @CqlName("c_id") int id);

    @Update(customWhereClause = "C_W_ID = :c_w_id AND C_D_ID = :c_d_id AND C_ID = :c_id",
            nullSavingStrategy = DO_NOT_SET)
    void updateWhereIdEquals(Customer customer, @CqlName("c_w_id") int warehouseId, @CqlName("c_d_id") int districtId,
                             @CqlName("c_id") int id);
}
