package cs4224.dao;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;

import java.util.List;

@Dao
public interface ItemDao {

    @Query("SELECT I_ID, I_NAME FROM ${qualifiedTableId} WHERE I_ID IN :ids")
    ResultSet getNameById(List<Integer> ids);

}
