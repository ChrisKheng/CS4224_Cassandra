package cs4224.mapper;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import cs4224.dao.DistrictDao;

@Mapper
public interface DistrictMapper {

    @DaoFactory
    DistrictDao dao(@DaoTable String table);
}
