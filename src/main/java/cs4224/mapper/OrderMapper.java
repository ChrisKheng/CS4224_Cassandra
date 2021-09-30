package cs4224.mapper;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import cs4224.dao.CustomerDao;
import cs4224.dao.OrderDao;

@Mapper
public interface OrderMapper {

    @DaoFactory
    OrderDao dao(@DaoTable String table);
}
