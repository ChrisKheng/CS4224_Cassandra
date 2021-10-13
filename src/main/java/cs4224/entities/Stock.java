package cs4224.entities;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
import cs4224.mapper.CQLMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.math.BigDecimal;

@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("Stock")
public class Stock {

    @CqlName("s_quantity")
    private BigDecimal quantity;

    @CqlName("s_ytd")
    private BigDecimal ytdQuantity;

    @CqlName("s_order_cnt")
    private Integer orderCount;

    @CqlName("s_remote_cnt")
    private Integer remoteOrderCount;

    @SuppressWarnings("ConstantConditions")
    public static Stock map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final Stock stock = new Stock();
        stock.setQuantity(cqlMapper.mapBigDecimal(row, "s_quantity"));
        stock.setYtdQuantity(cqlMapper.mapBigDecimal(row, "s_ytd"));
        stock.setOrderCount(cqlMapper.mapInt(row, "s_order_cnt"));
        stock.setRemoteOrderCount(cqlMapper.mapInt(row, "s_remote_cnt"));
        return stock;
    }
}
