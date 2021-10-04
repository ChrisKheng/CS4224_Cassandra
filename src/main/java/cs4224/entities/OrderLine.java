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

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("order_line")
public class OrderLine {

    @CqlName("ol_i_id")
    private Integer itemId;

    @CqlName("ol_quantity")
    private BigDecimal quantity;

    public static OrderLine map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final OrderLine orderLine = new OrderLine();
        orderLine.setItemId(cqlMapper.mapInt(row, "ol_i_id"));
        orderLine.setQuantity(cqlMapper.mapBigDecimal(row, "ol_quantity"));
        return orderLine;
    }
}
