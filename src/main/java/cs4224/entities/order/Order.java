package cs4224.entities.order;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
@JsonInclude(Include.NON_NULL)
public class Order {
    private Integer id;
    private Integer warehouseId;
    private Integer districtId;
    private Integer customerId;
    private Integer carrierId;
    private BigDecimal numItems;
    private BigDecimal allLocal;
    private Instant entryDateTime;
}
