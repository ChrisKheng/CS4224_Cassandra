package cs4224.entities;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("district")
public class District {

    @CqlName("d_id")
    @PartitionKey(value = 1)
    private Integer id;

    @CqlName("d_w_id")
    @PartitionKey(value = 0)
    private Integer warehouseId;

    @CqlName("d_name")
    private String name;

    @CqlName("d_street_1")
    private String street1;

    @CqlName("d_street_2")
    private String street2;

    @CqlName("d_city")
    private String city;

    @CqlName("d_state")
    private String state;

    @CqlName("d_zip")
    private String zip;

    @CqlName("d_tax")
    private BigDecimal tax;

    @CqlName("d_ytd")
    private BigDecimal amountPaidYTD;

    @CqlName("d_next_o_id")
    private Integer nextOrderId;
}
