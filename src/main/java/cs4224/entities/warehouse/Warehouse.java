package cs4224.entities.warehouse;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("warehouse")
public class Warehouse {

    @CqlName("w_id")
    @PartitionKey
    private Integer id;

    @CqlName("w_name")
    private String name;

    @CqlName("w_street_1")
    private String street1;

    @CqlName("w_street_2")
    private String street2;

    @CqlName("w_city")
    private String city;

    @CqlName("w_state")
    private String state;

    @CqlName("w_zip")
    private String zip;

    @CqlName("w_tax")
    private BigDecimal tax;

    @CqlName("w_ytd")
    private BigDecimal amountPaidYTD;

    public String addressToString() {
        return String.format("street1=%s, street2=%s, city=%s, state=%s, zip=%s",
                street1, street2, city, state, zip);
    }
}
