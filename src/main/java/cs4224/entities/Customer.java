package cs4224.entities;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("customer")
public class Customer {

    @CqlName("c_id")
    @ClusteringColumn
    private Integer id;

    @CqlName("c_w_id")
    @PartitionKey(value = 0)
    private Integer warehouseId;

    @CqlName("c_d_id")
    @PartitionKey(value = 1)
    private Integer districtId;

    @CqlName("c_first")
    private String firstName;

    @CqlName("c_middle")
    private String middleName;

    @CqlName("c_last")
    private String lastName;

    @CqlName("c_street_1")
    private String street1;

    @CqlName("c_street_2")
    private String street2;

    @CqlName("c_city")
    private String city;

    @CqlName("c_state")
    private String state;

    @CqlName("c_zip")
    private String zip;

    @CqlName("c_phone")
    private String phone;

    @CqlName("c_since")
    private Instant entryCreateDateTime;

    @CqlName("c_credit")
    private String creditStatus;

    @CqlName("c_credit_lim")
    private BigDecimal creditLimit;

    @CqlName("c_discount")
    private BigDecimal discountRate;

    @CqlName("c_balance")
    private BigDecimal balance;

    @CqlName("c_ytd_payment")
    private float paymentYTD;

    @CqlName("c_payment_cnt")
    private Integer numPayments;

    @CqlName("c_delivery_cnt")
    private Integer numDeliveries;

    @CqlName("c_data")
    private String miscData;

}
