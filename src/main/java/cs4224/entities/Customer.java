package cs4224.entities;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.fasterxml.jackson.annotation.JsonInclude;
import cs4224.mapper.CQLMapper;
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
    @ToString.Exclude
    private Float paymentYTD;

    @CqlName("c_payment_cnt")
    @ToString.Exclude
    private Integer numPayments;

    @CqlName("c_delivery_cnt")
    @ToString.Exclude
    private Integer numDeliveries;

    @CqlName("c_data")
    @ToString.Exclude
    private String miscData;

    public String toSpecifier() {
        return String.format("(%d, %d, %d)", warehouseId, districtId, id);
    }

    public static Customer map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final Customer customer = new Customer();
        customer.setId(cqlMapper.mapInt(row, "c_id"));
        customer.setWarehouseId(cqlMapper.mapInt(row,"c_w_id"));
        customer.setDistrictId(cqlMapper.mapInt(row,"c_d_id"));
        customer.setFirstName(cqlMapper.mapString(row, "c_first"));
        customer.setMiddleName(cqlMapper.mapString(row, "c_middle"));
        customer.setLastName(cqlMapper.mapString(row, "c_last"));
        customer.setStreet1(cqlMapper.mapString(row, "c_street_1"));
        customer.setStreet2(cqlMapper.mapString(row, "c_street_2"));
        customer.setCity(cqlMapper.mapString(row, "c_city"));
        customer.setState(cqlMapper.mapString(row, "c_state"));
        customer.setZip(cqlMapper.mapString(row, "c_zip"));
        customer.setPhone(cqlMapper.mapString(row, "c_phone"));
        customer.setEntryCreateDateTime(cqlMapper.mapInstant(row, "c_since"));
        customer.setCreditStatus(cqlMapper.mapString(row, "c_credit"));
        customer.setCreditLimit(cqlMapper.mapBigDecimal(row, "c_credit_lim"));
        customer.setDiscountRate(cqlMapper.mapBigDecimal(row, "c_discount"));
        customer.setBalance(cqlMapper.mapBigDecimal(row, "c_balance"));
        customer.setPaymentYTD(cqlMapper.mapFloat(row, "c_ytd_payment"));
        customer.setNumPayments(cqlMapper.mapInt(row, "c_payment_cnt"));
        customer.setNumDeliveries(cqlMapper.mapInt(row, "c_delivery_cnt"));
        customer.setMiscData(cqlMapper.mapString(row, "c_data"));

        return customer;
    }
}
