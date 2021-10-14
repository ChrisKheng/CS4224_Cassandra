package cs4224.entities;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.fasterxml.jackson.annotation.JsonInclude;
import cs4224.mapper.CQLMapper;
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

    public String toAddress() {
        return String.format(" Address (street_1, street_2, city, state, zip) : (%s, %s, %s, %s, %s)",
                street1, street2, city, state, zip);
    }

    public static District map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final District district = new District();
        district.setId(cqlMapper.mapInt(row, "d_id"));
        district.setWarehouseId(cqlMapper.mapInt(row,"d_w_id"));
        district.setName(cqlMapper.mapString(row, "d_name"));
        district.setStreet1(cqlMapper.mapString(row, "d_street_1"));
        district.setStreet2(cqlMapper.mapString(row, "d_street_2"));
        district.setCity(cqlMapper.mapString(row, "d_city"));
        district.setState(cqlMapper.mapString(row, "d_state"));
        district.setZip(cqlMapper.mapString(row, "d_zip"));
        district.setTax(cqlMapper.mapBigDecimal(row, "d_tax"));
        district.setAmountPaidYTD(cqlMapper.mapBigDecimal(row, "d_ytd"));
        district.setNextOrderId(cqlMapper.mapInt(row, "d_next_o_id"));
        return district;
    }
}
