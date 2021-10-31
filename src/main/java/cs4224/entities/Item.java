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

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Entity
@CqlName("item")
public class Item {

    @CqlName("i_id")
    private Integer id;

    @CqlName("i_name")
    private String name;

    public static Item map(Row row) {
        final CQLMapper cqlMapper = new CQLMapper();
        final Item item = new Item();
        item.setName(cqlMapper.mapString(row, "i_name"));
        item.setId(cqlMapper.mapInt(row, "i_id"));
        return item;
    }
}
