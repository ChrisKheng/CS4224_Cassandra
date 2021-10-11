package cs4224.mapper;

import com.datastax.oss.driver.api.core.cql.Row;

import java.math.BigDecimal;
import java.time.Instant;

public final class CQLMapper {

    public Integer mapInt(Row row, String attribute) {
        if (row.getColumnDefinitions().contains(attribute)) {
            return row.getInt(attribute);
        }
        return null;
    }

    public BigDecimal mapBigDecimal(Row row, String attribute) {
        if (row.getColumnDefinitions().contains(attribute)) {
            return row.getBigDecimal(attribute);
        }
        return null;
    }

    public String mapString(Row row, String attribute) {
        if (row.getColumnDefinitions().contains(attribute)) {
            return row.getString(attribute);
        }
        return null;
    }

    public Float mapFloat(Row row, String attribute) {
        if (row.getColumnDefinitions().contains(attribute)) {
            return row.getFloat(attribute);
        }
        return null;
    }

    public Instant mapInstant(Row row, String attribute) {
        if (row.getColumnDefinitions().contains(attribute)) {
            return row.getInstant(attribute);
        }
        return null;
    }
}
