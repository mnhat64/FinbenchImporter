package org.example.datasource;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

public class PropertyKeySelector implements KeySelector<TemporalVertex, String> {
    @Override
    public String getKey(TemporalVertex vertex) {
        return vertex.getId().toString();
    }
}
