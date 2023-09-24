package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

public class ImportVertexHelper {
    public static DataSet<ImportVertex<String>> importVertex(DataSet<String> stringDataSet) {
        return stringDataSet.map(new MapFunction<String, ImportVertex<String>>() {
            @Override
            public ImportVertex<String> map(String id) throws Exception {
                String vertexId = id;
                String vertexLabel = "Person " + id;
                return new ImportVertex<>(vertexId, vertexLabel);
            }
        });
    }
}
