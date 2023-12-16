package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

import java.util.UUID;

public class ImportEdgeHelper {
    public static DataSet<ImportEdge<String>> createEdgesFromCSV(
            DataSet<Tuple4<String, String, String, String>> edgeCSV) {
        return edgeCSV.map(new MapFunction<Tuple4<String, String, String, String>, ImportEdge<String>>() {
            @Override
            public ImportEdge<String> map(Tuple4<String, String, String, String> csvRecord) throws Exception {
                String fromId = csvRecord.f0;
                String toId = csvRecord.f1;
                String relation = "Guaranteed: " + csvRecord.f3;
                String edgeId = UUID.randomUUID().toString();
                return new ImportEdge<>(edgeId, fromId, toId, relation);
            }
        });
    }
}
