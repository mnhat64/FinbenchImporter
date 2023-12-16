package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

import java.io.Serializable;
import java.util.List;

public class RelationshipReader implements Serializable {

    ExecutionEnvironment env;

    public RelationshipReader (ExecutionEnvironment env){
        this.env = env;
    }

    public DataSet<ImportEdge<String>> readingEdges(String filePath) {
        return (DataSet<ImportEdge<String>>) env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter('|')
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {

                    String sourceId = "PE" + record.f0;
                    String targetId = "LO" + record.f1;
                    String edgeId = "Edge-" + sourceId + "-" + targetId;

                    String createTime = record.f2;
                    String org = record.f3;

                    Properties edgeProperties = Properties.create();
                    edgeProperties.set("CreateTime", createTime);
                    edgeProperties.set("Org", org);

                    // Specify the generic type parameters for ImportEdge
                    return new ImportEdge<String>(edgeId, sourceId, targetId, "Took", edgeProperties);
                }).returns(TypeInformation.of(new TypeHint<ImportEdge<String>>() {}));
    }



    public static DataSet<ImportVertex<String>> extractRelevantVertices(
            ExecutionEnvironment env,
            String edgeFilePath,
            DataSet<ImportVertex<String>> fromVertex,
            DataSet<ImportVertex<String>> toVertex) throws Exception {

        DataSet<Tuple4<String, String, String, String>> csvDataSet = CsvReaderHelper.readCsvFile(env, edgeFilePath);
        DataSet<String> relevantIds = csvDataSet.map(tuple -> Tuple2.of(tuple.f0, tuple.f1))
                .returns(new TypeHint<Tuple2<String, String>>(){})
                .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, String> id, Collector<String> out) {
                        out.collect("PE" + id.f0);
                        out.collect("LO" + id.f1);
                    }
                }).distinct();

        DataSet<ImportVertex<String>> joinedVertices = fromVertex
                .union(toVertex);

        DataSet<String> uniqueRelevantIds = relevantIds.distinct();
        final java.util.Set<String> relevantIdsSet = new java.util.HashSet<>(uniqueRelevantIds.collect());
        return joinedVertices.filter(new FilterFunction<ImportVertex<String>>() {
            @Override
            public boolean filter(ImportVertex<String> vertex) {
                // Keep only the vertices with IDs in the relevant set
                return relevantIdsSet.contains(vertex.getId());
            }
        });
    }
    public static DataSet<String> extractRelevantIds(ExecutionEnvironment env, String filepath) throws Exception {
        DataSet<Tuple4<String, String, String, String>> csvDataSet = CsvReaderHelper.readCsvFile(env, filepath);
        DataSet<String> idDataSet = csvDataSet.map(tuple -> Tuple2.of(tuple.f0, tuple.f1))
                .returns(new TypeHint<Tuple2<String, String>>(){})
                .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, String> id, Collector<String> out) {
                        out.collect("PE" + id.f0);
                        out.collect("LO" + id.f1);
                    }
                }).distinct();
        return idDataSet;
    }
}
