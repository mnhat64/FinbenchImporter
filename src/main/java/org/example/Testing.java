/*

package org.example;

import avro.shaded.com.google.common.collect.Maps;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.LogicalGraphFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class Testing {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String csvFilePath = "src/main/resources/PersonGuaranteePerson.csv";
        LogicalGraphFactory graphFactory = new LogicalGraphFactory(config);

        EPGMVertexFactory vertexFactory = new EPGMVertexFactory();
        EPGMEdgeFactory edgeFactory = new EPGMEdgeFactory();

        CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter('|').withHeader();

        Map<Long, EPGMVertex> vertexMap = new HashMap<>();


        DataSet<Tuple4<Long, Long, String, String>> csvDataSet = env
                .readCsvFile(csvFilePath)
                .fieldDelimiter('|')
                .includeFields(true, true, true, true)
                .ignoreFirstLine()
                .types(Long.class, Long.class, String.class, String.class);


        DataSet<EPGMVertex> verticesDataSet = csvDataSet
                .flatMap((Tuple4<Long, Long, String, String> record, Collector<EPGMVertex> out) -> {
                    Long fromId = record.f0;
                    Long toId = record.f1;

                    EPGMVertex fromVertex = vertexMap.computeIfAbsent(fromId, key -> {
                        EPGMVertex vertex = vertexFactory.createVertex(key.toString());
                        vertex.setId(GradoopId.fromString(addLeadingZeros(Long.toHexString(key))));
                        vertexMap.put(fromId, vertex);
                        return vertex;
                    });

                    EPGMVertex toVertex = vertexMap.computeIfAbsent(toId, key -> {
                        EPGMVertex vertex = vertexFactory.createVertex(key.toString());
                        vertex.setId(GradoopId.fromString(addLeadingZeros(Long.toHexString(key))));
                        vertexMap.put(toId, vertex);
                        return vertex;
                    });
                    out.collect(toVertex);
                    out.collect(fromVertex);
                })
                .returns(TypeInformation.of(EPGMVertex.class))
                .distinct("label");


        DataSet<EPGMEdge> edgesDataSet = csvDataSet.flatMap(
                (Tuple4<Long, Long, String, String> record, Collector<EPGMEdge> out) -> {
                    Long fromId = record.f0;
                    Long toId = record.f1;
                    String relation = record.f3;

                    EPGMEdge edge = edgeFactory.createEdge(relation, GradoopId.fromString(addLeadingZeros(Long.toHexString(fromId))),
                            GradoopId.fromString(addLeadingZeros(Long.toHexString(toId))));
                    out.collect(edge);
                }
        ).returns(TypeInformation.of(EPGMEdge.class));
    }
    public static String addLeadingZeros(String inputHex) {
        int desiredLength = 24;
        int currentLength = inputHex.length();

        if (currentLength >= desiredLength) {
            return inputHex;
        }
        int numberOfZerosToAdd = desiredLength - currentLength;
        StringBuilder paddedHex = new StringBuilder(desiredLength);
        for (int i = 0; i < numberOfZerosToAdd; i++) {
            paddedHex.append('0');
        }
        paddedHex.append(inputHex);
        return paddedHex.toString();
    }
    }




*/
