package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class FinbenchImporter {

    public static void main(String[] args) throws Exception {
        String csvFilePath = "src/main/resources/PersonGuaranteePerson.csv";
        runFlinkJob(csvFilePath);
    }

    public static void runFlinkJob(String csvFilePath) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        //reads in the csv file
        DataSet<Tuple4<String, String, String, String>> csvDataSet = CsvReaderHelper.readCsvFile(env, csvFilePath);

        //create a dataset of all id without duplications
        DataSet<String> idDataSet = csvDataSet.map(tuple -> Tuple2.of(tuple.f0, tuple.f1))
                .returns(new TypeHint<Tuple2<String, String>>(){})
                .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public void flatMap(Tuple2<String, String> id, Collector<String> out) {
                        out.collect(id.f0);
                        out.collect(id.f1);
                    }
                }).distinct();

        //import vertex for each id from the newly generated dataset
        DataSet<ImportVertex<String>> importVertices = ImportVertexHelper.importVertex(idDataSet);

        //import edge for each line in the original csv file
        DataSet<ImportEdge<String>> importEdges = ImportEdgeHelper.createEdgesFromCSV(csvDataSet);

        GraphDataSource<String> graphDataSource = new GraphDataSource(importVertices, importEdges, config);
        LogicalGraph graph = graphDataSource.getLogicalGraph();
        graph.writeTo(new DOTDataSink("/Users/pmnha/TemporalGraph/src/main/resources/graph.dot", true, DOTDataSink.DotFormat.HTML));
        env.execute("Testing 2");
    }
}
