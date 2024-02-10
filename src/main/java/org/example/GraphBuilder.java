package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.example.datasource.DataSourceExtractor;
import org.example.edge.RelationReader;
import org.example.query.SimpleQuery;
import org.example.vertex.EntitiesReader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;


public class GraphBuilder {

    public static void main (String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        TemporalGradoopConfig tConfig = TemporalGradoopConfig.fromGradoopFlinkConfig(config);


        DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(env);
        Tuple2<DataSet<TemporalEdge>, DataSet<TemporalVertex>> dataSource = dataSourceExtractor.graphDataSourceExtractor("Person", "Loan", "Apply");


        TemporalGraphFactory tgp = new TemporalGraphFactory(tConfig);
        TemporalGraph tg = tgp.fromDataSets(dataSource.f1, dataSource.f0);
        tg.print();



        /*

        String personLoanPath = "/Users/pmnha/TemporalGraph/src/main/resources/relations/PersonApplyLoan.csv";

        DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(env);
        Tuple2<DataSet<TemporalEdge>, DataSet<TemporalVertex>> dataSource = dataSourceExtractor.graphDataSourceExtractor("Person", "Loan", personLoanPath);

        String targetDirectory = "src/main/resources/graphs/";
        String fileNameWithoutExtension = new File(personLoanPath).getName().replaceFirst("[.][^.]+$", "");
        String dotFilePath = targetDirectory + fileNameWithoutExtension;

        TemporalGraphFactory tgp = new TemporalGraphFactory(tConfig);
        TemporalGraph graph = tgp.fromDataSets(dataSource.f1, dataSource.f0);

        TemporalCSVDataSink ds = new TemporalCSVDataSink(dotFilePath, config);

        graph.writeTo(ds);
        graph.print();

        JobExecutionResult result = env.execute("Export TemporalGraph to PDF");
        System.out.println("Job took " + result.getNetRuntime() + " milliseconds");
        */

    }

}