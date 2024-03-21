package org.example;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.example.datasource.DataSourceExtractor;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;


public class GraphBuilder {
    public static void main (String[] args) throws Exception {
        Options cliOption = new Options();
        cliOption.addOption(new Option("i", "inputPath", true, "Input path. (REQUIRED)"));
        cliOption.addOption(new Option("o", "outputPath", true, "Output path. (REQUIRED)"));

        CommandLine parsedOptions = new DefaultParser().parse(cliOption, args);
        if (!(parsedOptions.hasOption('i') && parsedOptions.hasOption('o'))) {
            System.err.println("No input- and output-path given.");
            System.err.println("See --help for more infos.");
            return;
        }
        final String inputPath = parsedOptions.getOptionValue('i');
        final String outputPath = parsedOptions.getOptionValue('o');



        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        TemporalGradoopConfig tConfig = TemporalGradoopConfig.fromGradoopFlinkConfig(config);
        DataSourceExtractor dS = new DataSourceExtractor(env);

        Tuple2<DataSet<TemporalVertex>, DataSet<TemporalEdge>> dataSource = dS.readingFinbench(inputPath);
        TemporalGraph tg = tConfig.getTemporalGraphFactory().fromDataSets(dataSource.f0, dataSource.f1);

        tg.writeTo(new TemporalCSVDataSink(outputPath, config), true);

        env.execute("Import Finbench in Gradoop");
    }
}