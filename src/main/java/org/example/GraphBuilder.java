package org.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class GraphBuilder {

    static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    static GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
    static EntitiesReader eR = new EntitiesReader(env);
    static RelationshipReader rR = new RelationshipReader(env);
    static DataSet<ImportVertex<String>> set1;
    static DataSet<ImportVertex<String>> set2;
    static DataSet<ImportEdge<String>> edges;
    static String targetDirectory = "src/main/resources/graphs/";


    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            System.out.print("Enter the directory path for the first entity CSV file: ");
            String entityFile1 = reader.readLine();

            System.out.print("Enter the type of entities in the first CSV file (e.g., Person, Company): ");
            String entityType1 = reader.readLine();

            System.out.print("Enter the directory path for the second entity CSV file: ");
            String entityFile2 = reader.readLine();

            System.out.print("Enter the type of entities in the second CSV file: ");
            String entityType2 = reader.readLine();

            System.out.print("Enter the directory path for the relationship CSV file: ");
            String relationshipFile = reader.readLine();

            createGraph(entityFile1, entityType1, entityFile2, entityType2, relationshipFile);

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createGraph(String entityFile1, String entityType1,
                                    String entityFile2, String entityType2,
                                    String relationshipFile) {
        try {
            switch (entityType1.toLowerCase()) {
                case "person":
                    set1 = eR.readingPerson(entityFile1);
                    break;
                case "company":
                    set1 = eR.readingCompany(entityFile1);
                    break;
                case "loan":
                    set1 = eR.readingLoan(entityFile1);
                    break;
                case "medium":
                    set1 = eR.readingMedium(entityFile1);
                default:
                    System.out.println("Invalid entity type for the first CSV file.");
            }

            switch (entityType2.toLowerCase()) {
                case "person":
                    set2 = eR.readingPerson(entityFile2);
                    break;
                case "company":
                    set2 = eR.readingCompany(entityFile2);
                    break;
                case "loan":
                    set2 = eR.readingLoan(entityFile2);
                    break;
                case "medium":
                    set2 = eR.readingMedium(entityFile2);
                default:
                    System.out.println("Invalid entity type for the second CSV file.");
            }


            String fileNameWithoutExtension = new File(relationshipFile).getName().replaceFirst("[.][^.]+$", "");
            String dotFilePath = targetDirectory + fileNameWithoutExtension + ".dot";

            prepareData(set1, set2, relationshipFile).writeTo(new DOTDataSink(dotFilePath, true, DOTDataSink.DotFormat.HTML));

            runGraphviz(dotFilePath, fileNameWithoutExtension+".pdf");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static LogicalGraph prepareData(DataSet<ImportVertex<String>> set1, DataSet<ImportVertex<String>> set2, String relationshipFile ) throws Exception {
        DataSet<ImportVertex<String>> relevantVertex = rR.extractRelevantVertices(env, relationshipFile, set1, set2);
        edges = rR.readingEdges(relationshipFile);

        GraphDataSource<String> graphDataSource = new GraphDataSource(relevantVertex, edges, config);
        LogicalGraph graph = graphDataSource.getLogicalGraph();

        return graph;
    }

    private static void runGraphviz(String dotFilePath, String outputPath) {
        try {
            // Additional Graphviz parameters
            int dpi = 300;          // DPI for image resolution
            double nodeSep = 0.1;   // Minimum separation between nodes
            double rankSep = 1.5;   // Separation between ranks
            int width = 100;
            int height = 10;

            String command = String.format("dot -Tpdf -Gdpi=%d -Gnodesep=%f -Granksep=%f -Grankdir=TB -Gwidth=%d -Gheight=%d %s -o %s",
                    dpi, nodeSep, rankSep, width, height, dotFilePath, outputPath);
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();

            // Check for errors
            if (process.exitValue() != 0) {
                System.err.println("Error executing Graphviz command.");
            } else {
                System.out.println("Graph exported successfully to " + outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}