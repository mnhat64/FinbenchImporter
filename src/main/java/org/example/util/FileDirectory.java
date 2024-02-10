package org.example.util;

public class FileDirectory {

    private static final String BASE_DIRECTORY = "/Users/pmnha/TemporalGraph/src/main/resources";
    private static final String ENTITIES_DIRECTORY = BASE_DIRECTORY + "/entities";
    private static final String EDGE_DIRECTORY = BASE_DIRECTORY + "/relations";
    private static final String GRAPH_DIRECTORY = BASE_DIRECTORY + "/graph";


    public static String getEntitiesDirectory(String type) {
        return ENTITIES_DIRECTORY + "/" + type.toLowerCase() + ".csv";
    }

    public static String getRelationDirectory(String sourceType, String relation, String targetType){
        return GRAPH_DIRECTORY + "/" + sourceType + relation + targetType + ".csv";
    }
}