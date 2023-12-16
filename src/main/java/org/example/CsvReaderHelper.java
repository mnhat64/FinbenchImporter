package org.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;

public class CsvReaderHelper {


    public static DataSet<Tuple4<String, String, String, String>> readCsvFile(ExecutionEnvironment env, String filePath) {
        return env
                .readCsvFile(filePath)
                .fieldDelimiter('|')
                .includeFields(true, true, true, true)
                .ignoreFirstLine()
                .types(String.class, String.class, String.class, String.class);
    }

}