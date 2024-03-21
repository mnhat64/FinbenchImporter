package org.example.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class HelperFunction {

    public static long convertTimeToUnix(String timestamp) throws ParseException {
        int millisIndex = timestamp.lastIndexOf('.');
        int missingZeros = 3 - (timestamp.length() - millisIndex - 1);

        if (millisIndex == -1) {
            timestamp = timestamp + ".000";
        } else if (missingZeros > 0) {
            StringBuilder zeros = new StringBuilder();
            for (int i = 0; i < missingZeros; i++) {
                zeros.append('0');
            }
            timestamp = timestamp.substring(0, millisIndex + 1) + zeros + timestamp.substring(millisIndex + 1);
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date parsedDate = dateFormat.parse(timestamp);
        return parsedDate.getTime();
    }

    public static DataSet<Tuple2<String, GradoopId>>  generateIdPairs(DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        return sourceVertices
                .union(targetVertices)
                .map(vertex -> new Tuple2<>(vertex.getPropertyValue("ID").toString(), vertex.getId()))
                .returns(new TypeHint<Tuple2<String, GradoopId>>() {});
    }

}
