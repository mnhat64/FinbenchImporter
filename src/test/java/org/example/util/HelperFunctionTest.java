package org.example.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class HelperFunctionTest {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final TemporalVertexFactory vertexFactory = new TemporalVertexFactory();

    @Test
    public void convertTimeToUnix() {
    }

    @Test
    public void generateIdPairs() throws Exception {
        Properties prop1 = Properties.create();
        prop1.set("ID", "1");

        Properties prop2 = Properties.create();
        prop1.set("ID", "2");

        Properties prop3 = Properties.create();
        prop1.set("ID", "3");

        Properties prop4 = Properties.create();
        prop1.set("ID", "4");

        Properties prop5 = Properties.create();
        prop1.set("ID", "5");

        Properties prop6 = Properties.create();
        prop1.set("ID", "6");

        GradoopIdSet idSet = new GradoopIdSet();


        DataSet<TemporalVertex> sourceVertices = env.fromElements(
                new TemporalVertex(GradoopId.fromString("65fb49ae8fb96e4bd359595e"), "Person", prop1, idSet, null, null),
                new TemporalVertex(GradoopId.fromString("65fb49ae8fb96e4bd3596033"), "Person", prop2, idSet, null, null),
                new TemporalVertex(GradoopId.fromString("65fb49ae8fb96e4bd3596036"), "Person", prop3, idSet, null, null)
        );

        DataSet<TemporalVertex> targetVertices = env.fromElements(
                new TemporalVertex(GradoopId.fromString("65fb49ae8fb96e4bd3595957"), "Loan", prop4, idSet, null, null),
                new TemporalVertex(GradoopId.fromString("65fb49ae8fb96e4bd3596038"), "Loan", prop5, idSet, null, null),
                new TemporalVertex(GradoopId.fromString("65fb49ae8fb96e4bd3596039"), "Loan", prop6, idSet, null, null)
        );

        DataSet<Tuple2<String, GradoopId>> idPairs = HelperFunction.generateIdPairs(sourceVertices, targetVertices);

    }
}