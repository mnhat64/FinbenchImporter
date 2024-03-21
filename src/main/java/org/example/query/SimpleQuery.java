package org.example.query;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.example.edge.RelationReader;
import org.example.vertex.EntitiesReader;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.tpgm.EdgeToTemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.security.acl.Group;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;


public class SimpleQuery {
    EntitiesReader entitiesReader;
    RelationReader relationReader;

    public SimpleQuery(ExecutionEnvironment env){
        this.entitiesReader = new EntitiesReader(env);
        this.relationReader = new RelationReader(env);
    }

    public void extractAccountQuery(String id) throws Exception {
        DataSet<TemporalVertex> accounts =  null;
        List<TemporalVertex> filteredAccounts = accounts
                .filter(new FilterFunction<TemporalVertex>() {
                    @Override
                    public boolean filter(TemporalVertex vertex) {
                        return vertex.getProperties().get("ID").toString().equals(id);
                    }
                })
                .collect();
        if (!filteredAccounts.isEmpty()) {
            Properties properties = filteredAccounts.get(0).getProperties();
            for (Property property : properties) {
                System.out.println(property.getKey() + ": " + property.getValue());
            }
        } else {
            System.out.println("No account found with ID: " + id);
        }
    }

    public TemporalGraph transferInOut (TemporalGraph source, String accountId, LocalDateTime startTime, LocalDateTime endTime) throws Exception {
        long startTimestamp = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTimestamp = endTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        TemporalGraph workgraph = source.fromTo(startTimestamp, endTimestamp).edgeInducedSubgraph(e -> e.getProperties().get("SourceID").equals(PropertyValue.create(accountId)) || e.getProperties().get("TargetID").equals(PropertyValue.create(accountId)));
        List<KeyFunction<TemporalVertex, ?>> vertexKeys = Arrays.asList(GroupingKeys.property("ID"));
        List<KeyFunction<TemporalEdge, ?>> edgeKeys = Arrays.asList(GroupingKeys.property("SourceID"), GroupingKeys.property("TargetID"));

        TemporalGraph summarizedGraph = workgraph.callForGraph(new KeyedGrouping<>(
                vertexKeys,
                null,
                edgeKeys,
                Arrays.asList(new SumEdgeProperty("Amount"), new MaxEdgeProperty("Amount"), new Count("Amount"))));

        return summarizedGraph;
    }

    public double calculateRatio(TemporalGraph graph, String accountId, LocalDateTime startTime, LocalDateTime endTime, double amountThreshold) throws Exception {
        long startTimestamp = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTimestamp = endTime.toInstant(ZoneOffset.UTC).toEpochMilli();

        TemporalGraph subGraph = graph.fromTo(startTimestamp, endTimestamp).vertexInducedSubgraph(new ByLabel<>("Account"));
        DataSet<TemporalEdge> transferIns = subGraph.getEdges()
                .filter(edge -> edge.getTargetId().toString().equals(accountId) // Assuming TargetID is the given account
                        && edge.getPropertyValue("Amount").getDouble() >= amountThreshold);

        long blockedCount = transferIns
                .join(graph.getVertices())
                .where(edge -> edge.getSourceId())
                .equalTo(vertex -> vertex.getId())
                .filter(pair -> pair.f1.getPropertyValue("IsBlocked").getBoolean())
                .count();

        //patern matching

        long totalCount = transferIns.count();

        if (totalCount > 0) {
            double ratio = (double) blockedCount / (totalCount - blockedCount);
            return Math.round(ratio * 1000) / 1000.0; // Round to 3 decimal places
        } else {
            return -1; // No transfer-ins to the given account
        }
    }

    /*
    (000d000040c5000000000009,000d000040de00000000000d)
(000d000040c5000000000009,000d000040de00000000000d)
(000d000001b00000000002f9,000d000001b00000000002f9)
(000d0000416c00000000016f,000d000042c000000000016d)
(000d00000390000000000006,000d000002a6000000000004)
(000d000043c9000000000071,000d0000425400000000005b)
(000d0000425400000000005b,000d000043c9000000000071)
(000d0000416c00000000016f,000d000042c000000000016d)
(000d000001ef000000000269,000d0000034900000000026e)
(000d000043ed000000000169,000d000042aa00000000016b)
(000d000043ed000000000169,000d000042aa00000000016b)
(000d00004370000000000115,000d00004354000000000117)
(000d000001600000000001e6,000d0000039e000000000206)
(000d000002b00000000002c7,000d000000e30000000002c6)
(000d000002b00000000002c7,000d000000e30000000002c6)
(000d000003f8000000000359,000d000002a200000000035b)
(000d000003e8000000000003,000d00000403000000000002)
(000d000003f8000000000359,000d000002a200000000035b)
(000d0000416b000000000069,000d000043ee00000000007f)
(000d0000416b000000000069,000d000043ee00000000007f)
(000d00004370000000000115,000d00004354000000000117)
(000d00000403000000000002,000d000003e8000000000003)
(000d000042f60000000001ac,000d000042170000000001aa)
(000d000003230000000002b2,000d000003ce0000000002ae)
(000d000000e30000000002c6,000d000002b00000000002c7)
(000d0000435a000000000002,000d000042fa000000000003)
(000d000000e30000000002c6,000d000002b00000000002c7)
(000d000003b600000000028e,000d0000037900000000028f)
(000d000003ce0000000002ae,000d0000037c0000000002af)
(000d0000032a0000000000a6,000d000003bc0000000000a9)
(000d0000032a0000000000a6,000d000003bc0000000000a9)
(000d0000032a0000000000a6,000d000003bc0000000000a9)
(000d000042c000000000016d,000d0000416c00000000016f)
(000d000042c000000000016d,000d0000416c00000000016f)
(000d0000035f00000000008c,000d0000033100000000008e)
(000d000043f1000000000103,000d000043c2000000000101)
(000d000043f1000000000103,000d000043c2000000000101)
(000d000043ed000000000169,000d0000425300000000017b)
(000d000002de0000000002ee,000d000003920000000002f1)
(000d000043ee00000000007f,000d0000416b000000000069)
(000d000043ee00000000007f,000d0000416b000000000069)
(000d0000037c0000000002af,000d000003ce0000000002ae)
(000d0000034900000000026e,000d000001ef000000000269)
(000d000002a200000000035b,000d000003f8000000000359)
(000d000002a200000000035b,000d000003f8000000000359)
(000d000040de00000000000d,000d000040c5000000000009)
(000d0000423700000000016e,000d0000423700000000016e)
(000d00004165000000000053,000d000043c9000000000071)
(000d000040de00000000000d,000d000040c5000000000009)
(000d000042e5000000000206,000d000043900000000001e6)
(000d000042e5000000000206,000d000043900000000001e6)
(000d000042e5000000000206,000d000043900000000001e6)
(000d000003bb00000000027a,000d00000289000000000279)
(000d000042e5000000000206,000d000043900000000001e6)
(000d000042e5000000000206,000d000043900000000001e6)
(000d000042e5000000000206,000d000043900000000001e6)
(000d000042fa000000000003,000d0000435a000000000002)
(000d0000026e0000000002c8,000d000000e30000000002c6)
(000d0000039e000000000206,000d000001600000000001e6)
(000d000042aa00000000016b,000d000043ed000000000169)
(000d000003ce0000000002ae,000d000003b10000000002b4)
(000d000043900000000001e6,000d000042e5000000000206)
(000d000043900000000001e6,000d000042e5000000000206)
(000d000000e30000000002c6,000d0000026f0000000002c9)
(000d000043900000000001e6,000d000042e5000000000206)
(000d000043900000000001e6,000d000042e5000000000206)
(000d000003ce0000000002ae,000d000003230000000002b2)
(000d000042aa00000000016b,000d000043ed000000000169)
(000d000043900000000001e6,000d000042e5000000000206)
(000d000043900000000001e6,000d000042e5000000000206)
(000d000042170000000001aa,000d000043150000000001ab)
(000d000043c2000000000101,000d000043f1000000000103)
(000d000043c9000000000071,000d00004165000000000053)
(000d000002a6000000000004,000d00000390000000000006)
(000d000003920000000002f1,000d000002de0000000002ee)
(000d000003bc0000000000a9,000d0000032a0000000000a6)
(000d0000033100000000008e,000d0000035f00000000008c)
(000d000003bc0000000000a9,000d0000032a0000000000a6)
(000d000043c2000000000101,000d000043f1000000000103)
(000d000000e30000000002c6,000d0000026e0000000002c8)
(000d000043150000000001ab,000d000042170000000001aa)
(000d00000289000000000279,000d000003bb00000000027a)
(000d000003bc0000000000a9,000d0000032a0000000000a6)
(000d000042170000000001aa,000d000042f60000000001ac)
(000d000003b10000000002b4,000d000003ce0000000002ae)
(000d0000425300000000017b,000d000043ed000000000169)
(000d0000037900000000028f,000d000003b600000000028e)
(000d00004354000000000117,000d00004370000000000115)
(000d00004354000000000117,000d00004370000000000115)
(000d0000026f0000000002c9,000d000000e30000000002c6)
     */
}
