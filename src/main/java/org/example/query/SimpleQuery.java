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
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.tpgm.EdgeToTemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;


public class SimpleQuery {
    EntitiesReader entitiesReader;
    RelationReader relationReader;

    public SimpleQuery(ExecutionEnvironment env){
        this.entitiesReader = new EntitiesReader(env);
        this.relationReader = new RelationReader(env);
    }

    public void extractAccountQuery(String id) throws Exception {
        DataSet<TemporalVertex> accounts =  entitiesReader.readingAccount();

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

    public Tuple2<TemporalGraph, Tuple2<Double, Double>> transferInOut (TemporalGraph source, String accountId, LocalDateTime startTime, LocalDateTime endTime) throws Exception {
        long startTimestamp = startTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTimestamp = endTime.toInstant(ZoneOffset.UTC).toEpochMilli();

        TemporalGraph snapshot = source.fromTo(startTimestamp, endTimestamp);

        DataSet<TemporalEdge> filteredEdges = snapshot.getEdges()
                .filter(new FilterFunction<TemporalEdge>() {
                    @Override
                    public boolean filter(TemporalEdge edge) {
                        GradoopId sourceId = edge.getSourceId();
                        GradoopId targetId = edge.getTargetId();
                        return sourceId.toString().equals(accountId) || targetId.toString().equals(accountId);
                    }
                });

        Tuple2<Double, Double> summary = filteredEdges
                .map(edge -> {
                    double amount = Double.parseDouble(edge.getPropertyValue("Amount").toString());
                    boolean isTransferOut = edge.getSourceId().toString().equals(accountId);
                    return new Tuple3<>(isTransferOut ? amount : 0.0, isTransferOut ? 0.0 : amount, amount);
                })
                .reduce((t1, t2) -> new Tuple3<>(
                        t1.f0 + t2.f0,
                        t1.f1 + t2.f1,
                        Math.max(t1.f2, t2.f2)
                ))
                .map(t -> new Tuple2<>(
                        t.f0 > 0 ? t.f2 : -1,
                        t.f1 > 0 ? t.f2 : -1
                ))
                .collect()
                .get(0);


        TemporalGraph filteredGraph = snapshot.getFactory()
                .fromDataSets(snapshot.getVertices(), filteredEdges);

        return new Tuple2<>(filteredGraph, summary);
    }

    public Tuple2<TemporalGraph, Tuple4<Long, Long, Long, Long>> transferInOut(TemporalGraph graph, GradoopId accountId, Long startTime, Long endTime) throws Exception {

        TemporalGraph subgraph = graph.fromTo(startTime, endTime);

        DataSet<TemporalEdge> transferIns = subgraph.getEdges().filter(edge ->
                edge.getTargetId().equals(accountId)
        );
        DataSet<TemporalEdge> transferOuts = subgraph.getEdges().filter(edge ->
                edge.getSourceId().equals(accountId)
        );

        Long sumIn = calculateSum(transferIns);
        Long sumOut = calculateSum(transferOuts);
        Long maxIn = calculateMax(transferIns);
        Long maxOut = calculateMax(transferOuts);

        Tuple4<Long, Long, Long, Long> transferStats = new Tuple4<>(sumIn, maxIn, sumOut, maxOut);

        return new Tuple2<>(subgraph, transferStats);
    }

    private Long calculateSum(DataSet<TemporalEdge> transfers) throws Exception {
        Optional<Long> sum = transfers
                .map(edge -> Long.parseLong(edge.getPropertyValue("Amount").toString()))
                .reduce(Long::sum)
                .collect()
                .stream()
                .findFirst();

        return sum.orElse(0L);
    }

    private Long calculateMax(DataSet<TemporalEdge> transfers) throws Exception {
        Optional<Long> max = transfers
                .map(edge -> Long.parseLong(edge.getPropertyValue("Amount").toString()))
                .reduce(Math::max)
                .collect()
                .stream()
                .findFirst();

        return max.orElse(-1L);
    }
}
