package org.example.edge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.io.Serializable;

import static org.example.util.HelperFunction.*;


public class RelationReader implements Serializable {

    transient ExecutionEnvironment env;
    TemporalEdgeFactory factory;

    public RelationReader(ExecutionEnvironment env) {
        this.env = env;
        this.factory = new TemporalEdgeFactory();
    }



    public DataSet<TemporalEdge> readingTransfer(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple8<String, String, String, String, String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple8<String, String, String, String, String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple8<String, String, String, String, String, String, String, String> edgeData = joinedTuple.f0;

                    GradoopId sourceId = joinedTuple.f1;
                    GradoopId targetId = targetIds.f1;

                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("Amount", edgeData.f2);
                    edgeProps.set("CreateTime", edgeData.f3);
                    edgeProps.set("OrderNum", edgeData.f4);
                    edgeProps.set("Comment", edgeData.f5);
                    edgeProps.set("PayType", edgeData.f6);
                    edgeProps.set("GoodsType", edgeData.f7);

                    TemporalEdge edge = factory.createEdge("Transfer", targetId, sourceId, edgeProps);
                    edge.setValidFrom(convertTimeToUnix(edgeData.f3));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    public DataSet<TemporalEdge> readingInvest(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;
                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("Ratio", edgeData.f2);
                    edgeProps.set("CreateTime", edgeData.f3);

                    TemporalEdge edge = factory.createEdge("Invest", fromId, toId, edgeProps);
                    edge.setValidFrom(convertTimeToUnix(edgeData.f3));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    public DataSet<TemporalEdge> readingOwn(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple3<String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple3<String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple3<String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;

                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("CreateTime", edgeData.f2);

                    TemporalEdge edge = factory.createEdge("Own", fromId, toId, edgeProps);
                    edge.setValidFrom(convertTimeToUnix(edgeData.f2));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    public DataSet<TemporalEdge> readingDeposit(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices){
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;
                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("Amount", edgeData.f2);
                    edgeProps.set("CreateTime", edgeData.f3);

                    TemporalEdge edge = factory.createEdge("Deposit", fromId, toId, edgeProps);

                    edge.setValidFrom(convertTimeToUnix(edgeData.f3));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    public DataSet<TemporalEdge> readingRepay(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;
                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("Amount", edgeData.f2);
                    edgeProps.set("CreateTime", edgeData.f3);

                    TemporalEdge edge = factory.createEdge("Repay", fromId, toId, edgeProps);

                    edge.setValidFrom(convertTimeToUnix(edgeData.f3));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }


    public DataSet<TemporalEdge> readingGuarantee(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;

                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("CreateTime", edgeData.f2);
                    edgeProps.set("Relation", edgeData.f3);

                    TemporalEdge edge = factory.createEdge("Guarantee", fromId, toId, edgeProps);

                    edge.setValidFrom(convertTimeToUnix(edgeData.f2));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    public DataSet<TemporalEdge> readingSignIn(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices){

        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;
                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("CreateTime", edgeData.f2);
                    edgeProps.set("Location", edgeData.f3);

                    TemporalEdge edge = factory.createEdge("SignIn", fromId, toId, edgeProps);
                    edge.setValidFrom(convertTimeToUnix(edgeData.f2));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }

    public DataSet<TemporalEdge> readingWithdraw(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {

                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;

                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;

                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("Amount", edgeData.f2);
                    edgeProps.set("CreateTime", edgeData.f3);

                    TemporalEdge edge = factory.createEdge("Withdraw", fromId, toId, edgeProps);
                    edge.setProperties(edgeProps);
                    edge.setValidFrom(convertTimeToUnix(edgeData.f3));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }


    public DataSet<TemporalEdge> readingApply(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {

        DataSet<Tuple4<String, String, String, String>> csvEdgeData = env
                .readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, GradoopId>> idPairs = generateIdPairs(sourceVertices, targetVertices);

        DataSet<TemporalEdge> edges = csvEdgeData
                .join(idPairs)
                .where(0)
                .equalTo(0)
                .with((entry, sourceIds) -> new Tuple2<>(entry, sourceIds.f1))
                .returns(new TypeHint<Tuple2<Tuple4<String, String, String, String>, GradoopId>>() {})
                .join(idPairs)
                .where("f0.f1")
                .equalTo(0)
                .with((joinedTuple, targetIds) -> {
                    Tuple4<String, String, String, String> edgeData = joinedTuple.f0;
                    GradoopId fromId = joinedTuple.f1;
                    GradoopId toId = targetIds.f1;
                    Properties edgeProps = Properties.create();

                    edgeProps.set("SourceID", edgeData.f0);
                    edgeProps.set("TargetID", edgeData.f1);
                    edgeProps.set("CreateTime", edgeData.f2);
                    edgeProps.set("Org", edgeData.f3);


                    TemporalEdge edge = factory.createEdge("Apply", fromId, toId, edgeProps);

                    edge.setValidFrom(convertTimeToUnix(edgeData.f2));
                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
        return edges;
    }
}



