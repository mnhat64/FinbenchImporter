package org.example.edge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;

import java.io.Serializable;
import java.util.Objects;

import static org.example.util.FileDirectory.getRelationDirectory;
import static org.example.util.HelperFunction.convertTimeToUnix;
import static org.example.util.HelperFunction.createGradoopID;


public class RelationReader implements Serializable {

    transient ExecutionEnvironment env;
    TemporalEdgeFactory factory;

    public RelationReader(ExecutionEnvironment env){
        this.env = env;
        this.factory = new TemporalEdgeFactory();
    }

    public DataSet<TemporalEdge> readingApply(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Apply", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;
                    Long fromTime = convertTimeToUnix(record.f2);
                    String org = record.f3;

                    Properties edgeProperties = Properties.create();

                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Org", org);

                    TemporalEdge edge = factory.createEdge("Apply", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(fromTime);
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {
                }));
    }


    public DataSet<TemporalEdge> readingTransfer(String sourceType, String targetType){
        return env.readCsvFile(getRelationDirectory(sourceType, "Transfer", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();

                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Amount", record.f2);
                    edgeProperties.set("Create Time", record.f3);
                    edgeProperties.set("Order Num", record.f4);
                    edgeProperties.set("Comment", record.f5);
                    edgeProperties.set("Pay Type", record.f6);
                    edgeProperties.set("Goods Type", record.f7);

                    TemporalEdge edge = factory.createEdge("Transfer", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f3));
                    edge.setProperties(edgeProperties);
                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {
                }));
    }

    public DataSet<TemporalEdge> readingInvest(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Invest", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();

                    edgeProperties.set("InvestorID", sourceId);
                    edgeProperties.set("CompanyID", targetId);
                    edgeProperties.set("Ratio", record.f2);
                    edgeProperties.set("Create Time", record.f3);

                    TemporalEdge edge = factory.createEdge("Invest", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f3));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    public DataSet<TemporalEdge> readingOwn(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Own", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class)
                .map(record -> {
                    String targetId = record.f0;
                    String sourceId = record.f1;

                    Properties edgeProperties = Properties.create();

                    edgeProperties.set("TargetID", sourceId);
                    edgeProperties.set("SourceID", targetId);
                    edgeProperties.set("Create Time", record.f2);

                    TemporalEdge edge = factory.createEdge("Own", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f2));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    public DataSet<TemporalEdge> readingDeposit(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Deposit", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();
                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Amount", record.f2);
                    edgeProperties.set("Create Time", record.f3);

                    TemporalEdge edge = factory.createEdge("Deposit", createGradoopID(sourceType, targetId), createGradoopID(targetType, sourceId));
                    edge.setValidFrom(convertTimeToUnix(record.f3));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    public DataSet<TemporalEdge> readingRepay(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Repay", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();
                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Amount", record.f2);
                    edgeProperties.set("Create Time", record.f3);

                    TemporalEdge edge = factory.createEdge("Repay", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f3));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    public DataSet<TemporalEdge> readingGuarantee(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Guarantee", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();
                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Create Time", record.f2);
                    edgeProperties.set("Relation", record.f3);

                    TemporalEdge edge = factory.createEdge("Guarantee", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f2));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    public DataSet<TemporalEdge> readingSignIn(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "SignIn", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();
                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Create Time", record.f2);
                    edgeProperties.set("Location", record.f3);

                    TemporalEdge edge = factory.createEdge("Sign In", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f2));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    public DataSet<TemporalEdge> readingWithdraw(String sourceType, String targetType) {
        return env.readCsvFile(getRelationDirectory(sourceType, "Withdraw", targetType))
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map(record -> {
                    String sourceId = record.f0;
                    String targetId = record.f1;

                    Properties edgeProperties = Properties.create();
                    edgeProperties.set("SourceID", sourceId);
                    edgeProperties.set("TargetID", targetId);
                    edgeProperties.set("Amount", record.f2);
                    edgeProperties.set("Create Time", record.f3);

                    TemporalEdge edge = factory.createEdge("Withdraw", createGradoopID(sourceType, sourceId), createGradoopID(targetType, targetId));
                    edge.setValidFrom(convertTimeToUnix(record.f3));
                    edge.setProperties(edgeProperties);

                    return edge;
                }).returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }



    public DataSet<TemporalEdge> readCSV(String filePath) {
        return env.readTextFile(filePath)
                .map(new MapFunction<String, Tuple2<String[], String[]>>() {
                    boolean isFirstLine = true;
                    String[] columnNames;

                    @Override
                    public Tuple2<String[], String[]> map(String line) {
                        if (isFirstLine) {
                            columnNames = line.split("\\|");
                            isFirstLine = false;
                            return null;
                        } else {
                            String[] values = line.split("\\|", -1);
                            return new Tuple2<>(columnNames, values);
                        }
                    }
                })
                .filter(Objects::nonNull)
                .map(new MapFunction<Tuple2<String[], String[]>, TemporalEdge>() {
                    @Override
                    public TemporalEdge map(Tuple2<String[], String[]> value) {
                        Properties edgeProperties = Properties.create();
                        for (int i = 0; i < value.f0.length; i++) {
                            if (i < value.f1.length) {
                                edgeProperties.set(value.f0[i], value.f1[i]);
                            }
                        }
                        String sourceId = edgeProperties.get("personId").toString();
                        String targetId = edgeProperties.get("loanId").toString();
                        TemporalEdge edge = factory.createEdge(createGradoopID("Source", sourceId), createGradoopID("Target", targetId));
                        edge.setProperties(edgeProperties);
                        return edge;
                    }
                })
                .filter(Objects::nonNull);
    }










/*
    public DataSet<TemporalEdge> readingPersonApplyLoan(String filePath, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class)
                .map((MapFunction<Tuple4<String, String, String, String>, TemporalEdge>) record -> {

                    String sourceID = record.f0;
                    String targetID = record.f1;
                    String createTime = record.f2;

                    Map<String, Object> propertiesMap = new HashMap<>();
                    propertiesMap.put("SourceID", record.f0);
                    propertiesMap.put("TargetID", record.f1);
                    propertiesMap.put("Create Time", record.f2);
                    propertiesMap.put("Org", record.f3);

                    TemporalEdge edge = factory.createEdge("Loan", findVertexWithProperty(sourceVertices, "ID", sourceID),findVertexWithProperty(targetVertices, "ID", targetID),Properties.createFromMap(propertiesMap));

                    edge.setValidFrom(convertToUnixTimestamp(createTime));

                    return edge;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalEdge>() {}));
    }

    private TemporalVertex findVertexWithProperty(DataSet<TemporalVertex> vertices, String propertyName, String propertyValue) throws Exception {
        DataSet<TemporalVertex> filteredVertices = vertices.filter(new FilterFunction<TemporalVertex>() {
            public boolean filter(TemporalVertex vertex) throws Exception {
                return propertyValue.equals(vertex.getProperties().get(propertyName).toString());
            }
        });

        List<TemporalVertex> vertexList = filteredVertices.collect();
        return vertexList.get(0);
    }
    public static long convertToUnixTimestamp(String timestamp) throws ParseException {
        int millisIndex = timestamp.lastIndexOf('.');
        int missingZeros = 3 - (timestamp.length() - millisIndex - 1);

        if (millisIndex == -1) {
            timestamp = timestamp + ".000";
        } else if (missingZeros > 0) {
            // If milliseconds have fewer than 3 digits, add the missing zeros
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

    private TemporalVertex findVertexWithProperty2(DataSet<TemporalVertex> vertices, String propertyName, String propertyValue) throws Exception {
        DataSet<TemporalVertex> filteredVertices = vertices.filter(new VertexMapper(propertyName, propertyValue));
        List<TemporalVertex> vertexList = filteredVertices.collect();
        return vertexList.isEmpty() ? null : vertexList.get(0);
    }

 */
}
