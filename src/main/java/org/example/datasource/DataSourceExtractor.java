package org.example.datasource;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.example.edge.RelationReader;
import org.example.vertex.EntitiesReader;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

public class DataSourceExtractor {

    private static RelationReader edgeReader;
    private static EntitiesReader entitiesReader;
    public DataSourceExtractor(ExecutionEnvironment env){
        this.edgeReader = new RelationReader(env);
        this.entitiesReader = new EntitiesReader(env);
    }

    public static Tuple2<DataSet<TemporalEdge>, DataSet<TemporalVertex>> graphDataSourceExtractor (String sourceType, String targetType, String relationType){

        DataSet<TemporalVertex> sourceVertices = null;
        switch (sourceType) {
            case "Person":
                sourceVertices = entitiesReader.readingPerson();
                break;
            case "Loan":
                sourceVertices = entitiesReader.readingLoan();
                break;
            case "Company":
                sourceVertices = entitiesReader.readingCompany();
                break;
            case "Account":
                sourceVertices = entitiesReader.readingAccount();
                break;
            case "Medium":
                sourceVertices = entitiesReader.readingMedium();
                break;
        }

        DataSet<TemporalVertex> targetVertices = null;
        switch (targetType) {
            case "Person":
                targetVertices = entitiesReader.readingPerson();
                break;
            case "Loan":
                targetVertices = entitiesReader.readingLoan();
                break;
            case "Company":
                targetVertices = entitiesReader.readingCompany();
                break;
            case "Account":
                targetVertices = entitiesReader.readingAccount();
                break;
            case "Medium":
                targetVertices = entitiesReader.readingMedium();
                break;
        }

        DataSet<TemporalEdge> edgeDataSet = null;
        switch (relationType) {
            case "Transfer":
                edgeDataSet = edgeReader.readingTransfer(sourceType, targetType);
            case "Own":
                edgeDataSet = edgeReader.readingOwn(sourceType, targetType);
            case "SignIn":
                edgeDataSet = edgeReader.readingSignIn(sourceType, targetType);
            case "Guarantee":
                edgeDataSet = edgeReader.readingGuarantee(sourceType, targetType);
            case "Apply":
                edgeDataSet = edgeReader.readingApply(sourceType, targetType);
            case "Invest":
                edgeDataSet = edgeReader.readingInvest(sourceType, targetType);
            case "Deposit":
                edgeDataSet = edgeReader.readingDeposit(sourceType, targetType);
            case "Repay":
                edgeDataSet = edgeReader.readingRepay(sourceType, targetType);
            case "Withdraw":
                edgeDataSet = edgeReader.readingWithdraw(sourceType, targetType);
                break;
        }
        return extractDataSource(edgeDataSet, sourceVertices, targetVertices);
    }

    // vertices are allowed to have no edges
    // could use TemporalGraphDataSource
    // FinBenchDataSource


    public static Tuple2<DataSet<TemporalEdge>, DataSet<TemporalVertex>> extractDataSource(
            DataSet<TemporalEdge> edges, DataSet<TemporalVertex> sourceVertices, DataSet<TemporalVertex> targetVertices) {
        DataSet<TemporalVertex> relevantSourceVertices = edges.join(sourceVertices)
                .where(edge -> edge.getProperties().get("SourceID").toString())
                .equalTo(sourceVertex -> sourceVertex.getProperties().get("ID").toString())
                .with((edge, sourceVertex) -> sourceVertex);

        DataSet<TemporalVertex> relevantTargetVertices = edges.join(targetVertices)
                .where(edge -> edge.getProperties().get("TargetID").toString())
                .equalTo(targetVertex -> targetVertex.getProperties().get("ID").toString())
                .with((edge, targetVertex) -> targetVertex);

        DataSet<TemporalVertex> distinctVertices = relevantTargetVertices.union(relevantSourceVertices).distinct(new PropertyKeySelector());

        return new Tuple2<>(edges, distinctVertices);
    }
}