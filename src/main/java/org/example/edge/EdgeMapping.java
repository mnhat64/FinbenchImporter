package org.example.edge;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;

import static org.example.util.HelperFunction.convertTimeToUnix;

public class EdgeMapping {

    private TemporalEdgeFactory factory;

    public EdgeMapping (TemporalEdgeFactory factory){
        this.factory = factory;
    }

    public TemporalEdge mapTransfer(Tuple2<Tuple8<String, String, String, String, String, String, String, String>, GradoopId> values, Tuple2<String, GradoopId> targetIds) throws Exception {
        Tuple8<String, String, String, String, String, String, String, String> edgeData = values.f0;

        GradoopId sourceId = values.f1;
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
    }


}
