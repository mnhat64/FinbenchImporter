package org.example.vertex;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

import java.util.HashMap;
import java.util.Map;

import static org.example.util.HelperFunction.convertTimeToUnix;

public class EntitiesMapping {


    public static class MediumMapFunction implements MapFunction<Tuple6<String, String, String, String, String, String>, TemporalVertex> {
        private final TemporalVertexFactory factory;

        public MediumMapFunction(TemporalVertexFactory factory) {

            this.factory = factory;
        }

        @Override
        public TemporalVertex map(Tuple6<String, String, String, String, String, String> record) throws Exception {
            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put("ID", record.f0);
            propertiesMap.put("MediumType", record.f1);
            propertiesMap.put("IsBlocked", record.f2);
            propertiesMap.put("CreateTime", record.f3);
            propertiesMap.put("LastLogin", record.f4);
            propertiesMap.put("RiskLevel", record.f5);

            Long createTime = convertTimeToUnix(record.f3);

            GradoopId id = GradoopId.get();

            TemporalVertex medium =  factory.initVertex(id, "Medium", Properties.createFromMap(propertiesMap));
            medium.setValidFrom(createTime);
            return medium;
        }
    }



    public static class PersonMapFunction implements MapFunction<Tuple8<String, Long, String, String, String, String, String, String>, TemporalVertex> {
        private final TemporalVertexFactory factory;

        public PersonMapFunction(TemporalVertexFactory factory) {

            this.factory = factory;
        }

        @Override
        public TemporalVertex map(Tuple8<String, Long, String, String, String, String, String, String> record) throws Exception {
            Map<String, Object> propertiesMap = new HashMap<>();

            propertiesMap.put("ID", record.f0);
            propertiesMap.put("Create Time", record.f1);
            propertiesMap.put("Name", record.f2);
            propertiesMap.put("Is Blocked", record.f3);
            propertiesMap.put("Gender", record.f4);
            propertiesMap.put("Birthday", record.f5);
            propertiesMap.put("Country", record.f6);
            propertiesMap.put("City", record.f7);


            GradoopId id = GradoopId.get();

            TemporalVertex person = factory.initVertex(id, "Person", Properties.createFromMap(propertiesMap));
            person.setValidFrom(record.f1);

            return person;
        }
    }


}
