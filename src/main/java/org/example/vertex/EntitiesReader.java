package org.example.vertex;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;

import java.io.Serializable;

import java.util.HashMap;

import java.util.Map;

import static org.example.util.HelperFunction.*;

public class EntitiesReader implements Serializable {

    private transient ExecutionEnvironment env;
    private TemporalVertexFactory factory;

    public EntitiesReader(ExecutionEnvironment env) {
        this.env = env;
        this.factory = new TemporalVertexFactory();
    }


    public DataSet<TemporalVertex> readingPerson(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, Long.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map((MapFunction<Tuple8<String, Long, String, String, String, String, String, String>, TemporalVertex>) record -> {

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
                })
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    public DataSet<TemporalVertex> readingLoan(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, Double.class, Double.class, String.class, String.class, Double.class)
                .map(record -> {

                    Map<String, Object> propertiesMap = new HashMap<>();
                    propertiesMap.put("ID", record.f0);
                    propertiesMap.put("Loan amount", record.f1);
                    propertiesMap.put("Balance", record.f2);
                    propertiesMap.put("Create Time", record.f3);
                    propertiesMap.put("Loan usage", record.f4);
                    propertiesMap.put("Interest Rate", record.f5);
                    Long createTime = convertTimeToUnix(record.f3);

                    GradoopId id = GradoopId.get();

                    TemporalVertex loan = factory.initVertex(id, "Loan", Properties.createFromMap(propertiesMap));
                    loan.setValidFrom(createTime);

                    return loan;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    public DataSet<TemporalVertex> readingCompany(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(record -> {

                    Map<String, Object> propertiesMap = new HashMap<>();
                    propertiesMap.put("ID", record.f0);
                    propertiesMap.put("Name", record.f1);
                    propertiesMap.put("Create Time", record.f3);
                    propertiesMap.put("Is Blocked", record.f2);
                    propertiesMap.put("Country", record.f4);
                    propertiesMap.put("City", record.f5);
                    propertiesMap.put("Business", record.f6);
                    propertiesMap.put("Description", record.f7);
                    propertiesMap.put("URL", record.f8);
                    Long createTime = convertTimeToUnix(record.f3);

                    GradoopId id = GradoopId.get();

                    TemporalVertex company = factory.initVertex(id, "Company", Properties.createFromMap(propertiesMap));
                    company.setValidFrom(createTime);

                    return company;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    public DataSet<TemporalVertex> readingAccount(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class)
                .map(record -> {

                    Map<String, Object> propertiesMap = new HashMap<>();
                    propertiesMap.put("ID", record.f0);
                    propertiesMap.put("CreateTime", record.f1);
                    propertiesMap.put("IsBlocked", record.f2);
                    propertiesMap.put("AccountType", record.f3);
                    propertiesMap.put("Nickname", record.f4);
                    propertiesMap.put("PhoneNumber", record.f5);
                    propertiesMap.put("Email", record.f6);
                    propertiesMap.put("FreqLoginType", record.f7);
                    propertiesMap.put("LastLoginTime", record.f8);
                    propertiesMap.put("AccountLevel", record.f9);

                    Long createTime = convertTimeToUnix(record.f1);

                    GradoopId id = GradoopId.get();

                    TemporalVertex account = factory.initVertex(id, "Account", Properties.createFromMap(propertiesMap));
                    account.setValidFrom(createTime);

                    return account;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }

    public DataSet<TemporalVertex> readingMedium(String filePath) {
        return env.readCsvFile(filePath)
                .ignoreFirstLine()
                .fieldDelimiter("|")
                .types(String.class, String.class, String.class, String.class, String.class, String.class)
                .map(record -> {

                    Map<String, Object> propertiesMap = new HashMap<>();
                    propertiesMap.put("ID", record.f0);
                    propertiesMap.put("MediumType", record.f1);
                    propertiesMap.put("IsBlocked", record.f2);
                    propertiesMap.put("CreateTime", record.f3);
                    propertiesMap.put("LastLogin", record.f4);
                    propertiesMap.put("RiskLevel", record.f5);

                    Long createTime = convertTimeToUnix(record.f3);

                    GradoopId id = GradoopId.get();
                    TemporalVertex medium = factory.initVertex(id, "Medium", Properties.createFromMap(propertiesMap));
                    medium.setValidFrom(createTime);

                    return medium;
                })
                .returns(TypeInformation.of(new TypeHint<TemporalVertex>() {}));
    }
}
