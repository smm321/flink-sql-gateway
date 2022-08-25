package com.ververica.flink.table.gateway.context;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.util.ArrayList;
import java.util.List;

/**
 * CustomSqlParser to parse Sql list.
 */
public class CustomSqlParser {

    private static final SqlParser.Config config;
    static {
        config = SqlParser.configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setConformance(FlinkSqlConformance.DEFAULT)
                .setLex(Lex.JAVA)
                .setIdentifierMaxLength(256)
                .build();
    }

    public SqlParser getParser(String sql){
        return SqlParser.create(sql, config);
    }

    public List<String> formatSql(String sql) throws Exception{
        List<String> sqlList = new ArrayList<>();
        SqlParser.create(sql, config).parseStmtList().iterator().forEachRemaining(s -> sqlList.add(s.toString()));
        return sqlList;
    }
}
