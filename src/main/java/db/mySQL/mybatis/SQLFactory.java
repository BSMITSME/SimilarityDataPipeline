package db.mySQL.mybatis;

import db.mySQL.mybatis.config.DemonConstants;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

public class SQLFactory {
    private String resource = DemonConstants.MYBITS_DIR;
    public SqlSession getSqlSession() throws IOException {
        SqlSessionFactory sf = null;
        try(InputStream inputStream = Resources.getResourceAsStream(resource)){
            sf = new SqlSessionFactoryBuilder().build(inputStream);
        }
        return sf.openSession(true); // commit 이 되어야 db 에 데이터가 들어감
    }
}