package db.mySQL.dao;

import db.mySQL.dto.MySQLData;
import org.apache.ibatis.session.SqlSession;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DAO_MySQL {
    public void putDBP(SqlSession session, ArrayList<MySQLData> db_p){
        int result_p = session.insert("insert_p", db_p);
    }
    public void putDBC(SqlSession session, ArrayList<MySQLData> db_c){
        int result_c = session.insert("insert_c", db_c);
    }
    public JSONArray getDBP(SqlSession session){
        List<Map<String, Object>> answer = session.selectList("getParent");
        JSONArray jar = new JSONArray(answer);
        return jar;
    }
    public JSONArray getDBC(SqlSession session){
        List<Map<String, Object>> answer = session.selectList("getChild");
        JSONArray jar = new JSONArray(answer);
        return jar;
    }

}
