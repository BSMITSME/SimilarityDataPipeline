import db.mySQL.dao.DAO_MySQL;
import db.mySQL.mybatis.SQLFactory;
import org.apache.ibatis.session.SqlSession;
import org.json.JSONArray;
import process.SimilarityEngine;
import process.algo.java.Jaccard;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        long beforeTime = System.currentTimeMillis();
        try {

            SimilarityEngine engine = new SimilarityEngine();

//
            engine.similarityProcess("sample_v4_lucy3_main_20240221", 500);

        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            long afterTime = System.currentTimeMillis(); // 코드 실행 후에 시간 받아오기
            long secDiffTime = (afterTime - beforeTime)/1000; //두 시간의 차 계산
            System.out.println("시간차이(m) : "+secDiffTime);
        }
    }
}
