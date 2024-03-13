package process;

import db.mySQL.dao.DAO_MySQL;
import db.mySQL.dto.MySQLData;
import db.mySQL.dto.ComparingData;
import db.mySQL.dto.CompareResult;
import db.mySQL.mybatis.SQLFactory;
import elasticsearch.ElasticsearchData;
import org.apache.ibatis.session.SqlSession;
import org.elasticsearch.client.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import process.algo.java.Jaccard;

import javax.swing.plaf.synth.SynthOptionPaneUI;
import java.io.IOException;
import java.util.*;

public class SimilarityEngine {
    ElasticsearchData es_data = null;
    JSONArray response = null;
    Jaccard jaccard = null;
    SQLFactory sqlFactory = null;
    StringBuffer js = null;
    // tip : static 하고 안하고 차이?
    // -> 둘다 전역변수 선언이지만 static을 선언하면 다른 클래스에서도 접근 가능하다. (안붙일 시 같은 클래스에서만 가능)
    static String search_after_value = "";
    ArrayList<MySQLData> db_P = null;
    ArrayList<MySQLData> db_C = null;
    DAO_MySQL dao = null;
    StringBuffer sb_sim = null;
    StringBuffer sb_sim_count = null;
    public void similarityProcess(String index, int size) {
        // 모 데이터 DB array
        db_P = new ArrayList<>();
        // 자 데이터 DB array
        db_C = new ArrayList<>();

        es_data = new ElasticsearchData();
        jaccard = new Jaccard();
        sqlFactory = new SQLFactory();
        dao = new DAO_MySQL();
        double result = 0.0;
        es_data.getClient();
        boolean routine = false;
        ArrayList<String> temp_db;
        ArrayList<String> temp_db_c;

        MySQLData data = null;

        while(true){
            try {
                response = es_data.getData(index, size, search_after_value);
                SqlSession session = sqlFactory.getSqlSession();
                int rs_len = response.length();
                if (rs_len == 0){
                    es_data.closeConnection();
                    break;
                }else {
                    search_after_value = response.getJSONObject(rs_len - 1).getJSONArray("sort").getString(0);
                }
                // similarity 인덱스에 bulk api로 보낼 Stringbuffer
                sb_sim = new StringBuffer();

                // sim_count 인덱스에 bulk api로 보낼 Stringbuffer
                sb_sim_count = new StringBuffer();

                // 첫 bulk로 db 만들 시 중복 방지
                temp_db = new ArrayList<>();
                temp_db_c = new ArrayList<>();

                // 첫 bulk -> DB가 만들어지지 않아 자기 자신을 보며 비교
                if(routine == false) {
                    for (int i = 0; i < rs_len; i++) {
                        try {
                            if (!response.getJSONObject(i).getJSONObject("_source").has("pid")) {
                                // elasticsearch에 보낼 데이터 모음
                                // doc의 원본 : response.getJSONObject(i)
                                JSONObject shortcut = response.getJSONObject(i).getJSONObject("_source");

                                String an_title = shortcut.getString("an_title");// 제목
                                String an_content = shortcut.getString("an_content");// 내용
                                String kw_docid = shortcut.getString("kw_docid"); // 도큐먼트 아이디

                                // 유사도 계산할 대상 String으로 추출
                                String sentence = makeSubject(i, response);

                                // i 부터 확인 -> pid 식별할 필요 없어짐 (하지만 보류)
                                for (int a = i; a < rs_len; a++) {
                                    // mysql에 넣을 데이터 타입 (dto)
                                    data = new MySQLData();

                                    // 현재 보고 있는(대조 되고 있는) 문서
                                    JSONObject current = response.getJSONObject(a).getJSONObject("_source");
                                    String compare_title = current.getString("an_title");
                                    String compare_content = current.getString("an_content");
                                    String compare_docid = current.getString("kw_docid");

                                    // 현재 보고있는 문서가 식별자가 부여되었는지 확인
                                    if (!current.has("pid")) {
                                        // makeCD => 비교할 키워드를 String으로 반환
                                        String sentense_comp = makeSubject(a, response);
                                        // 비교
                                        result = jaccard.jaccardAlgo(sentence, sentense_comp);

                                        if (result >= 0.15) {
                                            // 유사하다면 pid 넣기
                                            current.put("pid", kw_docid);

                                            // 내용이 같다면 -> 중복 or 자기 자신 -> 비교대상 id를 기입
                                            if (result == 1.0) {
                                                // 자기 자신인지 확인 -> kw_docid만 확인한다면
                                                if(kw_docid.equals(compare_docid) && !(temp_db_c.contains(compare_title))){
                                                    temp_db_c.add(compare_title);
                                                    sb_sim.append(es_data.makeBulk(UUID.randomUUID().toString(), compare_title, compare_content, kw_docid, compare_docid, "p"));
                                                }else {
                                                    // **** 내용이 완전히 같으나 doc_id가 다른 중복 기사 ****
                                                    // Elasticsearch의 count 인덱스로
                                                    sb_sim_count.append(es_data.makeBulk(UUID.randomUUID().toString(), compare_title, compare_content, kw_docid, compare_docid, "c"));
                                                }

                                                // 완전 같다면 parent db, similarity es 전송
                                                //DB -> IGNORE로 중복제거 하여 중복 상관 안한다.-> kw_docid 유니크
                                                data = articleData(sentence, kw_docid);
                                                db_P.add(data);
                                            } else {

                                                // child db 전송
                                                data = articleData(sentense_comp, kw_docid);

//                                            if(kw_docid.equals(compare_docid)){
                                                if(!temp_db_c.contains(compare_title)){
                                                    temp_db_c.add(compare_title);
                                                    sb_sim.append(es_data.makeBulk(UUID.randomUUID().toString(), compare_title, compare_content, kw_docid, compare_docid, "p_D"));
                                                }else {
                                                    // Elasticsearch의 count 인덱스로
                                                    sb_sim_count.append(es_data.makeBulk(UUID.randomUUID().toString(), compare_title, compare_content, kw_docid, compare_docid, "c_D"));
                                                }
                                                // 유사하면 자 데이터 DB로
                                                db_C.add(data);
                                            }
                                        }
                                    }
                                }
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                        }

                    }
                }

                // 2nd ~ -> DB와 비교
                else {
                    for (int i = 0; i < rs_len; i++) {
                        try {
                            data = new MySQLData();
                            // elasticsearch에 보낼 데이터 모음
                            // doc의 원본 : response.getJSONObject(i)
                            JSONObject subject = response.getJSONObject(i).getJSONObject("_source");

                            String an_title = subject.getString("an_title");
                            String kw_docid = subject.getString("kw_docid");
                            String an_content = subject.getString("an_content");

                            // 유사도 계산할 대상 String으로 추출
                            String sentence = makeSubject(i,response);



                            // 부모 DB에서 가져오기 -> paging 처리하여 데이터 가져와야 함(개선사항)
                            JSONArray jar_p = dao.getDBP(session);
                            // 자식 DB에서 가져오기 -> paging 처리하여 데이터 가져와야 함(개선사항)
                            JSONArray jar_c = dao.getDBC(session);

                            // parent db와 비교
                            CompareResult thread_p = mySQLLoop(jar_p, sentence);
                            int result_p = thread_p.getResult();
                            String pid = thread_p.getData().getKw_docid();

                            // 모데이터 DB 중복 -> bulk로 보내기 때문에 아직 들어가있지 않은 모데이터는 파악 못함 -> 중복 발생
                            if(result_p == 2){
                                subject.put("pid", pid);
                                sb_sim_count.append(es_data.makeBulk(UUID.randomUUID().toString(), an_title, an_content, pid, kw_docid, "c"));
                            }else if(result_p == 3){
                                if(!temp_db.contains(an_title)){
                                    subject.put("pid", kw_docid);
                                    data = articleData(sentence, kw_docid);
                                    temp_db.add(an_title);
                                    db_P.add(data);
                                    sb_sim.append(es_data.makeBulk(UUID.randomUUID().toString(), an_title, an_content, kw_docid, kw_docid,"p"));
                                }else { // 있다면
                                    subject.put("pid", pid);
                                    sb_sim_count.append(es_data.makeBulk(UUID.randomUUID().toString(), an_title, an_content, pid, kw_docid, "c"));
                                }
                            }
                            else{
                                // children db와 비교
                                CompareResult thread_c = mySQLLoop(jar_c, sentence);
                                int result_c = thread_c.getResult();

                                // 모 데이터의 자 데이터
                                if (result_c == 1 || result_c == 3){
                                    if(!(temp_db_c.contains(an_title)) && !(temp_db.contains(an_title))) {
                                        subject.put("pid", pid);
                                        data = articleData(sentence, pid);
                                        db_C.add(data);
                                        temp_db_c.add(an_title);
                                        sb_sim.append(es_data.makeBulk(UUID.randomUUID().toString(), an_title, an_content, pid, kw_docid, "c"));
                                    }
                                    else {
                                        subject.put("pid", pid);
                                        sb_sim_count.append(es_data.makeBulk(UUID.randomUUID().toString(), an_title, an_content, pid, kw_docid, "c"));
                                    }
                                }
                                else {
                                    // 자식 데이터의 중복 데이터 (1,2)
                                    subject.put("pid", pid);
                                    sb_sim_count.append(es_data.makeBulk(UUID.randomUUID().toString(), an_title, an_content, pid, kw_docid, "c"));
                                }
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                }

                // Elasticsearch 및 MySQL로 전송
                if(sb_sim.length() != 0){
                    Response resp = es_data.postDataSimilarity(sb_sim);
                }
                if (sb_sim_count.length() != 0){
                    Response respc = es_data.postDataSimCount(sb_sim_count);
                }
                if (!db_P.isEmpty()) {
                    dao.putDBP(session, db_P);
                }
                if (!db_C.isEmpty()) {
                    dao.putDBC(session, db_C);
                }
                routine = true;
                // clear vs new? -> 데이터가 많아지면 new가 유리
                db_C.clear();
                db_P.clear();
            } catch (IOException e){
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                es_data.closeConnection();
            }
        }
    }

    // limit으로 paging처리 하여 데이터 받아와야 함 -> 개선사항
    public CompareResult mySQLLoop(JSONArray jar, String sentence){
        CompareResult rt = new CompareResult();
        int answer =0;
        int jar_len = jar.length();

        // 유사도 최대값인 데이터 찾기
        double max = 0;
        String dbpid ="";
        ComparingData data_cp = new ComparingData();
        for (int a = 0; a< jar_len; a++){

            // pid 명칭 맞는지 DB 확인
            String pid = jar.getJSONObject(a).getString("pid");
            String keyword = jar.getJSONObject(a).getString("keyword");

            //유사도 가장 큰 것 확인
            double result = jaccard.jaccardAlgo(sentence, keyword);

            // result -> 1.0일때는 break
            if(result == 1){
                break;
            }else{
                if(result > max){
                    max = result;
                    dbpid = pid;
                }
            }
        }
        if(max >= 0.15){
            if(max == 1.0){
                answer = 2;
            }else {
                answer = 1;
            }
        }else {
            answer = 3;
        }
        data_cp.setResult(max);
        data_cp.setKw_docid(dbpid);

        rt.setResult(answer);
        rt.setData(data_cp);
        return rt;
    }

    // MySQL에 넣을 데이터 만들기
    public MySQLData articleData(String keyword, String pid) {
        MySQLData articleData = new MySQLData();
        articleData.setPid(pid);
        articleData.setKeyword(keyword);
        return articleData;
    }

    //Keyword 붙이기
    public String makeSubject(int i, JSONArray response) {
        StringBuffer sentence_kwl = new StringBuffer();
        JSONObject shortcut = response.getJSONObject(i).getJSONObject("_source");

        // "ㅋㅋㅋ"나 "루삥봉ㅇㅇㅇ" 이러한 제목일 시 키워드 추출이 안될 수 있음 -> for문 단에서 try_catch로 에러 처리하여 버리기
        JSONArray kwl_attirbute = shortcut.getJSONArray("kwl_attribute_total_word");
        JSONArray kwl_tpop = shortcut.getJSONArray("kwl_tpop_total");

        //JSONArray를 String(공백으로 구분)으로 바꾸기 (자카드 알고리즘)
        for (Object o : kwl_tpop) {
            sentence_kwl.append(" " + o);
        }
        for (Object o : kwl_attirbute) {
            sentence_kwl.append(" " + o);
        }

        String sentense = sentence_kwl.toString().trim();
        return sentense;
    }
}