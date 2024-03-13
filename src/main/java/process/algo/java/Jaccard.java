package process.algo.java;

import org.elasticsearch.client.Request;
import org.jcodings.util.Hash;

import java.util.*;

public class Jaccard {

    public double jaccardAlgo(String comment, String cp_sentence){
        HashMap<String, Integer> s1 = new HashMap<>();
        HashMap<String, Integer> s2 = new HashMap<>();
        double result = 0.0;

        try {
            StringTokenizer key_list = new StringTokenizer(comment, " ");

            while(key_list.hasMoreTokens()){
                String keyword = key_list.nextToken();
                if(!s1.containsValue(keyword)){
                    s1.put(keyword, 1);
                }
                else {
                    Integer keyInt = s1.get(keyword) + 1; // 키워드의 빈도수도 처리할까......
                    s1.put(keyword, keyInt);
                }
            }

            key_list = new StringTokenizer(cp_sentence, " ");

            while(key_list.hasMoreTokens()){
                String keyword = key_list.nextToken();
                if(!s2.containsValue(keyword)){
                    s2.put(keyword, 1);
                }
                else {
                    Integer keyInt = s2.get(keyword) + 1; // 키워드의 빈도수도 처리할까......
                    s2.put(keyword, keyInt);
                }
            }

            int numOfCommon = 0;
            Iterator<String> s1l = s1.keySet().iterator();
            while(s1l.hasNext()){
                String tempkey = s1l.next();
                if(s2.containsKey(tempkey)){
                    numOfCommon++;
                }
            }

            result = (double)numOfCommon/(double)(s1.keySet().size()+s2.keySet().size()-numOfCommon);

        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }
}
