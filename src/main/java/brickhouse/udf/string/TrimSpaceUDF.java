package brickhouse.udf.string;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author: liusilin
 * @Date: 2022/3/5 15:03
 * @Description:
 */
public class TrimSpaceUDF extends UDF {
    public List<String> evaluate(List<String> arr) {
        if(arr.isEmpty()){
            return null;
        }
        for (int i = 0; i < arr.size(); i++) {
            if (StringUtils.isEmpty(arr.get(i))) {
                arr.remove(arr.get(i));
            }
        }
        return arr.size() > 0 ? arr : null;
    }

    public static void main(String[] args) {
        List<String> a = new LinkedList<>();
        TrimSpaceUDF trimSpace = new TrimSpaceUDF();
        List<String> b = trimSpace.evaluate(a);
        System.out.println(b);
    }
}
