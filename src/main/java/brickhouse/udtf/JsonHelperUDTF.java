package brickhouse.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: liusilin
 * @Date: 2022/3/5 15:08
 * @Description:
 */
public class JsonHelperUDTF extends GenericUDTF {

    private final ArrayList<String> outList = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        //1.定义输出数据的列名和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        //2.添加输出数据的列名
        fieldNames.add("key");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        Object obj = args[0];
        System.out.println("参数：" + obj);
        if (null != obj && !"NULL".equals(obj)) {
            String arg = args[0].toString();
            try {
                JSONObject jsonObject = new JSONObject(arg);
                //通过迭代器获取这段json当中所有的key值
                Iterator keys = jsonObject.keys();
                //然后通过一个循环取出所有的key值
                while (keys.hasNext()) {
                    String key = String.valueOf(keys.next());
                    //最后就可以通过刚刚得到的key值去解析后面的json了
                    String value = jsonObject.optString(key);
                    System.out.println("key:" + key + "  value:" + value);

                    outList.clear();
                    outList.add(key);
                    outList.add(value);
                    forward(outList);

                }
            } catch (JSONException je) {
                je.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws JSONException {
        File file = new File("D:\\IdeaProjects\\amberdata\\amberHiveUdf\\src\\main\\java\\com\\amber\\udtf\\schema2.json");
        String str = txt2String(file);
        JSONObject jsonObject = new JSONObject(str);
        JSONObject jsonObject2 = jsonObject.getJSONObject("spot");

        Iterator keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = String.valueOf(keys.next());
            String value = jsonObject.optString(key);
            System.out.println("key:" + key + "  value:" + value);
        }

        System.out.println("++++++++++++++++++++++++++++");
        String[] strArray={null};
        String[] strArray2={"NULL"};
        String[] strArray3={str};
        process2(strArray3);

    }

    public static void process2(Object[] args)  {
        Object obj = args[0];
        System.out.println("参数：" + obj);
        if(null != obj && !"NULL".equals(obj)){
            String arg = args[0].toString();
            try {
                JSONObject jsonObject = new JSONObject(arg);
                //通过迭代器获取这段json当中所有的key值
                Iterator keys = jsonObject.keys();
                //然后通过一个循环取出所有的key值
                while (keys.hasNext()) {
                    String key = String.valueOf(keys.next());
                    //最后就可以通过刚刚得到的key值去解析后面的json了
                    String value = jsonObject.optString(key);
                    System.out.println("key:" + key + "  value:" + value);

                }
            }catch (JSONException je) {
                je.printStackTrace();
            }
        }

    }

    public static String txt2String(File file){
        StringBuilder result = new StringBuilder();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                result.append(System.lineSeparator()+s);
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return result.toString();
    }
}
