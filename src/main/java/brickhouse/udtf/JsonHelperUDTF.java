package brickhouse.udtf;

import org.apache.commons.lang.StringUtils;
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

    public static void main(String[] args) throws JSONException {
//        File file = new File("D:\\IdeaProjects\\amberdata\\amberHiveUdf\\src\\main\\java\\com\\amber\\udtf\\schema2.json");
//        String str = txt2String(file);
//        String[] strArray3 = {str};
        String[] strArray3 = {null};
        String[] strArray4 = {" \tnull "};
        process2(strArray4);
    }

    public static void process2(Object[] args) {
        Object obj = args[0];
        System.out.println("参数：" + obj);
        // 1.入参为'null'字符串的保留
        // 2.入参为null的保留
        try {
            if (null == obj || StringUtils.lowerCase(obj.toString()).trim().equals("null")) {
                String key = null;
                String value = null;
                System.out.println("key:" + key + "  value:" + value);

            } else {
                String arg = args[0].toString();
                // 入参为{}的处理为 key=null value=null
                if (arg.length() == 2) {
                    String key = null;
                    String value = null;
                    System.out.println("key:" + key + "  value:" + value);

                } else {
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
                }
            }
        } catch (JSONException je) {
            je.printStackTrace();
        }

    }

    public static String txt2String(File file) {
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                result.append(System.lineSeparator() + s);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.toString();
    }

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

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
        // 1.入参为'null'字符串的保留
        // 2.入参为null的保留
        try {
            if (null == obj || StringUtils.lowerCase(obj.toString()).trim().equals("null")) {
                String key = null;
                String value = null;
                System.out.println("key:" + key + "  value:" + value);
                outList.clear();
                outList.add(key);
                outList.add(value);
                forward(outList);
            } else {
                String arg = args[0].toString();
                // 3.入参为{}的处理为 key=null value=null
                if (arg.length() == 2) {
                    String key = null;
                    String value = null;
                    System.out.println("key:" + key + "  value:" + value);
                    outList.clear();
                    outList.add(key);
                    outList.add(value);
                    forward(outList);
                } else {
                    // 4.正常情况 1key1value 或者 多key多value
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
                }
            }
        } catch (JSONException je) {
            je.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
