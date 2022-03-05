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
 * @Date: 2022/3/5 15:12
 * @Description:
 */
public class JsonStrongerHelperUDTF extends GenericUDTF {
    private final ArrayList<String> outList = new ArrayList<>();

    public static void main(String[] args) {
        File file = new File("D:\\IdeaProjects\\amberdata\\amberHiveUdf\\src\\main\\java\\com\\amber\\udtf\\schema.json");
        String str = txt2String(file);
        String[] strArray = {str};

        process2(strArray);
    }

    public static void process2(Object[] args) {
        Object obj = args[0];
        System.out.println("参数：" + obj);
        if (null != obj && !"NULL".equals(obj)) {
            String arg = args[0].toString();
            try {
                JSONObject jsonObject = new JSONObject(arg);
                //通过迭代器获取这段json当中所有的key值
                Iterator keys = jsonObject.keys();

                String[][] list = new String[6][60];
                int j = 0;
                while (keys.hasNext()) {
                    String key = String.valueOf(keys.next());
                    String value = jsonObject.optString(key);
                    System.out.println(key + "    " + value);

                    System.out.println("=====================1111111==========================");
                    JSONObject jsonObject2 = new JSONObject(value);
                    Iterator keys2 = jsonObject2.keys();
                    int k = 0;
                    while (keys2.hasNext()) {
                        StringBuffer sb = new StringBuffer();

                        String key2 = String.valueOf(keys2.next());
                        String value2 = jsonObject2.optString(key2);
                        System.out.println("key:" + key2 + "  value:" + value2);
                        sb.append(key2 + "\t" + value2);
                        list[j][k] = sb.toString();
                        k++;
                    }
                    j++;
                }

                List<String> list2 = arrStr(list);
                System.out.println("***********************开始打印结果222222********************");
                for (String ss : list2) {
                    ArrayList<String> outList2 = new ArrayList<>();
                    String[] strArray = ss.split(",");

                    String res0 = strArray[0];
                    if (res0.equals("null")) {
                        outList2.add(null);
                        outList2.add(null);
                    } else {
                        String[] arr0 = strArray[0].split("\t");
                        outList2.add(arr0[0]);
                        outList2.add(arr0[1]);
                    }

                    String res1 = strArray[1];
                    if (res1.equals("null")) {
                        outList2.add(null);
                        outList2.add(null);
                    } else {
                        String[] arr1 = strArray[1].split("\t");
                        outList2.add(arr1[0]);
                        outList2.add(arr1[1]);
                    }

                    String res2 = strArray[2];
                    if (res2.equals("null")) {
                        outList2.add(null);
                        outList2.add(null);
                    } else {
                        String[] arr2 = strArray[2].split("\t");
                        outList2.add(arr2[0]);
                        outList2.add(arr2[1]);
                    }

                    String res3 = strArray[3];
                    if (res3.equals("null")) {
                        outList2.add(null);
                        outList2.add(null);
                    } else {
                        String[] arr3 = strArray[3].split("\t");
                        outList2.add(arr3[0]);
                        outList2.add(arr3[1]);
                    }

                    String res4 = strArray[4];
                    if (res4.equals("null")) {
                        outList2.add(null);
                        outList2.add(null);
                    } else {
                        String[] arr4 = strArray[4].split("\t");
                        outList2.add(arr4[0]);
                        outList2.add(arr4[1]);
                    }

                    String res5 = strArray[5];
                    if (res5.equals("null")) {
                        outList2.add(null);
                        outList2.add(null);
                    } else {
                        String[] arr5 = strArray[5].split("\t");
                        outList2.add(arr5[0]);
                        outList2.add(arr5[1]);
                    }

                    for (String str : outList2) {
                        System.out.println(str);
                    }
                }
            } catch (JSONException je) {
                je.printStackTrace();
            }

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

    public static void arr(int[][] arr) {
        int[][] arrtrans = new int[arr[0].length][arr.length];
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr[0].length; j++) {
                arrtrans[j][i] = arr[i][j];
            }
        }
        List<String> list = new ArrayList<>();
        for (int i = 0; i < arrtrans.length; i++) {
            for (int j = 0; j < arrtrans[0].length; j++) {
                System.out.print(arrtrans[i][j]);
            }
            System.out.println();
        }
    }

    public static List<String> arrStr(String[][] arr) {
        String[][] arrtrans = new String[arr[0].length][arr.length];
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr[0].length; j++) {
                arrtrans[j][i] = arr[i][j];
            }
        }
        List<String> list = new ArrayList<>();

        for (int i = 0; i < arrtrans.length; i++) {
            StringBuffer sb = new StringBuffer();
            for (int j = 0; j < arrtrans[0].length; j++) {
                // System.out.print(arrtrans[i][j]);
                //System.out.print("\t");
                sb.append(arrtrans[i][j] + ",");
            }
            String str = sb.toString();
            str = str.substring(0, str.length() - 1);
            //System.out.print(str);
            list.add(str);
            System.out.println();
        }

        List<String> list2 = new ArrayList<>();
        System.out.println("********************* arrStr 2222222**********************");
        for (String str : list) {
            int num = countStr(str, "null");
            if (num == 6) continue;
            list2.add(str);
            System.out.println(str);
        }

        return list2;

    }

    /**
     * @param str     原字符串
     * @param sToFind 需要查找的字符串
     * @return 返回在原字符串中sToFind出现的次数
     */
    private static int countStr(String str, String sToFind) {
        int num = 0;
        while (str.contains(sToFind)) {
            str = str.substring(str.indexOf(sToFind) + sToFind.length());
            num++;
        }
        return num;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //1.定义输出数据的列名和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        //2.添加输出数据的列名
        fieldNames.add("key1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value1");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("key2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value2");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("key3");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value3");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("key4");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value4");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("key5");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value5");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("key6");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("value6");
        // 3.定义输出数据的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Object obj = args[0];
        System.out.println("parames ： " + obj);
        if (null != obj && !"NULL".equals(obj)) {
            String arg = args[0].toString();
            try {
                JSONObject jsonObject = new JSONObject(arg);
                //通过迭代器获取这段json当中所有的key值
                Iterator keys = jsonObject.keys();

                String[][] list = new String[6][60];
                int j = 0;
                while (keys.hasNext()) {
                    String key = String.valueOf(keys.next());
                    String value = jsonObject.optString(key);
                    System.out.println(key + "    " + value);

                    System.out.println("=====================1111111==========================");
                    JSONObject jsonObject2 = new JSONObject(value);
                    Iterator keys2 = jsonObject2.keys();
                    int k = 0;
                    while (keys2.hasNext()) {
                        StringBuffer sb = new StringBuffer();

                        String key2 = String.valueOf(keys2.next());
                        String value2 = jsonObject2.optString(key2);
                        System.out.println("key:" + key2 + "  value:" + value2);
                        sb.append(key2 + "\t" + value2);
                        list[j][k] = sb.toString();
                        k++;
                    }
                    j++;
                }

                List<String> list2 = arrStr(list);
                System.out.println("***********************开始打印结果333333********************");
                for (String ss : list2) {
                    String[] strArray = ss.split(",");
                    outList.clear();

                    String res0 = strArray[0];
                    if (res0.equals("null")) {
                        outList.add(null);
                        outList.add(null);
                    } else {
                        String[] arr0 = strArray[0].split("\t");
                        outList.add(arr0[0]);
                        outList.add(arr0[1]);
                    }

                    String res1 = strArray[1];
                    if (res1.equals("null")) {
                        outList.add(null);
                        outList.add(null);
                    } else {
                        String[] arr1 = strArray[1].split("\t");
                        outList.add(arr1[0]);
                        outList.add(arr1[1]);
                    }

                    String res2 = strArray[2];
                    if (res2.equals("null")) {
                        outList.add(null);
                        outList.add(null);
                    } else {
                        String[] arr2 = strArray[2].split("\t");
                        outList.add(arr2[0]);
                        outList.add(arr2[1]);
                    }

                    String res3 = strArray[3];
                    if (res3.equals("null")) {
                        outList.add(null);
                        outList.add(null);
                    } else {
                        String[] arr3 = strArray[3].split("\t");
                        outList.add(arr3[0]);
                        outList.add(arr3[1]);
                    }

                    String res4 = strArray[4];
                    if (res4.equals("null")) {
                        outList.add(null);
                        outList.add(null);
                    } else {
                        String[] arr4 = strArray[4].split("\t");
                        outList.add(arr4[0]);
                        outList.add(arr4[1]);
                    }

                    String res5 = strArray[5];
                    if (res5.equals("null")) {
                        outList.add(null);
                        outList.add(null);
                    } else {
                        String[] arr5 = strArray[5].split("\t");
                        outList.add(arr5[0]);
                        outList.add(arr5[1]);
                    }

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
}
