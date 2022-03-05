package brickhouse.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author: liusilin
 * @Date: 2022/3/5 13:24
 * @Description:
 */
public class DateFormatUDF extends UDF {
    public static HashMap<String, String> monthMap = new HashMap<String, String>() {
        {
            put("Jan", "01");
            put("Feb", "02");
            put("Mar", "03");
            put("Apr", "04");
            put("May", "05");
            put("Jun", "06");
            put("Jul", "07");
            put("Aug", "08");
            put("Sep", "09");
            put("Oct", "10");
            put("Nov", "11");
            put("Dec", "12");
        }
    };

    public String evaluate(String date) {
        String newDate = "";
        System.out.println("************************ " + date);
        if (date == null) {
            return null;
        }
        if (date.length() == 10) {
            return date;
        }
//        1989-2-27
        else if (date.length() == 9 && date.split("-").length == 3) {
            return date.split("-")[0] + "-0" + date.split("-")[1] + "-" + date.split("-")[2];
        }
//        1993-09-12 08:00:00
        if (date.length() == 19) {
            newDate = date.split(" ")[0];
        } else if (date.length() == 28) {
//            Mon Jun 06 09:00:00 CDT 1988
            newDate = date.split(" ")[5] + "-".concat(monthMap.get(date.split(" ")[1]) + "-" + date.split(" ")[2]);
        } else if (date.length() == 6) {
//            950903
            if (Integer.parseInt(date.substring(0, 1)) > 3) {
                newDate = "19" + date.substring(0, 2) + "-" + date.substring(2, 4) + "-" + date.substring(4, 6);
            } else {
//                000903
                newDate = "20" + date.substring(0, 2) + "-" + date.substring(2, 4) + "-" + date.substring(4, 6);
            }
        } else if (date.length() == 8) {
//            19900811
            newDate = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
        } else if (date.length() == 11) {
//            88387200000
            newDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.parseLong(date)));
        } else {
            System.out.println("Error date format: " + date);
            newDate = date;
        }
        return newDate;
    }

    public static void main(String[] args) {
        DateFormatUDF dateFormat = new DateFormatUDF();
        String date1 = dateFormat.evaluate("Mon Jun 06 09:00:00 CDT 1988");
        System.out.println(date1);

        String date2 = dateFormat.evaluate("19900811");
        System.out.println(date2);

        String date3 = dateFormat.evaluate("88387200000");
        System.out.println(date3);

        String date4 = dateFormat.evaluate("1993-09-12 08:00:00");
        System.out.println(date4);

        String date5 = dateFormat.evaluate("950903");
        System.out.println(date5);

        String date6 = dateFormat.evaluate("000903");
        System.out.println(date6);

        String date7 = dateFormat.evaluate("1989-2-27");
        System.out.println(date7);
    }
}
