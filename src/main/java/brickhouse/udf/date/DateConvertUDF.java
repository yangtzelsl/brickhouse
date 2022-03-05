package brickhouse.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author: liusilin
 * @Date: 2022/3/5 15:00
 * @Description:
 */
public class DateConvertUDF extends UDF {
    public String evaluate(Object date) throws ParseException {

        ThreadLocal<SimpleDateFormat> thl = new ThreadLocal<>();


        SimpleDateFormat sdf = thl.get();
        if (sdf == null) {
            sdf = new SimpleDateFormat("yyyyMMdd");
            thl.set(sdf);
        }

        String dateStr = null;
        if (date.getClass() == String.class) {
            dateStr = (String) date;
            try {
                Integer.valueOf(dateStr);
            } catch (Exception e) {
                return null;
            }
        } else if (date.getClass() == Integer.class) {
            dateStr = String.valueOf(date);
        } else {
            throw new ClassCastException("date:" + dateStr + ",can not be cast to string or int");
        }

        Date dd = sdf.parse(dateStr);
        Calendar cld = Calendar.getInstance();
        cld.setTime(new Date(dd.getTime()));

        String strDate = sdf.format(cld.getTime());

        String result = strDate.substring(0, 4) + "-" + strDate.substring(4, 6) + "-" + strDate.substring(6, 8);


        return result;

    }

    public static void main(String[] args) throws ParseException {
        String dd = new DateConvertUDF().evaluate("2021-03-01");
        System.out.println(dd);
    }
}
