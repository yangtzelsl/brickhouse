package brickhouse.udf.date;

import brickhouse.utils.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author: liusilin
 * @Date: 2022/3/5 13:15
 * @Description:
 */
public class DateDiffStrongUDF extends UDF {
    public String evaluate(String date1,String date2,String date3,int flag) {
        if(flag == 1){
            String maxDate = null;
            int result = DateUtils.compareDateMax(date1, date2);
            maxDate = result >= 0 ? date1:date2;

            result = DateUtils.compareDateMax(maxDate, date3);
            return result >= 0 ? maxDate:date3;
        }else{
            String minDate = "";
            int result = DateUtils.compareDateMin(date1, date2);
            minDate = result >= 0 ? date1:date2;
            result = DateUtils.compareDateMin(minDate, date3);
            return result >= 0 ? minDate:date3;
        }
    }
    public static void main(String[] args) {
        String date1=null;
        String date2="2020-05-31 17:15:12";
        String date3="2020-04-28 22:25:51";

        DateDiffStrongUDF dateDiffStrongUDF = new DateDiffStrongUDF();
        String minDate = dateDiffStrongUDF.evaluate(date1,date2,date3,0);
        System.out.println(minDate);

        String date12=null;
        String date22="2020-06-17 11:30:13";
        String date32="2020-06-19 18:49:53";

        String maxDate = dateDiffStrongUDF.evaluate(date12,date22,date32,1);
        System.out.println(maxDate);
        System.out.println("------------------------");


        String date13="2020-05-31 17:15:12";
        String date23=null;
        String date33="2020-04-28 22:25:51";
        String minDate2 = dateDiffStrongUDF.evaluate(date13,date23,date33,0);
        System.out.println(minDate2);

        String date14="2020-06-17 11:30:13";
        String date24=null;
        String date34="2020-06-19 18:49:53";
        String maxDate2 = dateDiffStrongUDF.evaluate(date14,date24,date34,1);
        System.out.println(maxDate2);
        System.out.println("------------------------");


        String date15="2020-05-31 17:15:12";
        String date25="2020-04-28 22:25:51";
        String date35=null;
        String minDate3 = dateDiffStrongUDF.evaluate(date15,date25,date35,0);
        System.out.println(minDate3);

        String date16="2020-06-17 11:30:13";
        String date26="2020-06-19 18:49:53";
        String date36=null;

        String maxDate4 = dateDiffStrongUDF.evaluate(date16,date26,date36,1);
        System.out.println(maxDate4);
        System.out.println("------------------------");

        String date17=null;
        String date27=null;
        String date37="2020-04-28 22:25:51";
        String minDate5 = dateDiffStrongUDF.evaluate(date17,date27,date37,0);
        System.out.println(minDate5);

        String date18= null;
        String date28=null;
        String date38="2020-06-19 18:49:53";

        String maxDate6 = dateDiffStrongUDF.evaluate(date18,date28,date38,1);
        System.out.println(maxDate6);
        System.out.println("------------------------");

        String date19=null;
        String date29="2020-04-28 22:25:51";
        String date39=null;
        String mindate7 = dateDiffStrongUDF.evaluate(date19,date29,date39,0);
        System.out.println(mindate7);

        String date110= null;
        String date210="2020-06-19 18:49:53";
        String date310=null;

        String maxdate8 = dateDiffStrongUDF.evaluate(date110,date210,date310,1);
        System.out.println(maxdate8);
        System.out.println("------------------------");

        String date111=null;
        String date211=null;
        String date311=null;
        String mindate9 = dateDiffStrongUDF.evaluate(date111,date211,date311,0);
        System.out.println(mindate9);

        String date112= null;
        String date212=null;
        String date312=null;

        String maxdate10 = dateDiffStrongUDF.evaluate(date112,date212,date312,1);
        System.out.println(maxdate10);
        System.out.println("------------------------");


    }
}
