package brickhouse.udf.json;


import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.yecht.Data;


@Description(name = "get_json_array",
        value = "_FUNC_(expr) - get json array by index",
        extended = "Example:\n "
                + "  > SELECT _FUNC_('[{\"a\":\"a1\"}, {}, {}]', 0) FROM src LIMIT 1;\n"
                + "  '{\"a\":\"a1\"}'")
public class StrArrayUDF extends UDF {
    public static String evaluate(String jsonArray, int index) throws HiveException {

        JSONArray parseArray = JSONUtil.parseArray(jsonArray);

        Object o = parseArray.get(index);

        return o.toString();
    }

    public static void main(String[] args) throws HiveException {
        String strArrayJson = "[{\"componentName\":\"DDSelectField\",\"componentType\":\"DDSelectField\",\"props\":{\"bizAlias\":\"type\",\"holidayOptions\":[],\"id\":\"NumberField-OSUCIDP2\",\"label\":\"加班类型\",\"options\":[\"加班调休\",\"协定加班\"],\"required\":true},\"value\":\"协定加班\"},{\"componentName\":\"InnerContactField\",\"componentType\":\"InnerContactField\",\"extValue\":\"[{\\\"emplId\\\":\\\"262928072121506164\\\",\\\"read\\\":false,\\\"name\\\":\\\"xxx\\\",\\\"readTime\\\":0}]\",\"props\":{\"bizAlias\":\"partner\",\"choice\":1,\"holidayOptions\":[],\"id\":\"InnerContactField-OSUCIDP3\",\"label\":\"加班人\",\"placeholder\":\"请选择\",\"required\":true},\"value\":\"xxx\"},{\"componentName\":\"TextNote\",\"componentType\":\"TextNote\",\"props\":{\"align\":\"top\",\"bizAlias\":\"partnerTip\",\"content\":\"发起人可以为同事提交加班\",\"hiddenInApprovalDetail\":true,\"holidayOptions\":[],\"id\":\"TextNote-OSUCIDP4\",\"notPrint\":\"1\"}},{\"componentName\":\"DDDateField\",\"componentType\":\"DDDateField\",\"props\":{\"bizAlias\":\"startTime\",\"holidayOptions\":[],\"id\":\"DDDateField-OSUCIDP5\",\"label\":\"开始时间\",\"placeholder\":\"请选择\",\"required\":true,\"useCalendar\":true},\"value\":\"2021-01-01 上午\"},{\"componentName\":\"DDDateField\",\"componentType\":\"DDDateField\",\"props\":{\"bizAlias\":\"finishTime\",\"holidayOptions\":[],\"id\":\"DDDateField-OSUCIDP6\",\"label\":\"结束时间\",\"placeholder\":\"请选择\",\"required\":true,\"useCalendar\":true},\"value\":\"2021-01-01 下午\"},{\"children\":[{\"componentName\":\"DDDateField\",\"componentType\":\"DDDateField\",\"props\":{\"bizAlias\":\"overtimeDate\",\"disabled\":true,\"format\":\"yyyy-MM-dd\",\"holidayOptions\":[],\"id\":\"DDDateField-OSUCIDP8\",\"label\":\"加班时间\",\"placeholder\":\"请选择\",\"required\":true}},{\"componentName\":\"NumberField\",\"componentType\":\"NumberField\",\"props\":{\"bizAlias\":\"overtimeDuration\",\"holidayOptions\":[],\"id\":\"NumberField-OSUCIDP9\",\"label\":\"\",\"placeholder\":\"\",\"required\":true}}],\"componentName\":\"TableField\",\"componentType\":\"TableField\",\"props\":{\"actionName\":\"增加明细\",\"bizAlias\":\"everyDayDuration\",\"disabled\":true,\"hidden\":true,\"hideLabel\":true,\"holidayOptions\":[],\"id\":\"TableField-OSUCIDP7\",\"label\":\"明细\",\"mainTitle\":\"为了方便统计，请填写每日加班时长\",\"statField\":[]}},{\"componentName\":\"NumberField\",\"componentType\":\"NumberField\",\"extValue\":\"{\\\"extension\\\":\\\"{\\\\\\\"tag\\\\\\\":\\\\\\\"\\\\\\\"}\\\",\\\"featureMap\\\":{\\\"overtimeUrl\\\":\\\"https://attend.dingtalk.com/attend/index.html?corpId=dingec5578628aee68db35c2f4657eb6378f&showmenu=false&dd_share=false&overtimeId=140440289#admin/overtimeRuleDetail\\\",\\\"remark\\\":\\\" 加班时长以审批单为准；\\\",\\\"overtimeSettingId\\\":\\\"140440289\\\"},\\\"_from\\\":\\\"2021-01-01 上午\\\",\\\"pushTag\\\":\\\"\\\",\\\"excludePrincipalUserIds\\\":[],\\\"durationInDay\\\":1,\\\"isModifiable\\\":true,\\\"durationInHour\\\":8,\\\"compressedValue\\\":\\\"1f8b0800000000000000a551cb4ec33010fc973d47c8a5254d728346884a14a13e0e08f5b0b25d6ae1da911f4055f51bf806eef05bf01bacdb4a8dca11df7666d6b33bbb018e9a478d418eac905075331032a0d2b7ca07a81e3720a2c3a0ac199a1ad75075ce18cb5ae08d8d0eaae2041d291383f450f50a76424d24b74640755e148958480cd125e906b069f4fadad9d554ad68964ececa5e97f5587ad99e9dda2377d1c9fb65b9e3b846efef303130f879fffafefc184ce0809363b2f6bb75a41197dc594f15f551d5f22afbece0e503bad0d2edea9632cff7536de719283f9629ab056a2f337891ce931b0d4251c1368325fa411a03aae022092c29027d551f2299044a606ff36add734db7f8b37b22927d3bd7e4fdefe31cf19951b404d4970f949b7ce33a0a79ef94e1aa413df3d20d454a707e5c602c051dce5f9131acd044d4d4d944bf9ce21341b0fd05232fb7d35e020000\\\",\\\"unit\\\":\\\"DAY\\\",\\\"overtimeRedressBy\\\":\\\"manual\\\",\\\"detailList\\\":[{\\\"classInfo\\\":{\\\"name\\\":\\\"C班次CS\\\",\\\"hasClass\\\":true,\\\"sections\\\":[{\\\"endAcross\\\":0,\\\"startTime\\\":1609466400000,\\\"endTime\\\":1609497000000,\\\"startAcross\\\":0}]},\\\"workDate\\\":1609430400000,\\\"isRest\\\":false,\\\"workTimeMinutes\\\":480,\\\"approveInfo\\\":{\\\"fromAcross\\\":0,\\\"overtimeDurationStatus\\\":0,\\\"toAcross\\\":0,\\\"fromTime\\\":1609430400000,\\\"durationInDay\\\":1,\\\"toTime\\\":1609516799000,\\\"durationInHour\\\":8}}],\\\"_to\\\":\\\"2021-01-01 下午\\\"}\",\"props\":{\"bizAlias\":\"duration\",\"disabled\":true,\"holidayOptions\":[],\"id\":\"NumberField-OSUCIDP10\",\"label\":\"时长\",\"placeholder\":\"请输入时长\",\"required\":true},\"value\":\"1\"},{\"componentName\":\"DDSelectField\",\"componentType\":\"DDSelectField\",\"extValue\":\"charge\",\"props\":{\"bizAlias\":\"compensation\",\"disabled\":false,\"hidden\":false,\"holidayOptions\":[],\"id\":\"DDSelectField-OSUCIDP11\",\"label\":\"加班补偿\",\"placeholder\":\"请选择\"},\"value\":\"加班费\"}]";
        String evaluate = evaluate(strArrayJson, 0);
        String evaluate1 = evaluate(strArrayJson, 1);
        System.out.println(evaluate);
        System.out.println(evaluate1);
    }
}
