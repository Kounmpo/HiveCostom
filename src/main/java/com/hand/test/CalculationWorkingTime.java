package com.hand.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * hive自定义函数 计算工作时长
 *
 * @author jiehui.huang
 * @version 1.0
 * @date 2021/7/15 15:05
 */


public class CalculationWorkingTime extends UDF {

    private static final String YYYY_MM_DD = "yyyy-MM-dd";
    private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    private static final String HH_MM_SS = "HH:mm:ss";
    private static final Long DAY = 86400000L;
    private static final Long HOURS = 3600000L;
    private static final String RULES_KEY = "hap:report:duration_rule:";
    private static final String CALENDER_KEY = "hap:report:work_calender";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat();

    /**
     * 计算具体时长
     *
     * @param createdDateTime  创建时间
     * @param beginDateTime    开始时间
     * @param endDateTime      结束时间
     * @param organizationCode 公司code字段
     * @param redisInfos       redis连接信息
     * @return 毫秒值
     * @throws ParseException
     */
    public Text evaluate(String createdDateTime,
                         String beginDateTime,
                         String endDateTime,
                         String organizationCode,
                         String redisInfos) throws ParseException {

        final RedisWrapper redisWrapper = getRedisWrapper(redisInfos);
        final Jedis jedis = redisWrapper.getJedis();
        // 返回一个毫秒值
        long result = 0L;
        String beginDateStr = beginDateTime.substring(0, 10);
        String endDateStr = endDateTime.substring(0, 10);
        String createdDateStr = createdDateTime.substring(0, 10);
        // 用于判断输入的开始日期与结束日期是否为同一天
        DATE_FORMAT.applyPattern(YYYY_MM_DD);
        Date beginDate = DATE_FORMAT.parse(beginDateStr);
        Date endDate = DATE_FORMAT.parse(endDateStr);
        Date createdDate = DATE_FORMAT.parse(createdDateStr);
        // 用于计算时长
        DATE_FORMAT.applyPattern(YYYY_MM_DD_HH_MM_SS);
        Date startDateTime = DATE_FORMAT.parse(beginDateTime);
        Date closeDateTime = DATE_FORMAT.parse(endDateTime);
        // 用于计算时长
        DATE_FORMAT.applyPattern(HH_MM_SS);
        // 输入的开始时间
        Date beginTime = DATE_FORMAT.parse(beginDateTime.substring(11, 19));
        // 输入的结束时间
        Date endTime = DATE_FORMAT.parse(endDateTime.substring(11, 19));
        if (StringUtils.isEmpty(beginDateTime)
                || StringUtils.isEmpty(endDateTime)
                || StringUtils.isEmpty(organizationCode)) {
            return new Text("0");
        }
        // 从redis查询出时长规则并对日期进行相应的转化
        List<String> durationRules = jedis.lrange(RULES_KEY + organizationCode, 0, -1);
        // 获取所有节假日信息
        List<String> holidays = holidayList(jedis.lrange(CALENDER_KEY, 0, -1));
        redisWrapper.returnResource(jedis);
        // 判断开始日期与结束日期是否是节假日
        boolean beginDateFlag = holidayFlag(holidays, beginDateStr);
        boolean endDateFlag = holidayFlag(holidays, endDateStr);
        // 开始日期大于结束日期
        if (startDateTime.compareTo(closeDateTime) > 0) {
            return new Text("-1");
        }
        // 开始日期与结束日期之间的节假日天数
        int countHolidays = countHolidays(holidays, beginDateStr, endDateStr);
        for (String rule :
                durationRules) {
            final JSONObject ruleJson = JSON.parseObject(rule);
            // 格式化HH:mm:ss
            DATE_FORMAT.applyPattern(HH_MM_SS);
            final Date workingTime = DATE_FORMAT.parse(ruleJson.getString("workingTime").substring(11, 19));
            final Date closingTime = DATE_FORMAT.parse(ruleJson.getString("closingTime").substring(11, 19));
            final Date startLunchTime = DATE_FORMAT.parse(ruleJson.getString("startLunchTime").substring(11, 19));
            final Date endLunchTime = DATE_FORMAT.parse(ruleJson.getString("endLunchTime").substring(11, 19));
            DATE_FORMAT.applyPattern(YYYY_MM_DD);
            final Date effectiveDate = DATE_FORMAT.parse(ruleJson.getString("effectiveDate"));
            final Date expiredDate;
            if (ruleJson.getString("expiredDate") == null) {
                expiredDate = null;
            } else {
                expiredDate = DATE_FORMAT.parse(ruleJson.getString("expiredDate"));
            }
            final String excludeHolidayFlag = ruleJson.getString("excludeHolidayFlag");
            // 开始时间与结束时间为同一天
            if (beginDate.compareTo(endDate) == 0) {
                // 失效日期为空
                if (createdDate.compareTo(effectiveDate) >= 0 && null == expiredDate) {
                    if (createdDate.compareTo(effectiveDate) < 0) {
                        return new Text("-1");
                    }
                    // 不排除节假日
                    if ("N".equals(excludeHolidayFlag)) {
                        result = getDailyTime(beginTime, endTime, workingTime, closingTime, startLunchTime, endLunchTime);
                    } else {
                        // 排除节假日, 判断开始日期与结束日期是否是节假日
                        // 开始日期为节假日
                        if (beginDateFlag) {
                            return new Text("0");
                        } else {
                            result = getDailyTime(beginTime, endTime, workingTime, closingTime, startLunchTime, endLunchTime);
                        }
                    }
                } else if (createdDate.compareTo(effectiveDate) >= 0
                        && null != expiredDate) {
                    if (createdDate.compareTo(expiredDate) <= 0) {
                        // 不排除节假日
                        if ("N".equals(excludeHolidayFlag)) {
                            result = getDailyTime(beginTime, endTime, workingTime, closingTime, startLunchTime, endLunchTime);
                        } else {
                            // 排除节假日, 判断开始日期与结束日期是否是节假日
                            // 开始日期为节假日
                            if (beginDateFlag) {
                                return new Text("0");
                            } else {
                                result = getDailyTime(beginTime, endTime, workingTime, closingTime, startLunchTime, endLunchTime);
                            }
                        }
                    }
                } else {
                    return new Text("0");
                }
            } else {
                // 开始日期与结束日期不是同一天
                if (createdDate.compareTo(effectiveDate) >= 0 && null == expiredDate) {
                    // 确定计算规则 不排除节假日
                    if ("N".equals(excludeHolidayFlag)) {
                        result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                + getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                + ((endDate.getTime() - beginDate.getTime()) / DAY - 1)
                                * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                    } else {
                        // 排除节假日
                        // 判断开始日期与结束日期是否是节假日并且开始时间与结束时间之间有多少个节假日
                        if (beginDateFlag && endDateFlag) {
                            result = ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        } else if ((!beginDateFlag) && endDateFlag) {
                            result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        } else if (beginDateFlag && !(endDateFlag)) {
                            result = getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        } else {
                            result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        }
                    }
                } else if (createdDate.compareTo(effectiveDate) >= 0
                        && createdDate.compareTo(expiredDate) <= 0
                        && expiredDate != null) {
                    if ("N".equals(excludeHolidayFlag)) {
                        result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                + getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                + (((endDate.getTime() - beginDate.getTime()) / DAY) - 1)
                                * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                    } else {
                        // 排除节假日
                        // 判断开始日期与结束日期是否是节假日并且开始时间与结束时间之间有多少个节假日
                        if (beginDateFlag && endDateFlag) {
                            result = ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        } else if ((!beginDateFlag) && endDateFlag) {
                            result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        } else if (beginDateFlag && !(endDateFlag)) {
                            result = getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        } else {
                            result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                                    + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                                    * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                        }
                    }
                }
            }
        }
        final BigDecimal bigDecimal = new BigDecimal(((double) result) / HOURS);
        return new Text(bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
    }

    private RedisWrapper getRedisWrapper(String redisInfos) {
        // 解析redis连接信息
        String[] redisInfo = redisInfos.split("-");
        String host = redisInfo[0];
        int port = Integer.parseInt(redisInfo[1]);
        String pass = redisInfo[2];
        final RedisWrapper redisWrapper = new RedisWrapper(new JedisPoolConfig(), host, port, pass);
        return redisWrapper;
    }

    /**
     * 当开始日期与结束日期是同一天时根据具体时长规则计算每天的时长
     *
     * @param startTime      开始时间
     * @param endTime        结束时间
     * @param workingTime    上班时间
     * @param closingTime    下班时间
     * @param startLunchTime 开始午休时间
     * @param endLunchTime   午休时间至
     * @return 返回具体时长的毫秒数
     */


    private Long getDailyTime(Date startTime, Date endTime, Date workingTime, Date closingTime, Date startLunchTime, Date endLunchTime) {
        long result;
        long startTimeCal = startTime.getTime();
        long endTimeCal = endTime.getTime();
        long workingTimeCal = workingTime.getTime();
        long closingTimeCal = closingTime.getTime();
        long startLunchTimeCal = startLunchTime.getTime();
        long endLunchTimeCal = endLunchTime.getTime();


        if (startTimeCal < workingTimeCal) {
            if (endTimeCal > closingTimeCal) {
                result = closingTimeCal - workingTimeCal - (endLunchTimeCal - startLunchTimeCal);
            } else if (endTimeCal <= closingTimeCal && endTimeCal > endLunchTimeCal) {
                result = endTimeCal - workingTimeCal - (endLunchTimeCal - startLunchTimeCal);
            } else if (endTimeCal > startLunchTimeCal && endTimeCal <= endLunchTimeCal) {
                result = startLunchTimeCal - workingTimeCal;
            } else if (endTimeCal <= startLunchTimeCal && endTimeCal > workingTimeCal) {
                result = endTimeCal - workingTimeCal;
            } else {
                result = 0L;
            }
        } else if (startTimeCal >= workingTimeCal && startTimeCal < startLunchTimeCal) {
            if (endTimeCal > closingTimeCal) {
                result = closingTimeCal - startTimeCal - (endLunchTimeCal - startLunchTimeCal);
            } else if (endTimeCal <= closingTimeCal && endTimeCal >= endLunchTimeCal) {
                result = endTimeCal - workingTimeCal - (endLunchTimeCal - startLunchTimeCal);
            } else if (endTimeCal >= startLunchTimeCal && endTimeCal <= endLunchTimeCal) {
                result = startLunchTimeCal - startTimeCal;
            } else {
                result = endTimeCal - startTimeCal;
            }
        } else if (startTimeCal >= startLunchTimeCal && startTimeCal < endLunchTimeCal) {
            if (endTimeCal >= closingTimeCal) {
                result = closingTimeCal - endLunchTimeCal;
            } else if (endTimeCal <= closingTimeCal && endTimeCal >= endLunchTimeCal) {
                result = endTimeCal - endLunchTimeCal;
            } else {
                result = 0L;
            }
        } else if (startTimeCal >= endLunchTimeCal && startTimeCal < closingTimeCal) {
            if (endTimeCal >= closingTimeCal) {
                result = closingTimeCal - startTimeCal;
            } else {
                result = endTimeCal - startTimeCal;
            }
        } else {
            result = 0L;
        }
        return result;
    }

    /**
     * 计算完整的一天的总时长
     *
     * @param workingTime    上班时间
     * @param closingTime    下班时间
     * @param startLunchTime 午休时间
     * @param endLunchTime   午休时间至
     * @return 一天的总时长
     */


    private Long getDailyTime(Date workingTime, Date closingTime, Date startLunchTime, Date endLunchTime) {
        return closingTime.getTime() - workingTime.getTime() - (endLunchTime.getTime() - startLunchTime.getTime());
    }

    /**
     * 计算开始日期当天的时长
     *
     * @param startTime      开始时间
     * @param workingTime    上班时间
     * @param closingTime    下班时间
     * @param startLunchTime 午休时间
     * @param endLunchTime   午休时间至
     * @return 时长
     */


    private Long getBeginDailyTime(Date startTime,
                                   Date workingTime,
                                   Date closingTime,
                                   Date startLunchTime,
                                   Date endLunchTime) {
        long result;
        long startTimeCal = startTime.getTime();
        long workingTimeCal = workingTime.getTime();
        long closingTimeCal = closingTime.getTime();
        long startLunchTimeCal = startLunchTime.getTime();
        long endLunchTimeCal = endLunchTime.getTime();
        if (startTimeCal < workingTimeCal) {
            result = getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
        } else if (startTimeCal >= workingTimeCal && startTimeCal < startLunchTimeCal) {
            result = closingTimeCal - startTimeCal - (endLunchTimeCal - startLunchTimeCal);
        } else if (startTimeCal >= startLunchTimeCal && startTimeCal < endLunchTimeCal) {
            result = closingTimeCal - endLunchTimeCal;
        } else if (startTimeCal >= endLunchTimeCal && startTimeCal <= closingTimeCal) {
            result = closingTimeCal - startTimeCal;
        } else {
            return 0L;
        }
        return result;
    }

    /**
     * 计算结束日期当天的时长
     *
     * @param endTime        结束时间
     * @param workingTime    上班时间
     * @param closingTime    下班时间
     * @param startLunchTime 午休时间
     * @param endLunchTime   午休时间至
     * @return 总时长
     */


    private Long getEndDailyTime(Date endTime,
                                 Date workingTime,
                                 Date closingTime,
                                 Date startLunchTime,
                                 Date endLunchTime) {

        long endTimeCal = endTime.getTime();
        long workingTimeCal = workingTime.getTime();
        long closingTimeCal = closingTime.getTime();
        long startLunchTimeCal = startLunchTime.getTime();
        long endLunchTimeCal = endLunchTime.getTime();
        if (endTimeCal < workingTimeCal) {
            return 0L;
        } else if (endTimeCal >= workingTimeCal && endTimeCal < startLunchTimeCal) {
            return endTimeCal - workingTimeCal;
        } else if (endTimeCal >= startLunchTimeCal && endTimeCal < endLunchTimeCal) {
            return startLunchTimeCal - workingTimeCal;
        } else if (endTimeCal >= endLunchTimeCal && endTimeCal <= closingTimeCal) {
            return endTimeCal - workingTimeCal - (endLunchTimeCal - startLunchTimeCal);
        } else {
            return getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
        }
    }

    /**
     * 处理日历表的json字符串
     *
     * @param list json字符串的列表
     * @return 只包含节假日日期的列表集合
     */
    private List<String> holidayList(List<String> list) {
        List<String> holidayList = new ArrayList<String>();
        for (String str :
                list) {
            holidayList.add(JSON.parseObject(str).getString("dates"));
        }
        return holidayList;
    }

    /**
     * 计算开始日期与结束日期之间的节假日天数天数
     *
     * @param holidayList  节假日列表集合
     * @param beginDateStr 开始日期
     * @param endDateStr   结束日期
     * @return 节假日天数
     */
    private int countHolidays(List<String> holidayList, String beginDateStr, String endDateStr) {
        int day = 0;
        for (String holiday :
                holidayList) {
            if (holiday.compareTo(beginDateStr) > 0 && holiday.compareTo(endDateStr) < 0) {
                day += 1;
            }
        }
        return day;
    }

    /**
     * 判断日期字符串是否为节假日
     *
     * @param list 节假日列表
     * @return boolean
     */
    private boolean holidayFlag(List<String> list, String dateTime) {
        return list.contains(dateTime.substring(0, 10));
    }

    public Text evaluate(String beginDateTime,
                         String endDateTime,
                         boolean beginDateFlag,
                         boolean endDateFlag,
                         int countHolidays,
                         String workingTimeStr,
                         String closingTimeStr,
                         String startLunchTimeStr,
                         String endLunchTimeStr,
                         String excludeHolidayFlag
    ) throws ParseException {

        // 返回一个毫秒值
        long result;
        String beginDateStr = beginDateTime.substring(0, 10);
        String endDateStr = endDateTime.substring(0, 10);
        // 用于判断输入的开始日期与结束日期是否为同一天
        DATE_FORMAT.applyPattern(YYYY_MM_DD);
        Date beginDate = DATE_FORMAT.parse(beginDateStr);
        Date endDate = DATE_FORMAT.parse(endDateStr);
        // 用于计算时长
        DATE_FORMAT.applyPattern(YYYY_MM_DD_HH_MM_SS);
        Date startDateTime = DATE_FORMAT.parse(beginDateTime);
        Date closeDateTime = DATE_FORMAT.parse(endDateTime);
        // 用于计算时长
        DATE_FORMAT.applyPattern(HH_MM_SS);
        // 输入的开始时间
        Date beginTime = DATE_FORMAT.parse(beginDateTime.substring(11, 19));
        // 输入的结束时间
        Date endTime = DATE_FORMAT.parse(endDateTime.substring(11, 19));
        if (StringUtils.isEmpty(beginDateTime)
                || StringUtils.isEmpty(endDateTime)) {
            return new Text("0");
        }
        // 开始日期大于结束日期
        if (startDateTime.compareTo(closeDateTime) > 0) {
            return new Text("-1");
        }
        // 格式化HH:mm:ss
        DATE_FORMAT.applyPattern(HH_MM_SS);
        final Date workingTime = DATE_FORMAT.parse(workingTimeStr);
        final Date closingTime = DATE_FORMAT.parse(closingTimeStr);
        final Date startLunchTime = DATE_FORMAT.parse(startLunchTimeStr);
        final Date endLunchTime = DATE_FORMAT.parse(endLunchTimeStr);
        if (beginDate.compareTo(endDate) == 0) {
            // 不排除节假日
            if ("N".equals(excludeHolidayFlag)) {
                result = getDailyTime(beginTime, endTime, workingTime, closingTime, startLunchTime, endLunchTime);
            } else {
                // 排除节假日, 判断开始日期与结束日期是否是节假日
                // 开始日期为节假日
                if (beginDateFlag) {
                    return new Text("0");
                } else {
                    result = getDailyTime(beginTime, endTime, workingTime, closingTime, startLunchTime, endLunchTime);
                }
            }
        } else {
            // 开始日期与结束日期不是同一天
            // 确定计算规则 不排除节假日
            if ("N".equals(excludeHolidayFlag)) {
                result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                        + getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                        + ((endDate.getTime() - beginDate.getTime()) / DAY - 1)
                        * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
            } else {
                // 排除节假日
                // 判断开始日期与结束日期是否是节假日并且开始时间与结束时间之间有多少个节假日
                if (beginDateFlag && endDateFlag) {
                    result = ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                            * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                } else if ((!beginDateFlag) && endDateFlag) {
                    result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                            + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                            * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                } else if (beginDateFlag && !(endDateFlag)) {
                    result = getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                            + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                            * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                } else {
                    result = getBeginDailyTime(beginTime, workingTime, closingTime, startLunchTime, endLunchTime)
                            + getEndDailyTime(endTime, workingTime, closingTime, startLunchTime, endLunchTime)
                            + ((endDate.getTime() - beginDate.getTime()) / DAY - 1 - countHolidays)
                            * getDailyTime(workingTime, closingTime, startLunchTime, endLunchTime);
                }
            }
        }
        final BigDecimal bigDecimal = new BigDecimal(((double) result) / HOURS);
        return new Text(bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).toString());
    }
}