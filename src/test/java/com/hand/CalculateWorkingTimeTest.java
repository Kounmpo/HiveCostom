package com.hand;

import com.hand.test.CalculationWorkingTime;
import org.junit.Test;
import java.text.ParseException;

/**
 * @author jiehui.huang
 * @version 1.0
 * @date 2021/7/23 10:04
 */
public class CalculateWorkingTimeTest {
    @Test
    public void test1() throws ParseException {
        CalculationWorkingTime calculationWorkingTime = new CalculationWorkingTime();
        System.out.println(calculationWorkingTime.evaluate(
                "2020-01-05 00:00:00",
                "2020-01-01 09:00:00",
                "2020-01-03 20:40:00",
                "sc",
                "127.0.0.1-6379-123456"));

        System.out.println(calculationWorkingTime.evaluate(
                "2020-01-01 09:00:00",
                "2020-01-03 20:40:00",
                true,
                false,
                0,
                "08:00:00",
                "21:00:00",
                "12:00:00",
                "14:00:00",
                "Y"
                ));
    }
}
