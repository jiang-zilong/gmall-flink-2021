package com.jiangzilong.app.function;

/**
 * @Author:JZL
 * @Date: 2022/1/7  14:23
 * @Version 1.0
 */

import com.jiangzilong.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * Desc: 自定义UDTF函数实现分词功能
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
        try {
            //分词
            List<String> words = KeywordUtil.splitKeyWord(str);

            //遍历并写出
            for (String word : words) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }

    }


}
