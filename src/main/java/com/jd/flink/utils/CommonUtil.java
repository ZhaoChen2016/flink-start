package com.jd.flink.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
@Slf4j
public class CommonUtil {
    public static ArrayList<String> split(String line, String delimiter, String endDelimiter, int size){
        /*日志可能随时添加字段，预留8个字段，避免内存拷贝*/
        ArrayList<String> array = new ArrayList<>(size);
        int idx = line.indexOf(delimiter);
        int from = 0;
        while (idx != -1) {
            array.add(line.substring(from, idx));
            from = idx + delimiter.length();
            idx = line.indexOf(delimiter, from);
        }
        if (from < line.length()) {
            String lastEme = line.substring(from);
            if (endDelimiter.equals(lastEme)) {
                return array;
            } else {
                return new ArrayList();
            }
        } else {
            return new ArrayList<>();
        }
    }

    public static void main(String[] args) {
        String s = "2021-08-27T17:09:31+08:00   :1670488535.322   :0   :120.229.53.24   :111.48.197.3   :GET   :https   :END";
        split(s,"   :", "END", 10);
    }
}
