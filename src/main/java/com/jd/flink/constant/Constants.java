package com.jd.flink.constant;
/*https://coding.jd.com/backstage_system/flink/tree/master/idc_es_v2/src/main/java/com/jd/dmsa/constant
此类有必要
* */
public class Constants {

    //kafka
    public static String CONSUMER_TOPIC = "log-client-cdnlive-bytedance";
    public static String PRODUCER_TOPIC = "cdncld_reqstat_result_test";

    public static String USERNAME = "deeplog.jd.com";

    public static String CONSUMER_CLIENTID = "C23359c66";
    public static String CONSUMER_PASSWORD = "wDWZEaUu9o21T1fG";

    public static String PRODUCER_CLIENTID = "Pc2faf958";
    public static String PRODUCER_PASSWORD = "FIiC8BOCks7mbwjJ";


    //checkpoint
    public static final String HDFS_CHECKPOINT_PATH = "hdfs://ns-cdn/user/flink/checkpoint/";

    //log
    public static String LOG_END = "END";
    public static String LOG_DELIMITER = "   :";
    public static int LOG_MSEC_INDEX = 1;
    public static int LOG_DOMAIN_INDEX = 7;
    public static int LOG_URL_INDEX = 9;
    public static int LOG_FLOW_INDEX = 19;

}
