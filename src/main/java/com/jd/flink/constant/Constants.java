package com.jd.flink.constant;
/*https://coding.jd.com/backstage_system/flink/tree/master/idc_es_v2/src/main/java/com/jd/dmsa/constant
此类有必要
* */
public class Constants {

    //kafka
    public static String CONSUMER_TOPIC = "";
    public static String PRODUCER_TOPIC = "";

    public static String USERNAME = "";

    public static String CONSUMER_CLIENTID = "";
    public static String CONSUMER_PASSWORD = "";

    public static String PRODUCER_CLIENTID = "";
    public static String PRODUCER_PASSWORD = "";


    //checkpoint
    public static final String HDFS_CHECKPOINT_PATH = "";

    //log
    public static String LOG_END = "END";
    public static String LOG_DELIMITER = "   :";
    public static int LOG_MSEC_INDEX = 1;
    public static int LOG_DOMAIN_INDEX = 7;
    public static int LOG_URL_INDEX = 9;
    public static int LOG_FLOW_INDEX = 19;

}
