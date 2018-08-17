package parser.bean;

import bean.fact_idBean;

public class TableParser {

    /**
     * fact_id
     */
    public static fact_idBean getfact_id(String line) {
        fact_idBean fact_id = null;
        String arr[] = line.split("\t");
        if (arr.length < 15) {
            return fact_id;
        }
        fact_id = new fact_idBean();

        fact_id.setInner_id(arr[0]);
        fact_id.setAp(arr[1]);
        fact_id.setDate_id(arr[2]);
        fact_id.setProvided_id(arr[3]);
        fact_id.setExt_id(arr[4]);
        fact_id.setHost(arr[5]);
        fact_id.setUa(arr[6]);
        fact_id.setOs(arr[7]);
        fact_id.setDevice(arr[8]);
        fact_id.setArea(arr[9]);
        fact_id.setSource_id(arr[10]);
        fact_id.setPro_category(arr[11]);
        fact_id.setYear(arr[12]);
        fact_id.setMonth(arr[13]);
        fact_id.setMonth(arr[14]);
        return fact_id;
    }
}