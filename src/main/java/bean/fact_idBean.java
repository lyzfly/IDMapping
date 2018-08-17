package bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class fact_idBean implements Writable {

    private String inner_id;
    private String ap;
    private String date_id;
    private String provided_id;
    private String ext_id;
    private String host;
    private String ua;
    private String os;
    private String device;
    private String area;
    private String source_id;
    private String pro_category;
    private String year;
    private String month;
    private String day;
    private String muid;
    private String id;
    private String id_type;


    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeChars(inner_id);
        out.writeChars(ap);
        out.writeChars(date_id);
        out.writeChars(provided_id);
        out.writeChars(ext_id);
        out.writeChars(host);
        out.writeChars(ua);
        out.writeChars(os);
        out.writeChars(device);
        out.writeChars(area);
        out.writeChars(source_id);
        out.writeChars(pro_category);
        out.writeChars(year);
        out.writeChars(month);
        out.writeChars(day);
        out.writeChars(muid);
        out.writeChars(id);
        out.writeChars(id_type);

    }


    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        inner_id = in.readLine();
        ap = in.readLine();
        date_id = in.readLine();
        provided_id = in.readLine();
        ext_id = in.readLine();
        host = in.readLine();
        ua = in.readLine();
        os = in.readLine();
        device = in.readLine();
        area = in.readLine();
        source_id = in.readLine();
        pro_category = in.readLine();
        year = in.readLine();
        month = in.readLine();
        day = in.readLine();
        muid = in.readLine();
        id = in.readLine();
        id_type = in.readLine();

    }

    public String getInner_id() {
        return inner_id;
    }

    public void setInner_id(String inner_id) {
        this.inner_id = inner_id;
    }

    public String getAp() {
        return ap;
    }

    public void setAp(String ap) {
        this.ap = ap;
    }

    public String getDate_id() {
        return date_id;
    }

    public void setDate_id(String date_id) {
        this.date_id = date_id;
    }

    public String getProvided_id() {
        return provided_id;
    }

    public void setProvided_id(String provided_id) {
        this.provided_id = provided_id;
    }

    public String getExt_id() {
        return ext_id;
    }

    public void setExt_id(String ext_id) {
        this.ext_id = ext_id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getSource_id() {
        return source_id;
    }

    public void setSource_id(String source_id) {
        this.source_id = source_id;
    }

    public String getPro_category() {
        return pro_category;
    }

    public void setPro_category(String pro_category) {
        this.pro_category = pro_category;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }


    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getMuid() {
        return muid;
    }

    public void setMuid(String muid) {
        this.muid = muid;
    }

    public String getID(){ return id; }

    public void setID(){ this.id = id; }

    public String getId_type() {
        return id_type;
    }

    public void setId_type(String id_type) {
        this.id_type = id_type;
    }


}
