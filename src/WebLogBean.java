import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class WebLogBean implements Writable {
    private boolean valid = true; //data valid
    private String remote_addr;
    private String remote_user;
    private String time_local;
    private String request;
    private String status;
    private String body_bytes_sent;
    private String http_referer;
    private String http_user_agent;
    private String time_detail;
    private String time_til_hour;
    private String http_body;

    //设置WebLogBean进行字段数据封装
    public void setBean(boolean valid, String remote_addr, String remote_user, String time_local, String request,
                        String status, String body_bytes_sent, String http_referer, String http_user_agent, String time_detail, String time_til_hour, String http_body) {
        this.valid = valid;
        this.remote_addr = remote_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
        this.time_detail = time_detail;
        this.time_til_hour = time_til_hour;
        this.http_body = http_body;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return this.time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public String getTime_detail() {
        return this.time_detail;
    }

    public void setTime_detail(String time_detail) {
        this.time_detail = time_detail;
    }

    public String getTime_til_hour() {
        return time_til_hour;
    }

    public void setTime_til_hour(String time_til_hour) {
        this.time_til_hour = time_til_hour;
    }

    public String getHttp_body() {
        return http_body;
    }

    public void setHttp_body(String http_body) {
        this.http_body = http_body;
    }

    public boolean isvalid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
    //重写toString()方法,使用Hive默认分隔符进行分隔，为后期导入Hive表提供便利
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append(" ").append(this.getRemote_addr());
        sb.append(" ").append(this.getRemote_user());
        sb.append(" [").append(this.getTime_local()).append("]");
        sb.append(" \"").append(this.getRequest()).append("\"");
        sb.append(" ").append(this.getStatus());
        sb.append(" ").append(this.getBody_bytes_sent());
        sb.append(" \"").append(this.getHttp_referer()).append("\"");
        sb.append(" \"").append(this.getHttp_user_agent()).append("\"");
        sb.append(" ").append(this.getTime_detail());
        sb.append(" ").append(this.getTime_til_hour());
        sb.append(" ").append(this.getHttp_body());
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.valid);
        out.writeUTF(null == remote_addr ? "" : remote_addr);
        out.writeUTF(null == remote_user ? "" : remote_user);
        out.writeUTF(null == time_local ? "" : time_local);
        out.writeUTF(null == request ? "" : request);
        out.writeUTF(null == status ? "" : status);
        out.writeUTF(null == body_bytes_sent ? "" : body_bytes_sent);
        out.writeUTF(null == http_referer ? "" : http_referer);
        out.writeUTF(null == http_user_agent ? "" : http_user_agent);
        out.writeUTF(null==time_detail?"":time_detail);
        out.writeUTF(null==time_til_hour?"":time_til_hour);
        out.writeUTF(null==http_body?"":http_body);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.valid = in.readBoolean();
        this.remote_addr = in.readUTF();
        this.remote_user = in.readUTF();
        this.time_local = in.readUTF();
        this.request = in.readUTF();
        this.status = in.readUTF();
        this.body_bytes_sent = in.readUTF();
        this.http_referer = in.readUTF();
        this.http_user_agent = in.readUTF();
        this.time_detail = in.readUTF();
        this.time_til_hour = in.readUTF();
        this.http_body = in.readUTF();
    }
}
