package db.mySQL.dto;

public class MySQLData {
    private String id;
    private String keyword;
    private String pid;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String kw_docid) {
        this.pid = kw_docid;
    }
}
