package db.mySQL.dto;

public class CompareResult {
    private int result;
    private ComparingData data;

    public ComparingData getData() {
        return data;
    }

    public void setData(ComparingData data) {
        this.data = data;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }


}
