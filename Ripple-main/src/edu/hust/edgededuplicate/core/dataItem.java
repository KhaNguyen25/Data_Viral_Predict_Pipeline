package edu.hust.edgededuplicate.core;

public class dataItem {
    // --- Các thuộc tính gốc (Giữ nguyên để không lỗi code cũ) ---
    int dataID;
    int data;
    long dataHash;


    // --- Getter và Setter Gốc (Giữ nguyên) ---
    public int getDataID() {
        return dataID;
    }

    public void setDataID(int dataID) {
        this.dataID = dataID;
    }

    public int getData() {
        return data;
    }

    public void setData(int data) {
        this.data = data;
    }

    public long getDataHash() {
        return dataHash;
    }

    public void setDataHash(long dataHash) {
        this.dataHash = dataHash;
    }


}