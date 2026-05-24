package edu.hust.edgededuplicate.core;

public class UpdateMessage {
    public String type; //更新类型
    public int serverID; //信息来源
    public int transHop; //传播次数
    public long dataHash;//数据指纹

    /**
     * An Update Message to notify other servers in limit hops .
     * @param type  "insert" or "delete"
     * @param serverID   message source severID
     * @param dataHash   data hash
     * @param transHop   The shortest number of hops from source to destination node
     */
    UpdateMessage(String type, int serverID, long dataHash, int transHop) {
        this.type=type;
        this.serverID = serverID;
        this.dataHash = dataHash;
        this.transHop = transHop;
    }

}
