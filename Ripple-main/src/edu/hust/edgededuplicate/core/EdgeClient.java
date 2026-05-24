package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.utill.GlobalLogger;

import java.util.logging.Logger;

public class EdgeClient {
    private static final Logger logger = GlobalLogger.getLogger();
    private int clientID;
    private int serverIDCovered;//client所处server的ID
    private long clientRequestData;//请求数据

    public int getServerIDCovered() {
        return serverIDCovered;
    }

    public long getClientRequestData() {
        return clientRequestData;
    }

    public EdgeClient(int clientID, int serverIDCovered, long data) {
        this.clientID = clientID;
        this.serverIDCovered = serverIDCovered;
        this.clientRequestData = data;
    }

//    public RequestMessage clientSendRequestMessage(){
//        logger.info("[Client " + this.clientID +"] Send Request Message to " + this.serverIDCovered);
//        return new RequestMessage(this.clientRequestData, this.clientID, this.serverIDCovered);
//    }

}
