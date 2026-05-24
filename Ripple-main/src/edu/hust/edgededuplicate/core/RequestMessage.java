package edu.hust.edgededuplicate.core;

import edu.hust.edgededuplicate.utill.GlobalLogger;

import java.util.logging.Logger;

public class RequestMessage {
    private static final Logger logger = GlobalLogger.getLogger();
    public long clientRequestDataHash;
    public int clientID;
    public int serverIDCovered;

    public long calculateHash(long clientRequestData) {
        // TODO Client计算数据的Hash
        clientRequestDataHash = clientRequestData;
        logger.info("Calculate data Hashing " + clientRequestDataHash);
        return clientRequestDataHash;
    }

    public RequestMessage(long clientRequestData, int clientID, int serverIDCovered) {
        this.clientRequestDataHash = calculateHash(clientRequestData);
        this.clientID = clientID;
        this.serverIDCovered = serverIDCovered;
    }
}
