package edu.hust.edgededuplicate.core;

public class NeighborServerInfo {
    public int serverID;
    public int serverDataVolume;
    public int hop;

    public NeighborServerInfo(int serverId, int serverVolume, int distance) {
        serverID = serverId;
        serverDataVolume = serverVolume;
        hop = distance;
    }
}
