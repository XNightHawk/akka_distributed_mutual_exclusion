package it.distr.utils;

public class Logger {

    private final int nodeId;

    public Logger(int nodeId) {
        this.nodeId = nodeId;
    }

    public void logError(String message) {
        System.out.println("(" + nodeId + ")" + message);
    }

    public void logWarning(String message) {
        System.out.println("(" + nodeId + ")" + message);
    }

    public void logInfo(String message) {
        System.out.println("(" + nodeId + ")" + message);
    }

}
