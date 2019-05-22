package it.distr.utils;

import java.util.Queue;

public class Logger {

    private final int nodeId;

    public Logger(int nodeId) {
        this.nodeId = nodeId;
    }

    public void logError(String message) {
        System.err.println("(" + nodeId + ") " + message);
    }

    public void logWarning(String message) {
        System.out.println(Configuration.ANSI_YELLOW + "(" + nodeId + ") " + message + Configuration.ANSI_RESET);
    }

    public void logInfo(String message) {
        System.out.println("(" + nodeId + ") " + message);
    }

    public void logNodeState(int holder, Queue<Integer> request_list, boolean inside_cs) {
        System.out.println("(" + nodeId + ")" + "   token: " + (nodeId == holder ? "Y" : "N") + "   holder: " + holder + "   requests: " + request_list.toString() + "   CS: " + (inside_cs ? "Y" : "N") + "   ");
    }

}
