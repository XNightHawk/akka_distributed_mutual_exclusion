package it.distr.utils;

import akka.actor.ActorRef;
import it.distr.Node;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandParser {

    public static final Pattern COMMAND_REQUEST = Pattern.compile("^request\\s+(\\d+(?:\\s*,\\s*\\d+)*)+$");
    public static final Pattern COMMAND_CRASH = Pattern.compile("^crash\\s+(\\d+)$");
    public static final Pattern COMMAND_EXIT = Pattern.compile("^exit$");
    public static final Pattern COMMAND_HELP = Pattern.compile("^help$");
    public static final Pattern COMMAND_RFT = Pattern.compile("^rft\\s+(\\d+)\\s+(\\d+)$"); //request from -> to
    public static final Pattern COMMAND_FORCE_CRASH = Pattern.compile("^force_crash\\s+(\\d+)$");
    public static final Pattern COMMAND_FORCE_RECOVERY = Pattern.compile("^force_recovery\\s+(\\d+)$");

    private ActorRef[] nodes;
    private int crashedNode = -1;
    private Scanner keyboard;


    public CommandParser(ActorRef[] n) {
        keyboard = new Scanner(System.in);
        nodes = n;
    }

    /*
    Return true if parsing is over, false if continues
     */
    public boolean parse() {
        System.out.print("# ");
        String input;

        input = keyboard.nextLine();
        input = input.trim();


        Matcher match_request = COMMAND_REQUEST.matcher(input);
        Matcher match_crash = COMMAND_CRASH.matcher(input);
        Matcher match_exit = COMMAND_EXIT.matcher(input);
        Matcher match_help = COMMAND_HELP.matcher(input);
        Matcher match_rft = COMMAND_RFT.matcher(input);
        Matcher match_force_crash = COMMAND_FORCE_CRASH.matcher(input);
        Matcher match_force_recovery = COMMAND_FORCE_RECOVERY.matcher(input);

        if(match_exit.matches()) {
            return true;
        } else if(match_request.matches()) {
            execRequest(match_request.group(1));
        } else if(match_crash.matches()) {
            execCrash(match_crash.group(1));
        } else if(match_help.matches()) {
            printUsage();
        } else if(match_rft.matches()) {
            execRft(match_rft.group(1), match_rft.group(2));
        } else if(match_force_crash.matches()) {
            execForceCrash(match_force_crash.group(1));
        } else if(match_force_recovery.matches()) {
            execForceRecovery(match_force_recovery.group(1));
        } else if(input.equals("")){
            return false;
        } else {
            printUsage();
        }

        /*
        if(m.matches()) {
            System.out.println("Groups: " + m.groupCount());
            for (int i = 0; i <= m.groupCount(); i++) {
                System.out.println(i + "     ----->     " + m.group(i));
            }
        } else {
            System.out.println("false");
        }
        */

        return false;
    }

    private void execRequest(String a) {
        //parse arguments
        String args[] = a.split(",");
        int nodeIds[] = new int[args.length];
        for(int i = 0; i < args.length; i++) {
            args[i] = args[i].trim();
            nodeIds[i] = Integer.parseInt(args[i]);
            if(nodeIds[i] < 0 || nodeIds[i] >= nodes.length) {
                System.out.println("Node IDs not valid!");
                return;
            }
        }

        for(int i = 0; i < args.length; i++) {
            System.out.println("Sending request message to node " + nodeIds[i]);
            nodes[nodeIds[i]].tell(new Node.Request(), nodes[nodeIds[i]]);
        }
    }

    private void execCrash(String a) {
        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.length) {
            System.out.println("Node ID not valid!");
            return;
        }

        if(crashedNode == -1) {
            //crash node
            System.out.println("Crashing node " + nodeId);
            nodes[nodeId].tell(new Node.CrashBegin(), null);
            crashedNode = nodeId;
        } else if (crashedNode == nodeId) {
            //bring back node
            System.out.println("Reviving node " + nodeId);
            nodes[nodeId].tell(new Node.CrashEnd(), null);
            crashedNode = -1;
        } else {
            //a node is already crashed, must before recover the other
            System.out.println("Node " + crashedNode + " is already crashed! Cannot crash more!");
        }
    }

    private void execRft(String f, String t) {
        int from = Integer.parseInt(f);
        int to = Integer.parseInt(t);

        if(from < 0 || from >= nodes.length) {
            System.out.println("Node ID not valid!");
            return;
        }

        if(to < 0 || to >= nodes.length) {
            System.out.println("Node ID not valid!");
            return;
        }

        System.out.println("Injecting request from " + from + " to " + to);
        nodes[to].tell(new Node.Request(), nodes[from]);
    }

    private void execForceCrash(String a) {
        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.length) {
            System.out.println("Node ID not valid!");
            return;
        }

        System.out.println("Forcing crash of node " + nodeId);
        nodes[nodeId].tell(new Node.CrashBegin(), null);
    }

    private void execForceRecovery(String a) {
        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.length) {
            System.out.println("Node ID not valid!");
            return;
        }

        System.out.println("Forcing recovery of node " + nodeId);
        nodes[nodeId].tell(new Node.CrashEnd(), null);
    }

    private void printUsage() {
        System.out.println("Commands:");
        System.out.println("request comma_separated_list_of_nodes_id            -- ask node to enter CS");
        System.out.println("crash node_id                                       -- crashes/recovers node");
        System.out.println("help                                                -- shows this prompt");
        System.out.println("exit                                                -- terminates system");
        System.out.println(Configuration.ANSI_YELLOW);
        System.out.println("Debug Commands -- Will probably crash the software unless you know what you are doing!");
        System.out.println("rft node_from node_to                               -- generate request from node to node");
        System.out.println("force_crash                                         -- sends crashBegin signal (even with other crashes present)");
        System.out.println("force_recovery                                      -- sends crashEnd signal");
        System.out.println(Configuration.ANSI_RESET);
    }
}