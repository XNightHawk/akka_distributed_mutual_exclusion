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

        if(match_exit.matches()) {
            return true;
        } else if(match_request.matches()) {
            execRequest(match_request.group(1));
        } else if(match_crash.matches()) {
            execCrash(match_crash.group(1));
        } else if(match_help.matches()){
            printUsage();
        } else {
            return false;
        }
        //TODO add wrong usage (when string != "" but still does not match anything)
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

    private void printUsage() {
        System.out.println("Commands:");
        System.out.println("request comma_separated_list_of_nodes_id");
        System.out.println("crash node_id");
        System.out.println("exit");
    }


    public static void main(String args[]) {
        CommandParser cp = new CommandParser(null);

        boolean over = false;

        while(!over) {
            over = cp.parse();
        }

    }
}