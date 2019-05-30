package it.distr.utils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.distr.Node;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandParser {

    // UTILITY COMMANDS
    public static final Pattern COMMAND_SOURCE = Pattern.compile("^source\\s+(\\S+)$");

    // COMMANDS FOR BUILDING THE NETWORK
    public static final Pattern COMMAND_CREATE = Pattern.compile("^create\\s+(\\d+)$");
    public static final Pattern COMMAND_CONNECT = Pattern.compile("^connect\\s+(\\d+)\\s+(\\d+)$");
    public static final Pattern COMMAND_INJECT = Pattern.compile("^inject\\s+(\\d+)$"); //inject token into network

    // COMMANDS FOR TESTING THE NETWORK
    public static final Pattern COMMAND_REQUEST = Pattern.compile("^request\\s+(\\d+(?:\\s*,\\s*\\d+)*)+$");
    public static final Pattern COMMAND_CRASH = Pattern.compile("^crash\\s+(\\d+)$");
    public static final Pattern COMMAND_EXIT = Pattern.compile("^exit$");
    public static final Pattern COMMAND_DELAY = Pattern.compile("^delay\\s+(\\d+)$");
    public static final Pattern COMMAND_HELP = Pattern.compile("^help$");
    public static final Pattern COMMAND_RFT = Pattern.compile("^rft\\s+(\\d+)\\s+(\\d+)$"); //request from -> to
    public static final Pattern COMMAND_FORCE_CRASH = Pattern.compile("^force_crash\\s+(\\d+)$");
    public static final Pattern COMMAND_FORCE_RECOVERY = Pattern.compile("^force_recovery\\s+(\\d+)$");

    private ActorSystem system;
    private ArrayList<ActorRef> nodes;
    private int crashedNode = -1;
    private boolean tokenInjected = false;
    private Scanner inputSource;


    public CommandParser(ActorSystem s) {
        inputSource = new Scanner(System.in);
        nodes = new ArrayList<ActorRef>();
        system = s;
    }

    /*
    Return true if parsing is over, false if continues
     */
    public boolean parse() {
        System.out.print("# ");
        String input = "#";

        if(!inputSource.hasNext()) {
            System.out.println("Sourcing from file over, switching to interactive mode");
            inputSource = new Scanner(System.in);
        }

        while(input.startsWith("#") || input.isEmpty()) {
            input = inputSource.nextLine();
            input = input.trim();
        }


        Matcher match_source = COMMAND_SOURCE.matcher(input);

        Matcher match_create = COMMAND_CREATE.matcher(input);
        Matcher match_connect = COMMAND_CONNECT.matcher(input);
        Matcher match_inject = COMMAND_INJECT.matcher(input);

        Matcher match_request = COMMAND_REQUEST.matcher(input);
        Matcher match_crash = COMMAND_CRASH.matcher(input);
        Matcher match_exit = COMMAND_EXIT.matcher(input);
        Matcher match_delay = COMMAND_DELAY.matcher(input);
        Matcher match_help = COMMAND_HELP.matcher(input);
        Matcher match_rft = COMMAND_RFT.matcher(input);
        Matcher match_force_crash = COMMAND_FORCE_CRASH.matcher(input);
        Matcher match_force_recovery = COMMAND_FORCE_RECOVERY.matcher(input);

        if(match_exit.matches()) {
            return true;
        } if(match_delay.matches()) {
            execDelay(match_delay.group(1));
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
        } else if(match_create.matches()) {
            execCreate(match_create.group(1));
        } else if(match_connect.matches()) {
            execConnect(match_connect.group(1), match_connect.group(2));
        } else if(match_inject.matches()) {
            execInject(match_inject.group(1));
        } else if(match_source.matches()) {
            execSource(match_source.group(1));
        } else if(input.equals("")){
            return false;
        } else {
            printUsage();
        }

        return false;
    }

    private void execRequest(String a) {
        //parse arguments
        String args[] = a.split(",");
        int nodeIds[] = new int[args.length];
        for(int i = 0; i < args.length; i++) {
            args[i] = args[i].trim();
            nodeIds[i] = Integer.parseInt(args[i]);
            if(nodeIds[i] < 0 || nodeIds[i] >= nodes.size()) {
                System.out.println("Node IDs not valid!");
                return;
            }
        }

        for(int i = 0; i < args.length; i++) {
            System.out.println("Sending request message to node " + nodeIds[i]);
            nodes.get(nodeIds[i]).tell(new Node.Request(), nodes.get(nodeIds[i]));
        }
    }

    private void execDelay(String delayMillisStr) {
        //parse arguments
        int delayMillis = Integer.parseInt(delayMillisStr.trim());

        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void execCrash(String a) {
        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        if(crashedNode == -1) {
            //crash node
            System.out.println("Crashing node " + nodeId);
            nodes.get(nodeId).tell(new Node.CrashBegin(), null);
            crashedNode = nodeId;
        } else if (crashedNode == nodeId) {
            //bring back node
            System.out.println("Reviving node " + nodeId);
            nodes.get(nodeId).tell(new Node.CrashEnd(), null);
            crashedNode = -1;
        } else {
            //a node is already crashed, must before recover the other
            System.out.println("Node " + crashedNode + " is already crashed! Cannot crash more!");
        }
    }

    private void execRft(String f, String t) {
        int from = Integer.parseInt(f);
        int to = Integer.parseInt(t);

        if(from < 0 || from >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        if(to < 0 || to >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        System.out.println("Injecting request from " + from + " to " + to);
        nodes.get(to).tell(new Node.Request(), nodes.get(from));
    }

    private void execForceCrash(String a) {
        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        System.out.println("Forcing crash of node " + nodeId);
        nodes.get(nodeId).tell(new Node.CrashBegin(), null);
    }

    private void execForceRecovery(String a) {
        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        System.out.println("Forcing recovery of node " + nodeId);
        nodes.get(nodeId).tell(new Node.CrashEnd(), null);
    }

    private void execCreate(String n) {

        if(tokenInjected) {
            System.out.println("Token already injected, cannot add new nodes!");
            return;
        }

        //parse arguments
        int number = Integer.parseInt(n.trim());

        int alreadyThere = nodes.size() ;

        for (int i = 0; i < number; i++) {
            int index = alreadyThere + i;
            ActorRef t = system.actorOf(Node.props(index));
            nodes.add(t);
            System.out.println("Created node " + index);
        }
    }

    private void execConnect(String a, String b) {

        if(tokenInjected) {
            System.out.println("Token already injected, cannot change neighbors!");
            return;
        }

        //parse arguments
        int alpha = Integer.parseInt(a.trim());
        int beta = Integer.parseInt(b.trim());

        if(alpha < 0 || alpha >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        if(beta < 0 || beta >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        //send neighbor message to both
        nodes.get(alpha).tell(new Node.NeighborInit(beta, nodes.get(beta)), null);
        nodes.get(beta).tell(new Node.NeighborInit(alpha, nodes.get(alpha)), null);

        System.out.println("Nodes " + alpha + " and " + beta + " are now neighbor");
    }

    private void execInject(String a) {

        if(tokenInjected) {
            System.out.println("Token already injected!");
            return;
        }

        //parse arguments
        int nodeId = Integer.parseInt(a.trim());

        if(nodeId < 0 || nodeId >= nodes.size()) {
            System.out.println("Node ID not valid!");
            return;
        }

        nodes.get(nodeId).tell(new Node.TokenInject(), null);
        System.out.println("Injected token to " + nodeId);
        tokenInjected = true;
    }

    private void execSource(String filename) {
        try {
            inputSource = new Scanner(new File(filename));
            System.out.println("Sourcing commands from file " + filename);
        } catch (FileNotFoundException e) {
            System.out.println("File " + filename + " not found!");
        }
    }

    private void printUsage() {
        System.out.println(Configuration.ANSI_CYAN);
        System.out.println("Utility Commands:");
        System.out.println("source filename                                     -- executes commands contained in selected file");
        System.out.println(Configuration.ANSI_PURPLE);
        System.out.println("Setup Commands:");
        System.out.println("create n                                            -- creates n nodes and adds them to the Actor System");
        System.out.println("connect alpha beta                                  -- sets nodes alpha and beta as neighbors");
        System.out.println("inject node_id                                      -- injects token into selected node");
        System.out.println(Configuration.ANSI_GREEN);
        System.out.println("Simulation Commands:");
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