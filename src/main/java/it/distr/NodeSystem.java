package it.distr;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import it.distr.Node.JoinGroupMsg;


public class NodeSystem {
  final static int N_BRANCHES = 10;

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("banksystem");

    // Create bank branches and put them to a list
    List<ActorRef> group = new ArrayList<>();
    for (int i=0; i<N_BRANCHES; i++) {
      group.add(system.actorOf(Node.props(i), "bank" + i));
    }

    // Send join messages to the banks to inform them of the whole group
    JoinGroupMsg start = new JoinGroupMsg(group);
    for (ActorRef peer: group) {
      peer.tell(start, null);
    }

    // Schedule snapshots initiated by bank 0, once every second
    system.scheduler().schedule(
        Duration.create(1, TimeUnit.SECONDS), 
        Duration.create(1, TimeUnit.SECONDS),
        group.get(0), new StartSnapshot(), system.dispatcher(), null);

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ioe) {}

    system.terminate();
  }
}
