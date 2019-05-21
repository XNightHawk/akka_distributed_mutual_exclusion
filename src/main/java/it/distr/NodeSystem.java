package it.distr;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.distr.utils.CommandParser;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import it.distr.Node.*;


public class NodeSystem {

  public static void main(String[] args) {

    // Create the actor system
    Config config = ConfigFactory.parseString("akka {\n" +
            "  loglevel = \"DEBUG\"\n" +
            "}");
    //final ActorSystem system = ActorSystem.create("network", ConfigFactory.load(config));
    final ActorSystem system = ActorSystem.create("network");

    ActorRef[] nodes = new ActorRef[5];

    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = system.actorOf(Node.props(i));
    }

    // neighbor initialization
    nodes[0].tell(new NeighborInit(1, nodes[1]), null);
    nodes[1].tell(new NeighborInit(0, nodes[0]), null);
    nodes[1].tell(new NeighborInit(2, nodes[2]), null);
    nodes[2].tell(new NeighborInit(1, nodes[1]), null);
    nodes[2].tell(new NeighborInit(3, nodes[3]), null);
    nodes[2].tell(new NeighborInit(4, nodes[4]), null);
    nodes[3].tell(new NeighborInit(2, nodes[2]), null);
    nodes[4].tell(new NeighborInit(2, nodes[2]), null);

    system.scheduler().scheduleOnce(
            Duration.create(1, TimeUnit.SECONDS),
            nodes[2], new TokenInject(), system.dispatcher(), null);

    CommandParser cp = new CommandParser(nodes);

    boolean over = false;

    while(!over) {
      over = cp.parse();
    }

    /*
    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();

      nodes[0].tell(new Request(), nodes[0]);
      System.out.println(">>>asdsadt <<<");
      System.in.read();
    } 
    catch (IOException ioe) {}
    */

    system.terminate();
  }
}
