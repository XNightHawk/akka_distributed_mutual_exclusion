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

    ActorRef[] nodes = new ActorRef[7];

    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = system.actorOf(Node.props(i));
    }

    /*

    Il crash non riprende correttamente eg crash 1, request 0, crash 1
    il nodo 1 recupera correttamente le informazioni sull'holder e sulla sua request list
    Il problema e' che 1 non puo' sapere se ha o meno mandato una richiesta a 2
    nel nostro caso una richiesta non e' mai stata mandata.
    Bisogna stare attenti nel caso si decidesse di mandare una request a prescindere dopo un crash perche' potrebbe causare
    una richiesta duplicata

    Un'idea e' di mandare comunque una richiesta alla ripresa dal crash. Un nodo ogni volta che riceve una richiesta controlla se e' gia' in lista. Se non lo e' controlla che non provenga dal proprio holder.
    Se cio' non si verifica la richeista puo' essere aggiunta.
    Questo dovrebbe funzionare. Controllare che questa strategia non generi problemi

    //simulates the fact that 1 crashed after forwarding request and comes back before token has even reached him

    // pass token to 3
    request 3

    //node 1 crashes
    crash 1
    request 0

    //token is being used for a long time on the other side of the topology
    force_crash 3

    //node 1 forwarded request before crashing
    rft 1 2

    //node 1 comes back
    crash 1

    NOW IF BEHAVIOR IS CORRECT ONE SHOULD FORWARD REQUEST AND 2 SHOULD SEE A DUBLICATE WHICH IT DROPS

     */


    // neighbor initialization
    nodes[0].tell(new NeighborInit(1, nodes[1]), null);
    nodes[1].tell(new NeighborInit(0, nodes[0]), null);
    nodes[1].tell(new NeighborInit(2, nodes[2]), null);
    nodes[2].tell(new NeighborInit(1, nodes[1]), null);
    nodes[2].tell(new NeighborInit(3, nodes[3]), null);
    nodes[2].tell(new NeighborInit(4, nodes[4]), null);
    nodes[3].tell(new NeighborInit(2, nodes[2]), null);
    nodes[4].tell(new NeighborInit(2, nodes[2]), null);
    nodes[4].tell(new NeighborInit(6, nodes[6]), null);
    nodes[4].tell(new NeighborInit(5, nodes[5]), null);
    nodes[5].tell(new NeighborInit(4, nodes[4]), null);
    nodes[6].tell(new NeighborInit(4, nodes[4]), null);

    nodes[2].tell(new TokenInject(), null);

    //system.scheduler().scheduleOnce(
    //        Duration.create(0, TimeUnit.SECONDS),
    //        nodes[2], new TokenInject(), system.dispatcher(), null);

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
