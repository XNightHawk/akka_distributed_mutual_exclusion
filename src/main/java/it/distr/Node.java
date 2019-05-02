package it.distr;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


public class Node extends AbstractActor {
  private int id;
  private List<ActorRef> peers = new ArrayList<>();   // list of peer banks

  public Node(int id) {
    this.id = id;
  }

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }


  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> group;   // an array of group members
    public JoinGroupMsg(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
    }
  }


  private void onJoinGroupMsg(JoinGroupMsg msg) {
    for (ActorRef b: msg.group) {
      if (!b.equals(getSelf())) { // copy all bank refs except for self
        this.peers.add(b);
      }
    }
    System.out.println("" + id + ": starting with " + 
        msg.group.size() + " peer(s)");
    getSelf().tell(new NextTransfer(), getSelf());  // schedule 1st transaction 
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
      .build();
  }
}
