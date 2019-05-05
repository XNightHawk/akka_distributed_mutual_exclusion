package it.distr;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.dispatch.Recover;
import it.distr.utils.Tuple;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Node extends AbstractActor {
  //Node ID
  private int myId;
  //ID of holder node. Equals self in case we hold the token
  private int holder;
  //IDs and references of neighboring nodes
  private Map<Integer, ActorRef> neighbors = new HashMap<>();   // list of peer banks
  //Request queue. Each inserted ID means that node ID requested the token
  private Queue<Integer> request_list = new ArrayDeque<>();
  //Indicates whether node is inside critical section
  private boolean inside_cs;
  //Collects neighbors state (holder id, non empty request queue) during crash restart
  private Map<Integer, Tuple<Integer, Boolean>> recovery_info = new HashMap<>();

  public Node(int id) {
    this.myId = id;
    //TODO: neighbor initialization via message
    holder = -1;
    inside_cs = false;

    //TODO: ensure message broker is in init mode and discards correct messages
  }

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  public static class Init implements Serializable {
    public final int holderId;

    public Init(int holderId) {
      this.holderId = holderId;
    }
  }

  public static class TokenInject implements Serializable {}

  public static class Request implements Serializable {}

  public static class Privilege implements Serializable {
    public final boolean requiresTokenBack;

    public Privilege(boolean requiresTokenBack) {
      this.requiresTokenBack = requiresTokenBack;
    }
  }

  public static class RecoveryInfoRequest implements Serializable {}

  public static class RecoveryInfoResponse implements Serializable {
    public final int holderId;
    public final boolean requestListNotEmpty;

    public RecoveryInfoResponse(int holderId, boolean requestListNotEmpty) {
      this.holderId = holderId;
      this.requestListNotEmpty = requestListNotEmpty;
    }
  }
  public static class ExitCS implements Serializable {}

  public static class CrashBegin implements Serializable {}

  public static class CrashEnd implements Serializable {}

  private int getIdBySender(ActorRef sender) {
    for(Map.Entry<Integer, ActorRef> entry : neighbors.entrySet()) {
      if(entry.getValue().equals(sender)) {
        return entry.getKey();
      }
    }
    throw new Error("Cannot find id for neighbor " + sender.toString());
  }

  private ActorRef getNeighborRef(int neighborId) {
    return neighbors.get(neighborId);
  }

  private ActorRef getHolderRef() {
    return getNeighborRef(holder);
  }

  public void onTokenInject(TokenInject message, ActorRef sender) {
    holder = myId;

    for(ActorRef currentNeighbor : neighbors.values()) {
      currentNeighbor.tell(new Init(myId), getSelf());
    }

    //TODO: exit message broker init mode and work normally
  }


  public void onInit(Init message, ActorRef sender) {
    holder = getIdBySender(sender);

    for(ActorRef currentNeighbor : neighbors.values()) {
      if(!currentNeighbor.equals(sender)) {
        currentNeighbor.tell(new Init(myId), getSelf());
      }
    }

    //TODO: exit message broker init mode and work normally

  }

  public void onRequest(Request message, ActorRef sender) {
    ActorRef requester = sender;
    int requesterId = getIdBySender(sender);

    //Otherwise it means we had duplicate request which is bad
    assert(!request_list.contains(requester);

    request_list.add(requesterId);

    //Must forward the request to the holder or satisfy it if I am the holder and not in cs
    if(request_list.isEmpty()) {

      if(inside_cs) {
        request_list.add(requesterId); //Put in the list, as soon as I exit I will grant it

      } else if(holder == myId) {

        //Grant the privilege and say I don't require token back because I have no other pending requests
        requester.tell(new Privilege(false), getSelf());
        holder = requesterId;
      } else {
        //Request token on behalf of requester to my holder
        request_list.add(requesterId);

        ActorRef holderRef = getHolderRef();
        assert(holderRef != null);

        holderRef.tell(new Request(), getSelf());
      }
    } else {
      //Put in the list, but I already requested privilege before, so don't send a duplicate request
      request_list.add(requesterId);
    }
  }

  private void giveAccessToFirst() {

    //Fetches the first element
    assert(!request_list.isEmpty());
    int oldLength = request_list.size();
    int first_requester = request_list.poll();
    assert(request_list.size() == oldLength - 1);

    if(first_requester == myId) {
      getContext().getSystem().scheduler().scheduleOnce(Duration.create(1, TimeUnit.MILLISECONDS), getSelf(), new ExitCS(), getContext().system().dispatcher(), getSelf());
      inside_cs = true;
    } else {
      boolean need_privilege_back = !request_list.isEmpty();
      ActorRef first_requester_ref = getNeighborRef(first_requester);
      //Send a privilege to the node to serve and ask it back if I have other requests.
      first_requester_ref.tell(new Privilege(need_privilege_back), getSelf());
      holder = first_requester;
    }
  }

  public void onPrivilege(Privilege message, ActorRef sender) {
    assert(!request_list.isEmpty());

    holder = myId;
    giveAccessToFirst();
  }

  public void onExitCS(ExitCS message, ActorRef sender) {
    inside_cs = false;

    //If someone needs the token give it to them, otherwise sit idle
    if(!request_list.isEmpty()) {
      giveAccessToFirst();
    }
  }

  public void onCrashBegin(CrashBegin message, ActorRef sender) {
    if(inside_cs) {
      //TODO: log warning and ignore crash request
    } else {
      holder = -1;
      request_list.clear();
      recovery_info.clear();
      for(ActorRef neighbor : neighbors.values()) {
        //TODO: ask broker to drop anything from neighbors, but remain able to receive an end crash message
      }
    }
  }

  public void onCrashEnd(CrashEnd message, ActorRef sender) {
    for(ActorRef neighbor : neighbors.values()) {
      //Ask recovery info from everyone
      //TODO: ask broker to allow only RECOVERY_INFO_RESPONSE from neighbor
      neighbor.tell(new RecoveryInfoRequest(), getSelf());
    }
  }

  public void onRecoveryInfoRequest(RecoveryInfoRequest message, ActorRef sender) {
    //#Tells my holder and whether I have some request, if my holder is not the requesting node, the second boolean field is useless.
    sender.tell(new RecoveryInfoResponse(holder, !request_list.isEmpty()), getSelf());
  }

  private void decideHolder() {

    //Ensures we received the recovery information from all neighbors
    assert(recovery_info.size() == neighbors.size());

    //Search if there is a neighbor of which we are not the holders
    for(int neighborId : recovery_info.keySet()) {
      Tuple<Integer, Boolean> currentRecoveryInfo = recovery_info.get(neighborId);

      //If we found a neighbor of which we are not holders, then it is our holder
      if(currentRecoveryInfo.first() != myId) {
        holder = neighborId;
        return;
      }
    }

    //If all neighbors have us as holders, then we have the token
    holder = myId;
  }

  public void onRecoveryInfoResponse(RecoveryInfoResponse message, ActorRef sender) {

    int senderId = getIdBySender(sender);
    Tuple<Integer, Boolean> recoveryData = new Tuple<>(message.holderId, message.requestListNotEmpty);
    recovery_info.put(senderId, recoveryData);

    //TODO: ask broker to queue for later all messages coming from sender. They will be unlocked and processed when
    //recovery operations are over

	//I have a response from every neighbor
    if(recovery_info.size() == neighbors.size()) {

      //Sets the correct holder
      decideHolder();

      //Foreach neighbor that has me as holder
      for(int neighborId : recovery_info.keySet()) {
        Tuple<Integer, Boolean> currentRecoveryInfo = recovery_info.get(neighborId);
        if(currentRecoveryInfo.first() == myId) {
          //If the neighbor has a request and I am his holder, then I must have a request on his behalf
          if(currentRecoveryInfo.last() == true) {
            request_list.add(neighborId);
          }
        }
      }

      //TODO: ask broker to resume normally. Before resuming 'normally', the broker will process each pending message in his queue

      //Posso essere holder e avere delle richieste adesso se sono crashato mentre il token era su un link diretto verso di me
      //In questo caso potrei avere messaggi da spedire
      if((holder == myId) && (!request_list.isEmpty())) {
        giveAccessToFirst();
      }
    }
  }


  @Override
  public Receive createReceive() {
    //TODO: insert the broker as a receiver for all messages
    return receiveBuilder()
      .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
      .build();
  }
}
