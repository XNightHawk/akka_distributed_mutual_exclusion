package it.distr;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import it.distr.utils.Logger;
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

  private MessageBroker broker;
  private Logger logger;

  public Node(int id) {
    this.myId = id;
    logger = new Logger(myId);

    //TODO: neighbor initialization via message
    holder = -1;
    inside_cs = false;

    //Create a message broker in initialization mode
    broker = new MessageBroker();
  }

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  public static class NeighborInit implements Serializable {

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

    broker.changeMode(BrokerMode.NORMAL_MODE);
  }

  public void onNeighborInit(NeighborInit message, ActorRef sender) {
    //TODO: implement
  }

  public void onInit(Init message, ActorRef sender) {
    holder = getIdBySender(sender);

    for(ActorRef currentNeighbor : neighbors.values()) {
      if(!currentNeighbor.equals(sender)) {
        currentNeighbor.tell(new Init(myId), getSelf());
      }
    }

    broker.changeMode(BrokerMode.NORMAL_MODE);
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
      //Ignore crash request if in CS
      logger.logWarning("Node is in CS. Crash request message ignored.");
    } else {
      holder = -1;
      request_list.clear();
      recovery_info.clear();

      broker.changeMode(BrokerMode.SELECTIVE_RECOVERY_MODE);
    }
  }

  public void onCrashEnd(CrashEnd message, ActorRef sender) {
    for(ActorRef neighbor : neighbors.values()) {

      //Ask recovery info from everyone
      //Broker is already allowing recovery info response
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

    //ask broker to queue for later all messages coming from sender. They will be unlocked and processed when recovery operations are over
    broker.removeFromBlacklist(sender);

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

      //Ask broker to resume normally. Before resuming 'normally', the broker will process each pending message in his queue
      broker.changeMode(BrokerMode.NORMAL_MODE);

      //Posso essere holder e avere delle richieste adesso se sono crashato mentre il token era su un link diretto verso di me
      //In questo caso potrei avere messaggi da spedire
      if((holder == myId) && (!request_list.isEmpty())) {
        giveAccessToFirst();
      }
    }
  }

  private enum BrokerMode {
    PREINIT_MODE,
    NORMAL_MODE,
    SELECTIVE_RECOVERY_MODE;

  }

  private class MessageBroker {

    //INVARIANT: in the message queue there are never packets that may cause a change in state of the queue
    //In a transition from init to normal in the queue there may be only packets from other nodes which make the node
    //stay in normal
    //In a transition from recovery mode only packets from other nodes may be in the queue and those make the node stay
    //in normal

    private BrokerMode currentMode = BrokerMode.PREINIT_MODE;
    private Queue<Tuple<Object, ActorRef>> messageQueue = new ArrayDeque<>();
    private Set<ActorRef> recoveryBlacklist = new HashSet<>();

    public void changeMode(BrokerMode mode) {

      //Cannot go directly to recovery without passing from normal mode
      assert(!((currentMode == BrokerMode.PREINIT_MODE) && (mode == BrokerMode.SELECTIVE_RECOVERY_MODE)));

      currentMode = mode;
      if (currentMode == BrokerMode.NORMAL_MODE) {
        //All blacklist requests from previous crashed must have been already handled
        assert(recoveryBlacklist.isEmpty());
        //For the invariant no packet in the queue may make the state change, so it is safe to dispatch them all in batch
        dispatchAllWaitingMessages();
      } else if (currentMode == BrokerMode.PREINIT_MODE) {
        //Broker must be created in this mode and never return to it
        assert(false);
      } else if (currentMode == BrokerMode.SELECTIVE_RECOVERY_MODE) {
        //No messages must be there while switching to slective recovery mode
        assert(messageQueue.isEmpty());
        //All blacklist requests from previous crashed must have been already handled
        assert(recoveryBlacklist.isEmpty());
        recoveryBlacklist.clear();
        for(ActorRef neighborRef : Node.this.neighbors.values()) {
          recoveryBlacklist.add(neighborRef);
        }
      } else {
        throw new Error("Unknown BrokerMode " + currentMode.toString());
      }

    }

    public void removeFromBlacklist(ActorRef node) {
      assert(recoveryBlacklist.contains(node));

      recoveryBlacklist.remove(node);
    }

    private void dispatchAllWaitingMessages() {
      while(!messageQueue.isEmpty()) {

        //This method should be called only in normal mode
        assert(currentMode == BrokerMode.NORMAL_MODE);
        Tuple<Object, ActorRef> messageData = messageQueue.poll();
        dispatchMessage(messageData);
      }
    }

    private void dispatchMessage(Tuple<Object, ActorRef> messageData) {
      Object message = messageData.first();
      ActorRef sender = messageData.last();

      //Find node method to which dispatch the message and dispatch it
      if(message.getClass().equals(Init.class)) {
        Node.this.onInit((Init) message, sender);
      } else if(message.getClass().equals(TokenInject.class)) {
        Node.this.onTokenInject((TokenInject) message, sender);
      } else if(message.getClass().equals(Request.class)) {
        Node.this.onRequest((Request) message, sender);
      } else if(message.getClass().equals(Privilege.class)) {
        Node.this.onPrivilege((Privilege) message, sender);
      } else if(message.getClass().equals(RecoveryInfoRequest.class)) {
        Node.this.onRecoveryInfoRequest((RecoveryInfoRequest) message, sender);
      } else if(message.getClass().equals(RecoveryInfoResponse.class)) {
        Node.this.onRecoveryInfoResponse((RecoveryInfoResponse) message, sender);
      } else if(message.getClass().equals(ExitCS.class)) {
        Node.this.onExitCS((ExitCS) message, sender);
      } else if(message.getClass().equals(CrashBegin.class)) {
        Node.this.onCrashBegin((CrashBegin) message, sender);
      } else if(message.getClass().equals(CrashEnd.class)) {
        Node.this.onCrashEnd((CrashEnd) message, sender);
      } else if(message.getClass().equals(NeighborInit.class)) {
        Node.this.onNeighborInit((NeighborInit) message, sender);
      } else {
        throw new Error("Unregistered message class " + message.getClass().toString());
      }
    }

    public void messageArrived(Object message, ActorRef sender) {

      Tuple<Object, ActorRef> messageData = new Tuple<>(message, sender);

      if(currentMode == BrokerMode.NORMAL_MODE) {
        dispatchMessage(messageData);
      } else if(currentMode == BrokerMode.PREINIT_MODE) {
        if(message.getClass().equals(NeighborInit.class)) {
          dispatchMessage(messageData);
        } else {
          messageQueue.add(messageData);
        }
      } else if(currentMode == BrokerMode.SELECTIVE_RECOVERY_MODE) {
        //Messages with no sender coming from outside are always handled
        if(recoveryBlacklist.contains(sender)) {

          //We drop anything apart from RecoveryResponse messages
          if(message.getClass().equals(RecoveryInfoResponse.class)) {
            dispatchMessage(messageData);
          }
        //Packets from outside Eg CrashEnd are processed immediately
        } else if(sender == null) {
          dispatchMessage(messageData);
        //All other packets from non blacklisted nodes are remembered for later
        } else {
          messageQueue.add(messageData);
        }

      } else {
        throw new Error("Unknown BrokerMode " + currentMode.toString());
      }
    }

  }

  public void brokerDispatcher(Object message) {
    ActorRef sender = getSender();

    //Notify the broker when any message arrives
    broker.messageArrived(message, sender);
  }

  @Override
  public Receive createReceive() {
    //insert the broker as a receiver for all messages
    return receiveBuilder()
      .matchAny(this::brokerDispatcher)
      .build();
  }
}
