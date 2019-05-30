package it.distr;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import it.distr.utils.Configuration;
import it.distr.utils.Logger;
import it.distr.utils.Tuple;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;


@SuppressWarnings("WeakerAccess")
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

  Random generator = new Random();


  private MessageBroker broker;
  private Logger logger;

  public Node(int id) {
    this.myId = id;
    logger = new Logger(myId);

    holder = -1;
    inside_cs = false;

    //Create a message broker in initialization mode
    broker = new MessageBroker();
  }

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  public static class NeighborInit implements Serializable {
    public final int nodeId;
    public final ActorRef nodeRef;

    public NeighborInit(int nodeId, ActorRef nodeRef) { this.nodeId = nodeId; this.nodeRef = nodeRef; }

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

    if(sender.equals(getSelf())) return myId; // my id is not in the neighbor list, handled here

    for(Map.Entry<Integer, ActorRef> entry : neighbors.entrySet()) {
      if(entry.getValue().equals(sender)) {
        return entry.getKey();
      }
    }


    logger.logError("getIdBySender() cannot find ID!");
    throw new Error("Cannot find id for neighbor " + sender.toString());
  }

  private ActorRef getNeighborRef(int neighborId) {
    return neighbors.get(neighborId);
  }

  private ActorRef getHolderRef() {
    return getNeighborRef(holder);
  }

  private void tellWrapper(ActorRef dest, Object message, ActorRef sender) {
    tellWrapper(dest, message, sender, false);
  }

  private void tellWrapper(ActorRef dest, Object message, ActorRef sender, boolean bypass_delay) {
    if(Configuration.DEBUG) {
      try {
        int sleepMillis = generator.nextInt(Configuration.MAX_WAIT);
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    dest.tell(message, sender);
  }

  public void onTokenInject(TokenInject message, ActorRef sender) {
    holder = myId;

    for(ActorRef currentNeighbor : neighbors.values()) {
      tellWrapper(currentNeighbor, new Init(myId), getSelf(), true);
    }

    broker.changeMode(BrokerMode.NORMAL_MODE);

    if(Configuration.DEBUG) {
      logger.logInfo("Token injected!" + "   " + "holder: " + holder + "   " + "neighbors: " + (neighbors.keySet()).toString());
    }

  }

  public void onNeighborInit(NeighborInit message, ActorRef sender) {
    neighbors.put(message.nodeId, message.nodeRef);
  }

  public void onInit(Init message, ActorRef sender) {
    holder = getIdBySender(sender);

    for(ActorRef currentNeighbor : neighbors.values()) {
      if(!currentNeighbor.equals(sender)) {
        tellWrapper(currentNeighbor, new Init(myId), getSelf(), true);
      }
    }

    broker.changeMode(BrokerMode.NORMAL_MODE);

    if(Configuration.DEBUG) {
      logger.logInfo("Init received!" + "   " + "holder: " + holder + "   " + "neighbors: " + (neighbors.keySet()).toString());
    }
  }

  public void onRequest(Request message, ActorRef sender) {
    if(Configuration.DEBUG) {
      logger.logInfo("onRequest() - request received from: " + getIdBySender(sender));
    }

    ActorRef requester = sender;

    int requesterId = getIdBySender(sender);

    //Otherwise it means we had duplicate request which is bad
    if(request_list.contains(requesterId)) {
      logger.logWarning("onRequest() - duplicated request received from " + requesterId + " (did he crash?) request was dropped");
      return;
    }

    //If request comes from my holder he probably crashed (it sent the request during onCrashExit)
    if(requesterId == holder && holder != myId) {
      logger.logWarning("onRequest() - request received from holder " + requesterId + " (did he crash?) request was dropped");
      return;
    }

    //if(!(!request_list.contains(requesterId))) logger.logError("assertion - duplicated request");
    assert(!request_list.contains(requesterId));

    //Must forward the request to the holder or satisfy it if I am the holder and not in cs
    if(request_list.isEmpty()) {
      if(inside_cs) {
        request_list.add(requesterId); //Put in the list, as soon as I exit I will grant it

      } else if(holder == myId) {

        //If the request message is sent by myself, add me to the request_list
        //so that giveAccessToFirst() will be called (after Privilege message received) and i will enter the CS
        if(requesterId == myId) {
          request_list.add(requesterId);
        }
        //Grant the privilege and say I don't require token back because I have no other pending requests
        tellWrapper(requester, new Privilege(false), getSelf());

        if(Configuration.DEBUG) {
          logger.logInfo("onRequest() - privilege sent to: " + requesterId);
        }

        holder = requesterId;
      } else {
        //Request token on behalf of requester to my holder
        request_list.add(requesterId);

        ActorRef holderRef = getHolderRef();
        if(!(holderRef != null)) logger.logError("assertion - no ActorRef found for my holderId");
        assert(holderRef != null);

        tellWrapper(holderRef, new Request(), getSelf());

        if(Configuration.DEBUG) {
          logger.logInfo("onRequest() - request forwarded to: " + getIdBySender(getHolderRef()));
        }
      }
    } else {
      //Put in the list, but I already requested privilege before, so don't send a duplicate request
      request_list.add(requesterId);
    }

    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
    }
  }

  private void giveAccessToFirst() {
    //Fetches the first element
    if(!(!request_list.isEmpty())) logger.logError("assertion - giveAccessToFirst() but request_list is empty");
    assert(!request_list.isEmpty());
    int oldLength = request_list.size();
    int first_requester = request_list.poll();
    if(!(request_list.size() == oldLength - 1)) logger.logError("assertion - giveAccessToFirst() but not updated request_list");
    assert(request_list.size() == oldLength - 1);

    if(first_requester == myId) {
      getContext().getSystem().scheduler().scheduleOnce(Duration.create(1000, TimeUnit.MILLISECONDS), getSelf(), new ExitCS(), getContext().system().dispatcher(), getSelf());
      inside_cs = true;
      logger.logInfo("ENTER CS");
    } else {
      boolean need_privilege_back = !request_list.isEmpty();
      ActorRef first_requester_ref = getNeighborRef(first_requester);
      //Send a privilege to the node to serve and ask it back if I have other requests.
      tellWrapper(first_requester_ref, new Privilege(need_privilege_back), getSelf());

      if(Configuration.DEBUG) {
        logger.logInfo("giveAccessToFirst() - privilege sent to: " + first_requester);
      }

      holder = first_requester;
    }
  }

  public void onPrivilege(Privilege message, ActorRef sender) {

    int senderId = getIdBySender(sender);
    //After a crash it is not meaningful to recover the fact whether we wanted or not to access the critical section before the crash, because that decision
    //must be taken by the logic of the restarted application and not by the old one.
    //For this reason we may receive a privilege and have an empty request list (Eg scenario_4.txt)
    if(request_list.isEmpty()) {
      logger.logWarning("onPrivilege - privilege received from " + senderId + " but request list is empty. Did we crash?");
    }

    if(Configuration.DEBUG) {
      logger.logInfo("onPrivilege() - privilege received from: " + getIdBySender(sender));
    }

    holder = myId;

    //If the token is required by the previous owner (to compete other requests) add the sender to the request_list
    boolean requiresTokenBack = message.requiresTokenBack;
    if(requiresTokenBack) {
      request_list.add(senderId);
    }

    //Check whether we have requests to serve. Due to crash dynamics it may not happen if our request was the only one at the moment of crash.
    if(!request_list.isEmpty()) {
      giveAccessToFirst();
    }

    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
    }
  }

  public void onExitCS(ExitCS message, ActorRef sender) {

    inside_cs = false;
    logger.logInfo("EXIT CS");
    //If someone needs the token give it to them, otherwise sit idle
    if(!request_list.isEmpty()) {
      giveAccessToFirst();
    }
    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
    }
  }

  public void onCrashBegin(CrashBegin message, ActorRef sender) {

    if(Configuration.DEBUG) {
      logger.logInfo("onCrashBegin() - entering crash mode");
    }

    if(inside_cs) {
      //Ignore crash request if in CS
      logger.logWarning("Node is in CS. Crash request message ignored.");
    } else {
      holder = -1;
      request_list.clear();
      recovery_info.clear();

      broker.changeMode(BrokerMode.SELECTIVE_RECOVERY_MODE);
    }
    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
    }
  }

  public void onCrashEnd(CrashEnd message, ActorRef sender) {

    if(Configuration.DEBUG) {
      logger.logInfo("onCrashEnd() - starting recovery operations");
    }

    for(ActorRef neighbor : neighbors.values()) {

      //Ask recovery info from everyone
      //Broker is already allowing recovery info response
      tellWrapper(neighbor, new RecoveryInfoRequest(), getSelf());
    }
    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
    }
  }

  public void onRecoveryInfoRequest(RecoveryInfoRequest message, ActorRef sender) {

    if(Configuration.DEBUG) {
      logger.logInfo("onRecoveryInfoRequest() - sending recovery information");
    }

    //#Tells my holder and whether I have some request, if my holder is not the requesting node, the second boolean field is useless.
    tellWrapper(sender, new RecoveryInfoResponse(holder, !request_list.isEmpty()), getSelf());
    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
    }
  }

  private void decideHolder() {

    //Ensures we received the recovery information from all neighbors
    if(!(recovery_info.size() == neighbors.size())) logger.logError("assertion - recovery messages not received by anybody");
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

    if(Configuration.DEBUG) {
      logger.logInfo("onRecoveryInfoResponse() - received recovery information from: " + getIdBySender(sender));
    }

    int senderId = getIdBySender(sender);
    Tuple<Integer, Boolean> recoveryData = new Tuple<>(message.holderId, message.requestListNotEmpty);
    recovery_info.put(senderId, recoveryData);

    //ask broker to queue for later all messages coming from sender. They will be unlocked and processed when recovery operations are over
    broker.removeFromBlacklist(sender);

	//I have a response from every neighbor
    if(recovery_info.size() == neighbors.size()) {

      if(Configuration.DEBUG) {
        logger.logInfo("onRecoveryInfoResponse() - all recovery info received");
      }

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

      //I could have crashed before receiving the request from a node that wants to access, in that case my request list
      //will not be empty but i didn't forward the request yet, do it now
      if((holder != myId) && (!request_list.isEmpty())) {
        ActorRef holderRef = getHolderRef();
        tellWrapper(holderRef, new Request(), getSelf());

        if(Configuration.DEBUG) {
          logger.logInfo("onRequest() - request forwarded to: " + getIdBySender(getHolderRef()));
        }
      }

      //Ask broker to resume normally. Before resuming 'normally', the broker will process each pending message in his queue
      broker.changeMode(BrokerMode.NORMAL_MODE);

      //Check if I am the holder and have messages to send
      if((holder == myId) && (!request_list.isEmpty())) {
        giveAccessToFirst();
      }

    }
    if(Configuration.DEBUG) {
      logger.logNodeState(holder, request_list, inside_cs);
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
      if(!(!((currentMode == BrokerMode.PREINIT_MODE) && (mode == BrokerMode.SELECTIVE_RECOVERY_MODE)))) logger.logError("assertion - changeMode() trying to change mode uncorrectly");
      assert(!((currentMode == BrokerMode.PREINIT_MODE) && (mode == BrokerMode.SELECTIVE_RECOVERY_MODE)));

      currentMode = mode;
      if (currentMode == BrokerMode.NORMAL_MODE) {
        logger.logInfo(" changeMode() - begin change mode to Normal");
        //All blacklist requests from previous crashed must have been already handled
        if(!(recoveryBlacklist.isEmpty())) logger.logError("assertion - changeMode() recoveryBlacklist not empty");
        assert(recoveryBlacklist.isEmpty());
        //For the invariant no packet in the queue may make the state change, so it is safe to dispatch them all in batch
        dispatchAllWaitingMessages();
        logger.logInfo(" changeMode() - mode changed to Normal");
      } else if (currentMode == BrokerMode.PREINIT_MODE) {
        //Broker must be created in this mode and never return to it
        if(!(false)) logger.logError("assertion - changeMode()");
        assert(false);
      } else if (currentMode == BrokerMode.SELECTIVE_RECOVERY_MODE) {
        //No messages must be there while switching to selective recovery mode
        if(!(messageQueue.isEmpty())) logger.logError("assertion - changeMode() messageQueue not empty");
        assert(messageQueue.isEmpty());
        //All blacklist requests from previous crashed must have been already handled
        if(!(recoveryBlacklist.isEmpty())) logger.logError("assertion - changeMode() recoveryBlacklist not empty");
        assert(recoveryBlacklist.isEmpty());
        recoveryBlacklist.clear();
        for(ActorRef neighborRef : Node.this.neighbors.values()) {
          recoveryBlacklist.add(neighborRef);
        }
        logger.logInfo(" changeMode() - mode changed to Selective Recovery");
      } else {
        logger.logError("Unknown BrokerMode " + currentMode.toString());
        throw new Error("Unknown BrokerMode " + currentMode.toString());
      }

    }

    public void removeFromBlacklist(ActorRef node) {
      if(!(recoveryBlacklist.contains(node))) logger.logError("assertion - removeFromBlacklist() trying to remove somebody that is not there");
      assert(recoveryBlacklist.contains(node));

      recoveryBlacklist.remove(node);
    }

    private void dispatchAllWaitingMessages() {
      while(!messageQueue.isEmpty()) {
        //This method should be called only in normal mode
        if(!(currentMode == BrokerMode.NORMAL_MODE)) logger.logError("assertion - dispatchAllWaitingMessages() called not in normal mode");
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
        logger.logError("Exception");
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
        } else if(message.getClass().equals(TokenInject.class)) {
          dispatchMessage(messageData);
        } else if(message.getClass().equals(Init.class)) {
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
        } else if(message.getClass().equals(CrashEnd.class)) {

          dispatchMessage(messageData);
        //All other packets from non blacklisted nodes are remembered for later
        } else {
          messageQueue.add(messageData);
        }

      } else {
        logger.logError("Exception");
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
