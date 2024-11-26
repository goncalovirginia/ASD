package protocols.statemachine;

import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.MembershipChangedNotification;
import protocols.agreement.notifications.NewLeaderNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.PaxosAgreement;
import protocols.statemachine.messages.AddReplicaMessage;
import protocols.statemachine.messages.LeaderOrderMessage;
import protocols.statemachine.messages.ReplicaAddedMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.PrepareRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.app.HashApp;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.statemachine.timers.SendMessageTimer;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * This is NOT fully functional StateMachine implementation.
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between the StateMachine and the APPLICATION
 * You can change the requests/notification between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 *
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class StateMachine extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;
    private List<Host> membership;
    private int nextInstance;
    private Host leader;
    private List<ProposeRequest> pendingOrders;
    private List<Host> pendingRemoves;

    private List<UUID> wtvr;

    private Host timerLeader; 

    //INITIAL TO DELETE IS BEING USED TO TEST FAILURES, leader failures
    //Because the leader can't have a client, otherwise when we kill it 
    //since this was not made for failures, the client wont get a reply and will time out.
    //private boolean initialTODELETE;

    private Map<UUID, Pair<Long, byte[]>> pendingClientOrder;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 1;
        leader = null;

        pendingClientOrder = new HashMap<>();
        wtvr = new LinkedList<>();
        
        //initialTODELETE = false;

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        //registerMessageSerializer(channelId, LeaderElectionMessage.MSG_ID, LeaderElectionMessage.serializer);
        registerMessageSerializer(channelId, LeaderOrderMessage.MSG_ID, LeaderOrderMessage.serializer);
        registerMessageSerializer(channelId, AddReplicaMessage.MSG_ID, AddReplicaMessage.serializer);
        registerMessageSerializer(channelId, ReplicaAddedMessage.MSG_ID, ReplicaAddedMessage.serializer);

        //registerMessageHandler(channelId, LeaderElectionMessage.MSG_ID, this::uponLeaderElection, this::uponMsgFail);
        registerMessageHandler(channelId, LeaderOrderMessage.MSG_ID, this::uponLeaderOrderMessage, this::uponLeaderMsgFail);
        registerMessageHandler(channelId, AddReplicaMessage.MSG_ID, this::uponAddReplicaMessage, this::uponMsgFail);
        registerMessageHandler(channelId, ReplicaAddedMessage.MSG_ID, this::uponReplicaAddedMessage, this::uponMsgFail);


        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);
        registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponCurrentStateReply);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
        subscribeNotification(NewLeaderNotification.NOTIFICATION_ID, this::uponNewLeaderNotification);
        subscribeNotification(MembershipChangedNotification.NOTIFICATION_ID, this::uponMembershipChangeNotification);
    
        registerTimerHandler(SendMessageTimer.TIMER_ID, this::uponSendMessageTimer);
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        pendingOrders = new LinkedList<>();
        pendingRemoves = new LinkedList<>();

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }

        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, initialMembership.indexOf(self), true));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");

            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            membership.add(self);

            Host target = initialMembership.get(0);
            openConnection(target);
            sendMessage(new AddReplicaMessage(self, 0, target), target);
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);

        /* logger.info("Received order request: " + request); */
        if (state == State.JOINING) {
            pendingOrders.add(new ProposeRequest(0, request.getOpId(), request.getOperation()));
        } else if (state == State.ACTIVE) {            
            if (leader == null) {
                //TODELETE
                /* if(nextInstance == 1) {
                    sendMessage(new AddReplicaMessage(membership.get(2), 0, membership.get(2)), membership.get(2));
                    pendingOrders.add(new ProposeRequest(0, request.getOpId(), request.getOperation()));
                    return;
                } */

                //CORRECT --- Bellow
                sendRequest(new PrepareRequest(nextInstance), PaxosAgreement.PROTOCOL_ID);
                pendingOrders.add(new ProposeRequest(0, request.getOpId(), request.getOperation()));
            } else if(self.equals(leader)) {                
                sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                    PaxosAgreement.PROTOCOL_ID); 
            } else {
                timerLeader = leader;
                long tid = setupTimer(new SendMessageTimer(), 1000);
                pendingClientOrder.put(request.getOpId(), Pair.of(tid, request.getOperation()));
                sendMessage(new LeaderOrderMessage(nextInstance, request.getOpId(), request.getOperation()), leader);
            }
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponCurrentStateReply(CurrentStateReply reply, short protoID) {
		logger.info("Received Current State Reply: {}", reply.toString());

        Host newReplica = membership.get(membership.size()-1);
        sendMessage(new ReplicaAddedMessage(reply.getInstance(), reply.getState(), membership), newReplica);
	}


    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        Pair<Long, byte[]> val = pendingClientOrder.remove(notification.getOpId());
        if(val != null)
            this.cancelTimer(val.getLeft());

        wtvr.add(notification.getOpId());
        //if(!self.equals(leader)) nextInstance = notification.getInstance();
        if(!self.equals(leader)) nextInstance ++;
        triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));        
    }

    private void uponNewLeaderNotification(NewLeaderNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        
        leader = notification.getLeader();
        if (leader.equals(self)) {
            logger.info("Leader flushing pendingRemoves: " + pendingRemoves);

            pendingRemoves.forEach(m -> 
                sendRequest(new RemoveReplicaRequest(membership.indexOf(m), m), PaxosAgreement.PROTOCOL_ID));

            logger.info("Leader flushing prepare_ok messages: " + notification.getMessages());    
            
            notification.getMessages().forEach(m -> 
                sendRequest(new ProposeRequest(nextInstance++, m.getLeft(), m.getRight()), PaxosAgreement.PROTOCOL_ID));
            
            logger.info("Leader flushing pending orders:" + pendingOrders);
            pendingRemoves = new LinkedList<>();

            pendingOrders.forEach(m -> 
                sendRequest(new ProposeRequest(nextInstance++, m.getOpId(), m.getOperation()), PaxosAgreement.PROTOCOL_ID));

            //flush the adds last, since they dont have time out and the new replica can wait for the system to be stable.
        } else {
            logger.info("non leader yet -> sending to {}", leader);

            pendingOrders.forEach(m -> 
                sendMessage(new LeaderOrderMessage(m.getInstance(), m.getOpId(), m.getOperation()), leader));
        }
        pendingOrders = new LinkedList<>();
    }

    private void uponMembershipChangeNotification(MembershipChangedNotification notification, short sourceProto) {
        logger.info("Membership changed notification: " + notification);
        
        if (notification.isAdding()) {
            if(membership.contains(notification.getReplica()))
                sendRequest(new CurrentStateRequest(0), HashApp.PROTO_ID);
            else { 
                openConnection(notification.getReplica());
                membership.add(notification.getReplica());
            }            
        } else {
            closeConnection(notification.getReplica());
            membership.remove(notification.getReplica());
            pendingRemoves.remove(notification.getReplica());
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */

    private void uponLeaderOrderMessage(LeaderOrderMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received Leader Order Message: " + msg.getOpId() + nextInstance);
        
        if (leader == null) {
            logger.info("Leader still waiting majority, pending...");
            pendingOrders.add(new ProposeRequest(msg));
            return;
        }

        if(wtvr.contains(msg.getOpId())) {
            logger.info("FOR SOME REASON...");
            return;
        }
        wtvr.add(msg.getOpId());
        sendRequest(new ProposeRequest(nextInstance++, msg.getOpId(), msg.getOp()),
                    PaxosAgreement.PROTOCOL_ID); 
    }

    private void uponAddReplicaMessage(AddReplicaMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received Add Replica Message: " + msg);

        if (leader == null) {
            sendRequest(new PrepareRequest(nextInstance), PaxosAgreement.PROTOCOL_ID);
        
            //TODELETE BELLOW, TEST CASE:
            /* if(initialTODELETE == false) {
                sendRequest(new PrepareRequest(nextInstance), PaxosAgreement.PROTOCOL_ID);
                initialTODELETE = true;
            }
            return; */
        }
        
        if (self.equals(msg.getContact())) {
            openConnection(msg.getNewReplica());
            membership.add(msg.getNewReplica());
        }

        if(!self.equals(leader)) {
            sendMessage(new AddReplicaMessage(msg.getNewReplica(), nextInstance, msg.getContact()), leader);
            return;
        }

        openConnection(msg.getNewReplica());        
        sendRequest(new AddReplicaRequest(nextInstance, msg.getNewReplica()), PaxosAgreement.PROTOCOL_ID);
    }

    private void uponReplicaAddedMessage(ReplicaAddedMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Replica Added Message: {}", self);

        nextInstance = msg.getInstance();
        membership = new LinkedList<>(msg.getMembership());
        membership.forEach(this::openConnection);
        sendRequest(new InstallStateRequest(msg.getState()), HashApp.PROTO_ID);
        
        triggerNotification(new JoinedNotification(membership, membership.indexOf(self), false));
        state = State.ACTIVE;

        pendingOrders.forEach(m -> 
                sendMessage(new LeaderOrderMessage(m.getInstance(), m.getOpId(), m.getOperation()), leader));
        pendingOrders = new LinkedList<>();
    }

    private void uponLeaderMsgFail(LeaderOrderMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.info("Message {} to {} failed, reason: {}", msg, host, throwable);
        pendingOrders.add(new ProposeRequest(msg.getInstance(), msg.getOpId(), msg.getOp()));
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.debug("Message {} to {} failed, reason: {}", msg, host, throwable);

    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.info("Connection to {} is down, cause {}", event.getNode(), event.getCause());
 
        Host node = event.getNode();
        if(node.equals(leader)) {
            leader = null;
            closeConnection(node);
        }
        
        if (leader == null) { 
            pendingRemoves.add(node);
            sendRequest(new PrepareRequest(nextInstance), PaxosAgreement.PROTOCOL_ID);
        }

        if(self.equals(leader)) {
            sendRequest(new RemoveReplicaRequest(membership.indexOf(node), node), PaxosAgreement.PROTOCOL_ID);
        }
            
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        if (membership.contains(event.getNode()))
            openConnection(event.getNode());

        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
/*         Host node = event.getNode();
        if (!membership.contains(node)) return;

        Integer retries = retryHosts.putIfAbsent(node, nRetries);
        retries = (retries == null) ? nRetries : retries -1;

        if (retries > 0) {
            //Add Timer for retry
            //remove Replica after X
            retryHosts.put(node, retries);
            logger.info("Retrying connection to {}, retries left: {}", node, retries);
            openConnection(node);
        } else {
            logger.info("Removing {} from membership after {} failed retries", node, nRetries);
            retryHosts.remove(node);
            membership.remove(node);
            closeConnection(node); 
        } */
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    private void uponSendMessageTimer(SendMessageTimer timer, long timerId) {
        if (leader == null || !leader.equals(timerLeader)) {
            pendingClientOrder.values().stream()
                .filter(entry -> entry.getLeft() != null)
                .forEach(entry -> cancelTimer(entry.getLeft()));
        
            timerLeader = leader;
        }
        
		logger.info("helperTimer: {}", timerId);

        for (Map.Entry<UUID, Pair<Long, byte[]>> entry : pendingClientOrder.entrySet()) {
            if(!wtvr.contains(entry.getKey())) {
                if (entry.getValue().getLeft() == timerId) {
                    logger.info("THE TIMER IS WORKING?! Sending to {} the msg {}", leader, entry.getKey());
                    if(leader == null) {
                        pendingOrders.add(new ProposeRequest(0, entry.getKey(), entry.getValue().getRight()));
                    } else {
                        //long tid = setupTimer(new SendMessageTimer(), 3000);
                        //pendingClientOrder.put(entry.getKey(), Pair.of(tid, entry.getValue().getRight()));
                        sendMessage(new LeaderOrderMessage(nextInstance, entry.getKey(), entry.getValue().getRight()), leader);
                    }
                    
                }
            }
        }
		
	}

}
