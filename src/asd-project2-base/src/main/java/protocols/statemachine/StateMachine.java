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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.PaxosAgreement;
import protocols.statemachine.messages.AddReplicaMessage;
import protocols.statemachine.messages.LeaderElectionMessage;
import protocols.statemachine.messages.LeaderOrderMessage;
import protocols.statemachine.messages.ReplicaAddedMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.PrepareRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.app.HashApp;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

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

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 0;
        leader = null;

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

        registerMessageSerializer(channelId, LeaderElectionMessage.MSG_ID, LeaderElectionMessage.serializer);
        registerMessageSerializer(channelId, LeaderOrderMessage.MSG_ID, LeaderOrderMessage.serializer);
        registerMessageSerializer(channelId, AddReplicaMessage.MSG_ID, AddReplicaMessage.serializer);
        registerMessageSerializer(channelId, ReplicaAddedMessage.MSG_ID, ReplicaAddedMessage.serializer);

        registerMessageHandler(channelId, LeaderElectionMessage.MSG_ID, this::uponLeaderElection, this::uponMsgFail);
        registerMessageHandler(channelId, LeaderOrderMessage.MSG_ID, this::uponLeaderOrderMessage, this::uponMsgFail);
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
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        pendingOrders = new LinkedList<>();

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
            triggerNotification(new JoinedNotification(membership, initialMembership.indexOf(self)));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");

            String contact = props.getProperty("contact");
			try {
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                openConnection(contactHost);
		        sendMessage(new AddReplicaMessage(self), contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: {}", contact);
			    logger.error(e.getStackTrace());
			    System.exit(-1);
                e.printStackTrace();
            }
            
            //You have to do something to join the system and know which instance you joined
            // (and copy the state of that instance)
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        if (state == State.JOINING) {
            //Do something smart (like buffering the requests)
        } else if (state == State.ACTIVE) {            
            if (leader == null) {
                pendingOrders.add(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()));
                sendMessage(new LeaderElectionMessage(), membership.get(0));
            } else if(self.equals(leader)) {
                sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                    PaxosAgreement.PROTOCOL_ID); 
            } else {
                sendMessage(new LeaderOrderMessage(nextInstance++, request.getOpId(), request.getOperation()), leader);
            }
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponCurrentStateReply(CurrentStateReply reply, short protoID) {
		logger.info("Received Current State Reply: {}", reply.toString());

        Host newReplica = membership.get(reply.getInstance());
        openConnection(newReplica);
        sendMessage(new ReplicaAddedMessage(reply.getInstance(), reply.getState(), membership), newReplica);
	}


    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.info("{} On Instance {} Received notification: {}", leader.equals(self) ? "LEADER" : self, notification.getInstance(), notification.getOpId());
        
        if(leader.equals(self)) {            
            triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));        
        } else triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));
    }

    private void uponNewLeaderNotification(NewLeaderNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        
        leader = notification.getLeader();
        if (leader.equals(self)) {
            logger.info("Leader, flushing");
            pendingOrders.forEach(m -> 
            sendRequest(new ProposeRequest(nextInstance++, m.getOpId(), m.getOperation()), PaxosAgreement.PROTOCOL_ID));
        } else {
            logger.info("non leader yet");
            pendingOrders.forEach(m -> 
                sendMessage(new LeaderOrderMessage(m.getInstance(), m.getOpId(), m.getOperation()), leader));
        }
        pendingOrders = new LinkedList<>();
    }

    private void uponMembershipChangeNotification(MembershipChangedNotification notification, short sourceProto) {
        logger.debug("Membership changed notification: " + notification);
        
        if (notification.isAdding()) {
            openConnection(notification.getReplica());
            membership.add(notification.getReplica());
        } else {
            closeConnection(notification.getReplica());
            membership.remove(notification.getReplica());
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponLeaderElection(LeaderElectionMessage msg, Host host, short sourceProto, int channelId) {
        sendRequest(new PrepareRequest(nextInstance), PaxosAgreement.PROTOCOL_ID);
    }

    private void uponLeaderOrderMessage(LeaderOrderMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received Leader Order Message: ");
        //the leader is not initialized(has not received majority yet)
        if (leader == null) {
            logger.info("Leader still waiting majority, pending...");
            pendingOrders.add(new ProposeRequest(msg));
            return;
        } else if(pendingOrders.size() > 0) { //this is probably not needed
            logger.info("NEEDED AFTER ALL?...");
            pendingOrders.forEach(m -> 
            sendRequest(new ProposeRequest(nextInstance++, m.getOpId(), m.getOperation()), PaxosAgreement.PROTOCOL_ID));
        }

        sendRequest(new ProposeRequest(nextInstance++, msg.getOpId(), msg.getOp()),
                    PaxosAgreement.PROTOCOL_ID); 
    }

    private void uponAddReplicaMessage(AddReplicaMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received Add Replica Message: ");
        //the leader is not initialized(has not received majority yet) 

        if(!leader.equals(self)) { //this is probably not needed
            sendMessage(new AddReplicaMessage(msg.getNewReplica()), leader);
        } else if(leader.equals(self)) {
            membership.add(msg.getNewReplica());    
            sendRequest(new CurrentStateRequest(nextInstance), HashApp.PROTO_ID);

            sendRequest(new AddReplicaRequest(nextInstance, msg.getNewReplica()), PaxosAgreement.PROTOCOL_ID);
        } else {
            logger.info("leader null, is this possible? Later");
        }
    }

    private void uponReplicaAddedMessage(ReplicaAddedMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Replica Added Message: {}", self);

        nextInstance = msg.getInstance();
        membership = new LinkedList<>(msg.getMembership());
        membership.forEach(this::openConnection);
        triggerNotification(new JoinedNotification(membership, membership.indexOf(self)));
        state = State.ACTIVE;
        sendRequest(new InstallStateRequest(msg.getState()), HashApp.PROTO_ID);		
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
        if(membership.contains(event.getNode()))
            openConnection(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
