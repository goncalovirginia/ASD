package protocols.agreement;

import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptOKMessage;
import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.messages.PrepareOKMessage;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.MembershipChangedNotification;
import protocols.agreement.notifications.NewLeaderNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.PrepareRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang3.tuple.Pair;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;

import java.io.IOException;
import java.util.*;

public class PaxosAgreement extends GenericProtocol {

    private static class AgreementInstanceState {
        private int acceptOkCount;
        private boolean decided;
        public AgreementInstanceState() {
            this.acceptOkCount = 0;
            this.decided = false;
        }

        public int getAcceptokCount() {
            return acceptOkCount;
        }
        public void incrementAcceptCount() {
            acceptOkCount ++;
        }

        public boolean decided() {
            return decided;
        }

        public void decide() {
            decided = true;
        }
    }

    private static final Logger logger = LogManager.getLogger(PaxosAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Agreement";

    private Host myself;
    private Host newLeader;
    private int joinedInstance;
    private int prepare_ok_count;
    private int highest_prepare;
    private int proposer_seq_number;
    private List<Host> membership;

    private int lastChosen;
    private int lastDecided;

    private Map<Integer, AgreementInstanceState> instanceStateMap; 

    private Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
    private Map<Integer, Pair<UUID, byte[]>> executedMessages;


    public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        prepare_ok_count = 0;
        
        highest_prepare = -1;
        proposer_seq_number = -1;
        
        
        lastChosen = 0; //
        lastDecided = 1; //

        toBeDecidedMessages = new TreeMap<>();
        executedMessages = new TreeMap<>();


        instanceStateMap = new HashMap<>();
        /*--------------------- Register Timer Handlers ----------------------------- */

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(PrepareRequest.REQUEST_ID, this::uponPrepareRequest);
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for events from the application or agreement
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOKMessage.MSG_ID, AcceptOKMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        try {
              registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
              registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
              registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
              registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    //TO DELETE AFTER DEALING WITH COMMENTS
    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            //Obviously your agreement protocols will not decide things as soon as you receive the first message
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    //highest joinedInstance wins
    private void uponPrepareRequest(PrepareRequest request, short sourceProto) {
        prepare_ok_count = 0; //this probably needs to be an actual set, for the edge case mentioned in the slides, we'll see
        proposer_seq_number = request.getInstance() + joinedInstance;
        PrepareMessage msg = new PrepareMessage(proposer_seq_number);
        membership.forEach(h -> sendMessage(msg, h));  
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            if(msg.getInstance() > highest_prepare) {
                highest_prepare = msg.getInstance();
                PrepareOKMessage prepareOK = new PrepareOKMessage(msg.getInstance());
                sendMessage(prepareOK, host);

                if(host.equals(newLeader)) {
                    triggerNotification(new NewLeaderNotification(host));
                    highest_prepare--;
                }
                newLeader = host;
            }
        } else {
            //TODO: uponBroadcast above comments
        }
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if (proposer_seq_number == msg.getInstance() && proposer_seq_number >= highest_prepare) {
            prepare_ok_count ++;
            if (prepare_ok_count >= (membership.size() / 2) + 1) {
                prepare_ok_count = -1;

                triggerNotification(new NewLeaderNotification(myself));
                membership.forEach(h -> {
                    if (!h.equals(myself))
                        sendMessage(new PrepareMessage(proposer_seq_number+1), h);
                });
            }
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        //The joining instances are sequential, the initial membership is 1,2,3,etc...
        //so in the joining proccess, we should take that into account.
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        instanceStateMap.putIfAbsent(request.getInstance(), new AgreementInstanceState());
        AcceptMessage msg = new AcceptMessage(request.getInstance(), request.getOpId(), request.getOperation(), lastChosen);
        membership.forEach(h -> sendMessage(msg, h));          
    }

    //joinInstance -1 case
    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {        
        if (!host.equals(myself)) {
            for(; lastDecided <= msg.getLastChosen(); lastDecided++) {
                Pair<UUID, byte[]> pair = toBeDecidedMessages.remove(lastDecided);
                if(pair != null) {
                    if(!msg.isAddOrRemoving()) {
                        triggerNotification(new DecidedNotification(lastDecided, pair.getLeft(), pair.getRight()));
                    } else if(msg.isAdding()) {
                        membership.add(msg.getNewReplica());
                        triggerNotification(new MembershipChangedNotification(msg.getNewReplica(), true));
                    } else {
                        membership.remove(msg.getNewReplica());
                        triggerNotification(new MembershipChangedNotification(msg.getNewReplica(), false));
                    }
                    
                    executedMessages.put(lastDecided, pair);
                } else break;
            }

            toBeDecidedMessages.putIfAbsent(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));
        }
        
        AcceptOKMessage acceptOK = new AcceptOKMessage(msg);
        sendMessage(acceptOK, host);
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
        AgreementInstanceState state = instanceStateMap.get(msg.getInstance());
        if (state != null) {
            state.incrementAcceptCount();
            if (state.getAcceptokCount() >= (membership.size() / 2) + 1 && !state.decided()) {
                state.decide();
                triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
                lastChosen = msg.getInstance();

                membership.forEach(h -> 
                    sendMessage(new AcceptMessage(msg.getInstance(), msg.getOpId(), msg.getOp(), lastChosen), h));
                          
            }
        }

        //Change the message instead like in Accept
/*                 if (state.isAdding()) {
                    membership.add(state.getReplica());
                    triggerNotification(new MembershipChangedNotification(state.getReplica(), true));
                } else if (state.isRemoving()) {
                    membership.remove(state.getReplica());
                    triggerNotification(new MembershipChangedNotification(state.getReplica(), false));
                } else {  } */
    }
    
    //Change Message instead, dunno if addReplica is added to Log  of State Machine
    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received Add Replica Request: " + request);
        instanceStateMap.putIfAbsent(request.getInstance(), new AgreementInstanceState());

        AcceptMessage msg = new AcceptMessage(request.getInstance(), request.getReplica(), true);
        membership.forEach(h -> sendMessage(msg, h)); 

        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
    }
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
