package protocols.agreement;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.abd.utils.Tag;
import protocols.agreement.messages.*;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.MembershipChangedNotification;
import protocols.agreement.notifications.NewLeaderNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.PrepareRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class PaxosAgreement extends GenericProtocol {

	private static class AgreementInstanceState {

		private int acceptOkCount;
		private boolean isDecided;

		public AgreementInstanceState() {
			this.acceptOkCount = 0;
			this.isDecided = false;
		}

		public int getAcceptOkCount() {
			return acceptOkCount;
		}

		public void incrementAcceptCount() {
			acceptOkCount++;
		}

		public boolean isDecided() {
			return isDecided;
		}

		public void decide() {
			isDecided = true;
		}

	}

	private static final Logger logger = LogManager.getLogger(PaxosAgreement.class);

	//Protocol information, to register in babel
	public final static short PROTOCOL_ID = 100;
	public final static String PROTOCOL_NAME = "Agreement";

	private Host myself;
	private int joinedInstance;
	private int prepare_ok_count;
	private List<Pair<UUID, byte[]>> prepareOkMessages;
	private Tag highest_prepare;
	private List<Host> membership;
	private int toBeDecidedIndex;
	private Map<Integer, AgreementInstanceState> instanceStateMap;
	private Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
	private Map<Integer, Pair<UUID, byte[]>> acceptedMessages;

	public PaxosAgreement(Properties props) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		joinedInstance = -1; //-1 means we have not yet joined the system
		membership = null;
		prepare_ok_count = 0;
		highest_prepare = new Tag(-1, -1);
		toBeDecidedIndex = 1; //

		toBeDecidedMessages = new TreeMap<>();
		acceptedMessages = new TreeMap<>();

		prepareOkMessages = new LinkedList<>();
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

		registerMessageSerializer(cId, ChangeMembershipMessage.MSG_ID, ChangeMembershipMessage.serializer);
		registerMessageSerializer(cId, ChangeMembershipOKMessage.MSG_ID, ChangeMembershipOKMessage.serializer);

		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
			registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
			registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOKMessage, this::uponMsgFail);
			registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
			registerMessageHandler(cId, AcceptOKMessage.MSG_ID, this::uponAcceptOKMessage, this::uponMsgFail);

			registerMessageHandler(cId, ChangeMembershipMessage.MSG_ID, this::uponChangeMembershipMessage, this::uponMsgFail);
			registerMessageHandler(cId, ChangeMembershipOKMessage.MSG_ID, this::uponChangeMembershipOKMessage, this::uponMsgFail);
		} catch (HandlerRegistrationException e) {
			throw new AssertionError("Error registering message handler.", e);
		}

	}

	//TO DELETE AFTER DEALING WITH COMMENTS
	private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
		if (joinedInstance >= 0) {
			//Obviously your agreement protocols will not decide things as soon as you receive the first message
			triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
		} else {
			//We have not yet received a JoinedNotification, but we are already receiving messages from the other
			//agreement instances, maybe we should do something with them...?
		}
	}

	//highest joinedInstance wins
	private void uponPrepareRequest(PrepareRequest request, short sourceProto) {
		prepare_ok_count = 0;
		/*         proposer_seq_number = joinedInstance; */
		Tag newTag = new Tag(highest_prepare.getOpSeq() + 1, joinedInstance);
		PrepareMessage msg = new PrepareMessage(newTag, request.getInstance(), false);

		membership.forEach(h -> sendMessage(msg, h));
	}

	private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
		if (joinedInstance < 0 || !msg.getSeqNumber().greaterOrEqualThan(highest_prepare)) return;

		if (msg.isOK()) {
			highest_prepare = msg.getSeqNumber();
			logger.info("host {} --> NEW LEADER {} ", myself, host);
			triggerNotification(new NewLeaderNotification(host));
			return;
		}
		highest_prepare = msg.getSeqNumber();

		logger.info("ON PREPARE TBI {} - instance {} ", toBeDecidedIndex, msg.getInstance());
		List<Pair<UUID, byte[]>> relevantMessages = new LinkedList<>();
		if (toBeDecidedIndex >= (msg.getInstance())) {//toBeDecided - 1 == last executed/accepted msg
			logger.info("SHOULD BE TRUE");
			relevantMessages.addAll((((TreeMap<Integer, Pair<UUID, byte[]>>) acceptedMessages)
					.tailMap((msg.getInstance()), true)
					.values()));

			relevantMessages.addAll((((TreeMap<Integer, Pair<UUID, byte[]>>) toBeDecidedMessages)
					.tailMap((msg.getInstance()), true)
					.values()));
		}

		PrepareOKMessage prepareOK = new PrepareOKMessage(highest_prepare, relevantMessages);
		sendMessage(prepareOK, host);
	}

	//Prepare_OK messages with accepted values for any instance >= n.
	private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
		if (!msg.getSeqNumber().greaterOrEqualThan(highest_prepare)) return;

		prepare_ok_count++;

		if (msg.getPrepareOKMsgs().size() > prepareOkMessages.size())
			prepareOkMessages = msg.getPrepareOKMsgs();

		if (prepare_ok_count >= (membership.size() / 2) + 1) {
			prepare_ok_count = -1;

			logger.info(prepareOkMessages);
			highest_prepare = msg.getSeqNumber();

			triggerNotification(new NewLeaderNotification(myself, prepareOkMessages));
			membership.forEach(h -> {
				if (!h.equals(myself))
					sendMessage(new PrepareMessage(msg.getSeqNumber(), -1, true), h);
			});
		}
	}

	private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
		joinedInstance = notification.getJoinInstance();
		membership = new LinkedList<>(notification.getMembership());
		logger.info("Agreement starting at INSTANCE {},  PROCESS: {}, MEMBERSHIP: {}", toBeDecidedIndex, joinedInstance, membership);
	}

	private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
		logger.debug("Received Add Replica Request: " + request);
		instanceStateMap.put(0, new AgreementInstanceState());

		sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), highest_prepare, true, true), request.getReplica());

		membership.forEach(h ->
				sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), highest_prepare, false, true), h));
	}

	private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
		logger.debug("Received Remove Replica Request: " + request);
		//makes no sense to include the replica that is dead in the broadcast
		membership.remove(request.getReplica());
		if (joinedInstance > request.getInstance())
			joinedInstance--;

		instanceStateMap.put(0, new AgreementInstanceState());
		membership.forEach(h ->
				sendMessage(new ChangeMembershipMessage(request.getReplica(), request.getInstance(), highest_prepare, false, false), h));
	}

	private void uponProposeRequest(ProposeRequest request, short sourceProto) {
		logger.info("Received Propose Request: {} - {}", request.getInstance(), request.getOpId() );

		instanceStateMap.put(request.getInstance(), new AgreementInstanceState());
		AcceptMessage msg = new AcceptMessage(request.getInstance(), highest_prepare, request.getOpId(), request.getOperation(), toBeDecidedIndex - 1);
		membership.forEach(h -> sendMessage(msg, h));
	}

	private void uponChangeMembershipMessage(ChangeMembershipMessage msg, Host host, short sourceProto, int channelId) {
		if (!msg.getSeqNumber().greaterOrEqualThan(highest_prepare)) return;
		highest_prepare = msg.getSeqNumber();

		if (msg.isOK()) {
			if (msg.getReplica().equals(myself)) { //obviously, when removing this is impossible to trigger
				toBeDecidedIndex = msg.getInstance();
				return;
			}

			if (msg.isAdding()) {
				membership.add(msg.getReplica());
			} else {
				membership.remove(msg.getReplica());
				if (joinedInstance > msg.getInstance())
					joinedInstance--;
			}
			triggerNotification(new MembershipChangedNotification(msg.getReplica(), msg.isAdding(), channelId));
			return;
		}

		ChangeMembershipOKMessage acceptOK = new ChangeMembershipOKMessage(msg.getReplica(), msg.getInstance(), msg.isAdding());
		sendMessage(acceptOK, host);
	}

	private void uponChangeMembershipOKMessage(ChangeMembershipOKMessage msg, Host host, short sourceProto, int channelId) {
		AgreementInstanceState state = instanceStateMap.get(0);

		if (state == null) return;
		state.incrementAcceptCount();

		if (state.getAcceptOkCount() < (membership.size() / 2) + 1 || state.isDecided()) return;

		state.decide();

		membership.forEach(h -> {
			if (!h.equals(myself)) sendMessage(new ChangeMembershipMessage(msg.getReplica(), msg.getInstance(), highest_prepare, true, msg.isAdding()), h);
		});

		if (msg.isAdding())
			membership.add(msg.getReplica());

		triggerNotification(new MembershipChangedNotification(msg.getReplica(), msg.isAdding(), channelId));
	}

	private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
		logger.info("ON ACCEPT: sn ({}, {}) - hp ({}, {})" + msg.getSeqNumber().getOpSeq(), msg.getSeqNumber().getProcessId(), highest_prepare.getOpSeq(), highest_prepare.getProcessId());

		if (!msg.getSeqNumber().greaterOrEqualThan(highest_prepare)) return;
		if (!(msg.getSeqNumber()).greaterOrEqualThan(highest_prepare))
			return;
		highest_prepare = msg.getSeqNumber(); //update info for replicas that missed prepare (added for instance)

		if (!host.equals(myself) && (msg.getInstance() >= toBeDecidedIndex)) {
			if (joinedInstance >= 0) {
				for (; toBeDecidedIndex <= msg.getLastChosen(); toBeDecidedIndex++) {
					Pair<UUID, byte[]> pair = toBeDecidedMessages.remove(toBeDecidedIndex);
					if (pair != null) {
						acceptedMessages.put(toBeDecidedIndex, pair);
						triggerNotification(new DecidedNotification(toBeDecidedIndex, pair.getLeft(), pair.getRight()));
					}
				}

				if (acceptedMessages.containsKey(msg.getInstance())) {
					return;
				}
			}

			Pair<UUID, byte[]> val = toBeDecidedMessages.putIfAbsent(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));
			if (val != null) {
				return;
			}
		}

		AcceptOKMessage acceptOK = new AcceptOKMessage(msg);
		sendMessage(acceptOK, host);
	}

	private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
		AgreementInstanceState state = instanceStateMap.get(msg.getInstance());

		if (state == null) return;
		state.incrementAcceptCount();

		if (state.getAcceptOkCount() < (membership.size() / 2) + 1 || state.isDecided()) return;

		state.decide();
		triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));

		acceptedMessages.put(msg.getInstance(), Pair.of(msg.getOpId(), msg.getOp()));
		membership.forEach(h -> {
			if (h != myself) sendMessage(new AcceptMessage(msg.getInstance(), highest_prepare, msg.getOpId(), msg.getOp(), toBeDecidedIndex), h);
		});
		toBeDecidedIndex = msg.getInstance() + 1;
	}


	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		//If a message fails to be sent, for whatever reason, log the message and the reason
		//logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

}
