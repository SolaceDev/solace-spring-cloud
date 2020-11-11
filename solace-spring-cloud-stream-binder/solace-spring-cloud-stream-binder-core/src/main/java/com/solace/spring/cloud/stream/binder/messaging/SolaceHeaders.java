package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.Destination;
import org.springframework.messaging.Message;

/**
 * <p>Solace-defined Spring headers to get/set Solace message properties from/to
 * Spring {@link Message Message} headers.</p>
 * <br/>
 * <p><b>Header Access Control:</b></p>
 * <p>Be aware that each header has an expected usage scenario.
 * Using headers outside of their intended access-control scenario is not supported.</p>
 */
public final class SolaceHeaders {
	/**
	 * The prefix used for all headers in this class.
	 */
	static final String PREFIX = "solace_";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The message ID (a string for an application-specific message identifier).</p>
	 * <p>This is the {@code JMSMessageID} header field if publishing/consuming to/from JMS.</p>
	 */
	public static final String APPLICATION_MESSAGE_ID = Meta.APPLICATION_MESSAGE_ID.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The application message type.</p>
	 * <p>This is the {@code JMSType} header field if publishing/consuming to/from JMS.</p>
	 */
	public static final String APPLICATION_MESSAGE_TYPE = Meta.APPLICATION_MESSAGE_TYPE.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The correlation ID.</p>
	 */
	public static final String CORRELATION_ID = Meta.CORRELATION_ID.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Destination}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br/>
	 * <p>The destination this message was published to.</p>
	 */
	public static final String DESTINATION = Meta.DESTINATION.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code boolean}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br/>
	 * <p>Whether one or more messages have been discarded prior to the current message.</p>
	 */
	public static final String DISCARD_INDICATION = Meta.DISCARD_INDICATION.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code boolean}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>Whether the message is eligible to be moved to a Dead Message Queue.</p>
	 */
	public static final String DMQ_ELIGIBLE = Meta.DMQ_ELIGIBLE.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The UTC time (in milliseconds, from midnight, January 1, 1970 UTC) when the message is supposed to
	 * expire.</p>
	 */
	public static final String EXPIRATION = Meta.EXPIRATION.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The HTTP content encoding header value from interaction with an HTTP client.</p>
	 */
	public static final String HTTP_CONTENT_ENCODING = Meta.HTTP_CONTENT_ENCODING.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code int}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>Priority value in the range of 0–255, or -1 if it is not set.</p>
	 */
	public static final String PRIORITY = Meta.PRIORITY.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code long}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br/>
	 * <p>The receive timestamp (in milliseconds, from midnight, January 1, 1970 UTC).</p>
	 */
	public static final String RECEIVE_TIMESTAMP = Meta.RECEIVE_TIMESTAMP.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code boolean}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br/>
	 * <p>Indicates if the message has been delivered by the broker to the API before.</p>
	 */
	public static final String REDELIVERED = Meta.REDELIVERED.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Destination}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The replyTo destination for the message.</p>
	 */
	public static final String REPLY_TO = Meta.REPLY_TO.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The Sender ID for the message.</p>
	 */
	public static final String SENDER_ID = Meta.SENDER_ID.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The send timestamp (in milliseconds, from midnight, January 1, 1970 UTC).</p>
	 */
	public static final String SENDER_TIMESTAMP = Meta.SENDER_TIMESTAMP.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The sequence number.</p>
	 */
	public static final String SEQUENCE_NUMBER = Meta.SEQUENCE_NUMBER.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>The number of milliseconds before the message is discarded or moved to a Dead Message Queue.</p>
	 */
	public static final String TIME_TO_LIVE = Meta.TIME_TO_LIVE.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code byte[]}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br/>
	 * <p>When an application sends a message, it can optionally attach application-specific data along
	 * with the message, such as user data.</p>
	 */
	public static final String USER_DATA = Meta.USER_DATA.getName();

	enum Meta implements HeaderMeta {
		APPLICATION_MESSAGE_ID(PREFIX + "applicationMessageId", String.class, AccessLevel.READ_WRITE),
		APPLICATION_MESSAGE_TYPE(PREFIX + "applicationMessageType", String.class, AccessLevel.READ_WRITE),
		CORRELATION_ID(PREFIX + "correlationId", String.class, AccessLevel.READ_WRITE),
		DESTINATION(PREFIX + "destination", Destination.class, AccessLevel.READ),
		DISCARD_INDICATION(PREFIX + "discardIndication", boolean.class, AccessLevel.READ),
		DMQ_ELIGIBLE(PREFIX + "dmqEligible", boolean.class, AccessLevel.READ_WRITE),
		EXPIRATION(PREFIX + "expiration", long.class, AccessLevel.READ_WRITE),
		HTTP_CONTENT_ENCODING(PREFIX + "httpContentEncoding", String.class, AccessLevel.READ_WRITE),
		PRIORITY(PREFIX + "priority", int.class, AccessLevel.READ_WRITE),
		RECEIVE_TIMESTAMP(PREFIX + "receiveTimestamp", long.class, AccessLevel.READ),
		REDELIVERED(PREFIX + "redelivered", boolean.class, AccessLevel.READ),
		REPLY_TO(PREFIX + "replyTo", Destination.class, AccessLevel.READ_WRITE),
		SENDER_ID(PREFIX + "senderId", String.class, AccessLevel.READ_WRITE),
		SENDER_TIMESTAMP(PREFIX + "senderTimestamp", Long.class, AccessLevel.READ_WRITE),
		SEQUENCE_NUMBER(PREFIX + "sequenceNumber", Long.class, AccessLevel.READ_WRITE),
		TIME_TO_LIVE(PREFIX + "timeToLive", long.class, AccessLevel.READ_WRITE),
		USER_DATA(PREFIX + "userData", byte[].class, AccessLevel.READ_WRITE);

		private final String name;
		private final Class<?> type;
		private final AccessLevel accessLevel;

		Meta(String name, Class<?> type, AccessLevel accessLevel) {
			this.name = name;
			this.type = type;
			this.accessLevel = accessLevel;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public Class<?> getType() {
			return type;
		}

		@Override
		public AccessLevel getAccessLevel() {
			return accessLevel;
		}
	}
}
