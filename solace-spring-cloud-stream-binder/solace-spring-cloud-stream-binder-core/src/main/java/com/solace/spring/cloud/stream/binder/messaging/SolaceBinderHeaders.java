package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.SDTStream;
import org.springframework.messaging.Message;

/**
 * <p>Solace-defined Spring headers to get/set Solace Spring Cloud Stream Binder properties
 * from/to Spring {@link Message Message} headers.</p>
 * <br/>
 * <p>These can be used for:</p>
 * <ul>
 *     <li>Getting/Setting Solace Binder metadata</li>
 *     <li>Directive actions for the binder when producing/consuming messages</li>
 * </ul>
 * <br/>
 * <p><b>Header Access Control:</b></p>
 * <p>Be aware that each header has an expected usage scenario.
 * Using headers outside of their intended access-control scenario is not supported.</p>
 */
public final class SolaceBinderHeaders {
	/**
	 * The prefix used for all headers in this class.
	 */
	static final String PREFIX = SolaceHeaders.PREFIX + "scst_";

	/**
	 * <p><b>Acceptable Value Type:</b> {@code int}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br/>
	 * <p>A static number set by the publisher to indicate the Spring Cloud Stream Solace message version.</p>
	 *
	 * <p>Primarily used internally by the binder for backwards-compatibility while consuming messages published
	 * by older versions of the binder.</p>
	 */
	public static final String MESSAGE_VERSION = Meta.MESSAGE_VERSION.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@code boolean}</p>
	 * <p><b>Access:</b> Internal Binder Use Only</p>
	 * <br/>
	 * <p>Is {@code true} if a Solace Spring Cloud Stream binder has serialized the payload before publishing
	 * it to a broker. Is undefined otherwise.</p>
	 */
	public static final String SERIALIZED_PAYLOAD = Meta.SERIALIZED_PAYLOAD.getName();

	/**
	 * <p><b>Acceptable Value Type:</b> {@link SDTStream}</p>
	 * <p><b>Access:</b> Internal Binder Use Only</p>
	 * <br/>
	 * <p>A stream of header names where each entry indicates that that header’s value was serialized by a
	 * Solace Spring Cloud Stream binder before publishing it to a broker.</p>
	 */
	public static final String SERIALIZED_HEADERS = Meta.SERIALIZED_HEADERS.getName();

	enum Meta implements HeaderMeta {
		MESSAGE_VERSION(PREFIX + "messageVersion", int.class, AccessLevel.READ),
		SERIALIZED_PAYLOAD(PREFIX + "serializedPayload", boolean.class, AccessLevel.NONE),
		SERIALIZED_HEADERS(PREFIX + "serializedHeaders", SDTStream.class, AccessLevel.NONE);

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
