package com.solace.spring.cloud.stream.binder.messaging;

public interface HeaderMeta {
	String getName();
	Class<?> getType();
	AccessLevel getAccessLevel();

	enum AccessLevel {
		READ("Read"),
		WRITE("Write"),
		READ_WRITE("Read/Write"),
		NONE("None");

		private final String label;

		AccessLevel(String label) {
			this.label = label;
		}

		public String getLabel() {
			return label;
		}
	}
}
