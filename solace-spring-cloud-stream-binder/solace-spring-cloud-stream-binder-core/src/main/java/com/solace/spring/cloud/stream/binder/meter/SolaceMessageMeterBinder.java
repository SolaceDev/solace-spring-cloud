package com.solace.spring.cloud.stream.binder.meter;

import com.solacesystems.jcsmp.XMLMessage;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.lang.NonNull;

public class SolaceMessageMeterBinder implements MeterBinder {
	MeterRegistry registry;

	public static final String METER_NAME_TOTAL_SIZE = "solace.message.size.total";
	public static final String METER_NAME_PAYLOAD_SIZE = "solace.message.size.payload";
	public static final String METER_DESCRIPTION_TOTAL_SIZE = "Total message size";
	public static final String METER_DESCRIPTION_PAYLOAD_SIZE = "Message payload size";
	public static final String TAG_NAME = "name";

	@Override
	public void bindTo(@NonNull MeterRegistry registry) {
		this.registry = registry;
	}

	public void recordMessage(@NonNull String bindingName, @NonNull XMLMessage message) {
		long payloadSize = message.getAttachmentContentLength() + message.getContentLength();
		registerSizeMeter(METER_NAME_TOTAL_SIZE, METER_DESCRIPTION_TOTAL_SIZE, bindingName)
				.record(payloadSize + message.getBinaryMetadataContentLength(0));
		registerSizeMeter(METER_NAME_PAYLOAD_SIZE, METER_DESCRIPTION_PAYLOAD_SIZE, bindingName)
				.record(payloadSize);
	}

	private DistributionSummary registerSizeMeter(@NonNull String meterName,
												  @NonNull String description,
												  @NonNull String bindingName) {
		return DistributionSummary.builder(meterName)
				.description(description)
				.tag(TAG_NAME, bindingName)
				.baseUnit(BaseUnits.BYTES)
				.register(registry);
	}
}
