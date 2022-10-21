package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Objects;
import java.util.StringJoiner;

public class RetryableBindTask implements RetryableTaskService.RetryableTask {
	private final FlowReceiverContainer flowReceiverContainer;

	private static final Log logger = LogFactory.getLog(RetryableBindTask.class);

	public RetryableBindTask(FlowReceiverContainer flowReceiverContainer) {
		this.flowReceiverContainer = flowReceiverContainer;
	}

	@Override
	public boolean run(int attempt) {
		try {
			flowReceiverContainer.abortableBind();
			return true;
		} catch (FlowReceiverContainer.AbortedBindException e) {
			logger.debug(String.format("Async bind task was told to abort, cancelling bind task to queue %s",
					flowReceiverContainer.getQueueName()));
			return true;
		} catch (JCSMPException e) {
			if (e instanceof JCSMPErrorResponseException &&
					((JCSMPErrorResponseException) e).getSubcodeEx() == JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME) {
				logger.error(String.format("Queue %s no longer exists. Aborting bind",
						flowReceiverContainer.getQueueName()), e);
				//TODO Set flow health indicator as DOWN (SOL-79060)
				return true;
			}

			logger.warn(String.format("failed to bind queue %s. Will retry", flowReceiverContainer.getQueueName()), e);
			return false;
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RetryableBindTask.class.getSimpleName() + "[", "]")
				.add("flowReceiverContainer=" + flowReceiverContainer.getId())
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RetryableBindTask that = (RetryableBindTask) o;
		return Objects.equals(flowReceiverContainer.getId(), that.flowReceiverContainer.getId());
	}

	@Override
	public int hashCode() {
		return Objects.hash(flowReceiverContainer);
	}
}
