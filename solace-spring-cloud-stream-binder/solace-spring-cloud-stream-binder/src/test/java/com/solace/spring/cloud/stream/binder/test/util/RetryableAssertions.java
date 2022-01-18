package com.solace.spring.cloud.stream.binder.test.util;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.SoftAssertionsProvider;

import java.util.concurrent.TimeUnit;

public class RetryableAssertions {
	public static void retryAssert(SoftAssertionsProvider.ThrowingRunnable assertRun) throws InterruptedException {
		retryAssert(assertRun, 10, TimeUnit.SECONDS);
	}

	@SuppressWarnings("BusyWait")
	public static void retryAssert(SoftAssertionsProvider.ThrowingRunnable assertRun, long timeout, TimeUnit unit)
			throws InterruptedException {
		final long expiry = System.currentTimeMillis() + unit.toMillis(timeout);
		SoftAssertions softAssertions;
		do {
			softAssertions = new SoftAssertions();
			softAssertions.check(assertRun);
			if (!softAssertions.wasSuccess()) {
				Thread.sleep(500);
			}
		} while (!softAssertions.wasSuccess() && System.currentTimeMillis() < expiry);
		softAssertions.assertAll();
	}
}
