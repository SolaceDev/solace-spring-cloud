package com.solace.spring.cloud.stream.binder.messaging;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.messaging.MessageHeaders;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.CoreMatchers.startsWithIgnoringCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SolaceHeadersTest {
	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public Class<?> headersClass;

	@Parameterized.Parameter(2)
	public Map<String, ? extends HeaderMeta<?>> headersMeta;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<?> headerSets() {
		return Arrays.asList(new Object[][]{
				{SolaceHeaders.class.getSimpleName(), SolaceHeaders.class, SolaceHeaderMeta.META},
				{SolaceBinderHeaders.class.getSimpleName(), SolaceBinderHeaders.class, SolaceBinderHeaderMeta.META}
		});
	}

	@Test
	public void testPrefix() throws Exception {
		Field field = getPrefixField();
		assertEquals(String.format("%s is not a String", field.getName()), String.class, field.getType());
		assertTrue(String.format("%s is not static", field.getName()), Modifier.isStatic(field.getModifiers()));
		assertTrue(String.format("%s is not final", field.getName()), Modifier.isFinal(field.getModifiers()));
		assertThat((String) field.get(null), startsWith(SolaceHeaders.PREFIX));
		assertTrue(((String) field.get(null)).matches("[a-z][a-z_]+_"));

		if (headersClass != SolaceHeaders.class) {
			assertNotEquals(field.get(null), SolaceHeaders.PREFIX);
		}
	}

	@Test
	public void testFieldDeclaration() {
		for (Field field : getAllHeaderFields()) {
			assertEquals(String.format("%s is not a String", field.getName()), String.class, field.getType());
			assertTrue(String.format("%s is not final", field.getName()), Modifier.isFinal(field.getModifiers()));
		}
	}

	@Test
	public void testFieldNameSyntax() throws Exception {
		for (Field field : getAllHeaderFields()) {
			assertTrue(String.format("%s name does not have proper syntax", field.getName()),
					field.getName().matches("[A-Z][A-Z_]+[A-Z]"));

			assertThat(String.format("%s name should not start with prefix", field.getName()),
					field.getName(), not(startsWithIgnoringCase((String) getPrefixField().get(null))));

			String noPrefixHeader = ((String) field.get(null))
					.substring(((String) getPrefixField().get(null)).length());
			assertEquals(String.format(
					"%s name should be the prefix-trimmed, fully-capitalized, '_'-delimited version of %s",
					field.getName(), field.get(null)),
					camelCaseToSnakeCase(noPrefixHeader).toUpperCase(), field.getName());
		}
	}

	@Test
	public void testHeaderSyntax() throws Exception {
		for (String header : getAllHeaders()) {
			String prefix = (String) getPrefixField().get(null);
			assertThat(header, startsWith(prefix));
			assertTrue(String.format("%s does not have proper syntax", header),
					header.matches(prefix + "[a-z][a-zA-Z]+"));
		}
	}

	@Test
	public void testUniqueHeaders() {
		List<String> headers = getAllHeaders();
		assertEquals(String.join(", ", headers) + " does not have unique values",
				headers.stream().distinct().count(), headers.size());
	}

	@Test
	public void testHeadersHaveMetaObjects() {
		List<String> headers = getAllHeaders();
		assertEquals(headersMeta.size(), headers.size());
		for (String header : headers) {
			assertThat(headersMeta.keySet(), hasItem(header));
		}
	}

	@Test
	public void testValidMeta() {
		headersMeta.values()
				.forEach(m -> {
					assertNotNull(m.getType());
					assertFalse(String.format("primitives not supported by %s", MessageHeaders.class.getSimpleName()),
							m.getType().isPrimitive());
				});
	}

	@Test
	public void testUniqueMetaNames() {
		assertEquals(String.join(", ", headersMeta.keySet()) + " does not have unique values",
				headersMeta.keySet().stream().distinct().count(), headersMeta.keySet().size());
	}

	private Field getPrefixField() throws NoSuchFieldException {
		return headersClass.getDeclaredField("PREFIX");
	}

	private List<Field> getAllHeaderFields() {
		return Arrays.stream(headersClass.getDeclaredFields())
				.filter(f -> Modifier.isPublic(f.getModifiers()))
				.filter(f -> Modifier.isStatic(f.getModifiers()))
				.collect(Collectors.toList());
	}

	private List<String> getAllHeaders() {
		return getAllHeaderFields().stream().map(f -> {
					try {
						return (String) f.get(null);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}
				})
				.collect(Collectors.toList());
	}

	private String camelCaseToSnakeCase(String camelCase) {
		Matcher camelCaseMatcher = Pattern.compile("(?<=[a-z])[A-Z]").matcher(camelCase);
		StringBuffer buffer = new StringBuffer();
		while (camelCaseMatcher.find()) {
			camelCaseMatcher.appendReplacement(buffer, "_" + camelCaseMatcher.group().toLowerCase());
		}
		camelCaseMatcher.appendTail(buffer);
		return buffer.toString();
	}
}
