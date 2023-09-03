package com.ververica.flink.table.gateway.utils;

import lombok.Builder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A util for validate {@link Configuration} in flink sql gateway
 *
 * <p>all the method are copy from {org.apache.flink.table.descriptors.DescriptorProperties}.
 */
@Builder
public class ConfigurationValidater {

    private Configuration configuration;

    private static final Consumer<String> EMPTY_CONSUMER = (value) -> {};

    /** Validates an long property. */
    /** Validates an long property. */
    public void validateLong(String key, boolean isOptional) {
        validateLong(key, isOptional, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /** Validates an long property. The boundaries are inclusive. */
    public void validateLong(String key, boolean isOptional, long min) {
        validateLong(key, isOptional, min, Long.MAX_VALUE);
    }

    /** Validates an long property. The boundaries are inclusive. */
    public void validateLong(String key, boolean isOptional, long min, long max) {
        validateComparable(key, isOptional, min, max, "long", Long::valueOf);
    }

    /** Validates a string property. */
    public void validateString(String key, boolean isOptional) {
        validateString(key, isOptional, 0, Integer.MAX_VALUE);
    }

    /** Validates a string property. The boundaries are inclusive. */
    public void validateString(String key, boolean isOptional, int minLen) {
        validateString(key, isOptional, minLen, Integer.MAX_VALUE);
    }

    /** Validates a string property. The boundaries are inclusive. */
    public void validateString(String key, boolean isOptional, int minLen, int maxLen) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    final int length = value.length();
                    if (length < minLen || length > maxLen) {
                        throw new ValidationException(
                                "Property '"
                                        + key
                                        + "' must have a length between "
                                        + minLen
                                        + " and "
                                        + maxLen
                                        + " but was: "
                                        + value);
                    }
                });
    }

    private void validateOptional(
            String key, boolean isOptional, Consumer<String> valueValidation) {
        if (!configuration.toMap().containsKey(key)) {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        } else {
            final String value = configuration.toMap().get(key);
            valueValidation.accept(value);
        }
    }


    /** Validates an integer property. */
    public void validateInt(String key, boolean isOptional) {
        validateInt(key, isOptional, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    /** Validates an integer property. The boundaries are inclusive. */
    public void validateInt(String key, boolean isOptional, int min) {
        validateInt(key, isOptional, min, Integer.MAX_VALUE);
    }

    /** Validates an integer property. The boundaries are inclusive. */
    public void validateInt(String key, boolean isOptional, int min, int max) {
        validateComparable(key, isOptional, min, max, "integer", Integer::valueOf);
    }

    /**
     * Validates a property by first parsing the string value to a comparable object. The boundaries
     * are inclusive.
     */
    private <T extends Comparable<T>> void validateComparable(
            String key,
            boolean isOptional,
            T min,
            T max,
            String typeName,
            Function<String, T> parseFunction) {
        if (!configuration.toMap().containsKey(key)) {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        } else {
            final String value = configuration.toMap().get(key);
            try {
                final T parsed = parseFunction.apply(value);
                if (parsed.compareTo(min) < 0 || parsed.compareTo(max) > 0) {
                    throw new ValidationException(
                            "Property '"
                                    + key
                                    + "' must be a "
                                    + typeName
                                    + " value between "
                                    + min
                                    + " and "
                                    + max
                                    + " but was: "
                                    + parsed);
                }
            } catch (Exception e) {
                throw new ValidationException(
                        "Property '"
                                + key
                                + "' must be a "
                                + typeName
                                + " value but was: "
                                + value);
            }
        }
    }

    private Optional<String> optionalGet(String key) {
        return Optional.ofNullable(configuration.toMap().get(key));
    }

    /** Returns a long value under the given key if it exists. */
    public Optional<Long> getOptionalLong(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Long.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid long value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a string value under the given key if it exists. */
    public Optional<String> getOptionalString(String key) {
        return optionalGet(key);
    }

    /** Returns a string value under the given existing key. */
    public String getString(String key) {
        return getOptionalString(key).orElseThrow(exceptionSupplier(key));
    }

    private Supplier<TableException> exceptionSupplier(String key) {
        return () -> {
            throw new TableException(
                    "Property with key '"
                            + key
                            + "' could not be found. "
                            + "This is a bug because the validation logic should have checked that before.");
        };
    }

    /** Returns an integer value under the given key if it exists. */
    public Optional<Integer> getOptionalInt(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Integer.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid integer value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns an integer value under the given existing key. */
    public int getInt(String key) {
        return getOptionalInt(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns the properties as a map copy with a prefix key. */
    public Map<String, String> asPrefixedMap(String prefix) {
        return configuration.toMap().entrySet().stream()
                .collect(Collectors.toMap(e -> prefix + e.getKey(), Map.Entry::getValue));
    }

    /** Returns a new properties instance with the given keys removed. */
    public Configuration withoutKeys(List<String> keys) {
        final Set<String> keySet = new HashSet<>(keys);
        final Map copyMap = new HashMap();
        configuration.toMap().entrySet().stream()
                .filter(e -> !keySet.contains(e.getKey()))
                .forEach(e -> copyMap.put(e.getKey(), e.getValue()));
        return Configuration.fromMap(copyMap);
    }

    /**
     * Validates a Java {@link Duration}.
     *
     * <p>The precision defines the allowed minimum unit in milliseconds (e.g. 1000 would only allow
     * seconds).
     */
    public void validateDuration(String key, boolean isOptional, int precision) {
        validateDuration(key, isOptional, precision, 0L, Long.MAX_VALUE);
    }

    /**
     * Validates a Java {@link Duration}. The boundaries are inclusive and in milliseconds.
     *
     * <p>The precision defines the allowed minimum unit in milliseconds (e.g. 1000 would only allow
     * seconds).
     */
    public void validateDuration(String key, boolean isOptional, int precision, long min) {
        validateDuration(key, isOptional, precision, min, Long.MAX_VALUE);
    }

    /**
     * Validates a Java {@link Duration}. The boundaries are inclusive and in milliseconds.
     *
     * <p>The precision defines the allowed minimum unit in milliseconds (e.g. 1000 would only allow
     * seconds).
     */
    public void validateDuration(
            String key, boolean isOptional, int precision, long min, long max) {
        Preconditions.checkArgument(precision > 0);

        validateComparable(
                key,
                isOptional,
                min,
                max,
                "time interval (in milliseconds)",
                (value) -> {
                    final long ms = TimeUtils.parseDuration(value).toMillis();
                    if (ms % precision != 0) {
                        throw new ValidationException(
                                "Duration for key '"
                                        + key
                                        + "' must be a multiple of "
                                        + precision
                                        + " milliseconds but was: "
                                        + value);
                    }
                    return ms;
                });
    }

    /** Returns an empty validation logic. */
    public static Consumer<String> noValidation() {
        return EMPTY_CONSUMER;
    }

    /** Validates an enum property with a set of enum values. */
    public void validateEnumValues(String key, boolean isOptional, List<String> values) {
        validateEnum(
                key,
                isOptional,
                values.stream().collect(Collectors.toMap(v -> v, v -> noValidation())));
    }

    /** Validates an enum property with a set of validation logic for each enum value. */
    public void validateEnum(
            String key, boolean isOptional, Map<String, Consumer<String>> enumValidation) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    if (!enumValidation.containsKey(value)) {
                        throw new ValidationException(
                                "Unknown value for property '"
                                        + key
                                        + "'.\n"
                                        + "Supported values are "
                                        + enumValidation.keySet()
                                        + " but was: "
                                        + value);
                    } else {
                        // run validation logic
                        enumValidation.get(value).accept(key);
                    }
                });
    }

    /** Returns all array elements under a given existing key. */
    public <E> List<E> getArray(String key, Function<String, E> keyMapper) {
        return getOptionalArray(key, keyMapper).orElseThrow(exceptionSupplier(key));
    }

    /**
     * Returns all array elements under a given key if it exists.
     *
     * <p>For example:
     *
     * <pre>
     *     primary-key.0 = field1
     *     primary-key.1 = field2
     * </pre>
     *
     * <p>leads to: List(field1, field2)
     *
     * <p>or:
     *
     * <pre>
     *     primary-key = field1
     * </pre>
     *
     * <p>leads to: List(field1)
     *
     * <p>The key mapper gets the key of the current value e.g. "primary-key.1".
     */
    public <E> Optional<List<E>> getOptionalArray(String key, Function<String, E> keyMapper) {
        // determine max index
        final int maxIndex = extractMaxIndex(key, "");

        if (maxIndex < 0) {
            // check for a single element array
            if (containsKey(key)) {
                return Optional.of(Collections.singletonList(keyMapper.apply(key)));
            } else {
                return Optional.empty();
            }
        } else {
            final List<E> list = new ArrayList<>();
            for (int i = 0; i < maxIndex + 1; i++) {
                final String fullKey = key + '.' + i;
                final E value = keyMapper.apply(fullKey);
                list.add(value);
            }
            return Optional.of(list);
        }
    }

    /** Returns if the given key is contained. */
    public boolean containsKey(String key) {
        return configuration.toMap().containsKey(key);
    }

    private int extractMaxIndex(String key, String suffixPattern) {
        // extract index and property keys
        final String escapedKey = Pattern.quote(key);
        final Pattern pattern = Pattern.compile(escapedKey + "\\.(\\d+)" + suffixPattern);
        final IntStream indexes =
                configuration.toMap().keySet().stream()
                        .flatMapToInt(
                                k -> {
                                    final Matcher matcher = pattern.matcher(k);
                                    if (matcher.find()) {
                                        return IntStream.of(Integer.valueOf(matcher.group(1)));
                                    }
                                    return IntStream.empty();
                                });

        // determine max index
        return indexes.max().orElse(-1);
    }

    /**
     * Validates an array of values.
     *
     * <p>For example:
     *
     * <pre>
     *     primary-key.0 = field1
     *     primary-key.1 = field2
     * </pre>
     *
     * <p>leads to: List(field1, field2)
     *
     * <p>or:
     *
     * <pre>
     *     primary-key = field1
     * </pre>
     *
     * <p>The validation consumer gets the key of the current value e.g. "primary-key.1".
     */
    public void validateArray(String key, Consumer<String> elementValidation, int minLength) {
        validateArray(key, elementValidation, minLength, Integer.MAX_VALUE);
    }

    /**
     * Validates an array of values.
     *
     * <p>For example:
     *
     * <pre>
     *     primary-key.0 = field1
     *     primary-key.1 = field2
     * </pre>
     *
     * <p>leads to: List(field1, field2)
     *
     * <p>or:
     *
     * <pre>
     *     primary-key = field1
     * </pre>
     *
     * <p>The validation consumer gets the key of the current value e.g. "primary-key.1".
     */
    public void validateArray(
            String key, Consumer<String> elementValidation, int minLength, int maxLength) {

        // determine max index
        final int maxIndex = extractMaxIndex(key, "");

        if (maxIndex < 0) {
            // check for a single element array
            if (configuration.toMap().containsKey(key)) {
                elementValidation.accept(key);
            } else if (minLength > 0) {
                throw new ValidationException(
                        "Could not find required property array for key '" + key + "'.");
            }
        } else {
            // do not allow a single element array
            if (configuration.toMap().containsKey(key)) {
                throw new ValidationException("Invalid property array for key '" + key + "'.");
            }

            final int size = maxIndex + 1;
            if (size < minLength) {
                throw new ValidationException(
                        "Array for key '"
                                + key
                                + "' must not have less than "
                                + minLength
                                + " elements but was: "
                                + size);
            }

            if (size > maxLength) {
                throw new ValidationException(
                        "Array for key '"
                                + key
                                + "' must not have more than "
                                + maxLength
                                + " elements but was: "
                                + size);
            }
        }

        // validate array elements
        for (int i = 0; i <= maxIndex; i++) {
            final String fullKey = key + '.' + i;
            if (configuration.toMap().containsKey(fullKey)) {
                // run validation logic
                elementValidation.accept(fullKey);
            } else {
                throw new ValidationException(
                        "Required array element at index '"
                                + i
                                + "' for key '"
                                + key
                                + "' is missing.");
            }
        }
    }

}
