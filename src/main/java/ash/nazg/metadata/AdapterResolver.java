/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.metadata;

import ash.nazg.dist.InvalidConfigurationException;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Map;

public class AdapterResolver {
    private final Map<String, Object> holder;
    private final Map<String, DefinitionMeta> definitions;
    private final String descr;

    public AdapterResolver(AdapterMeta definitions, Map<String, Object> params) {
        this.definitions = definitions.settings;
        this.descr = definitions.descr;
        this.holder = params;
    }

    public <T> T get(String key) {
        DefinitionMeta defMeta = definitions.get(key);
        if (defMeta == null) {
            for (String def : definitions.keySet()) {
                if (key.startsWith(def)) {
                    defMeta = definitions.get(def);
                    break;
                }
            }
        }
        if (defMeta == null) {
            throw new InvalidConfigurationException("Invalid parameter " + key + " of " + descr);
        }

        Class<T> clazz;
        try {
            clazz = (Class<T>) Class.forName(defMeta.type);
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException("Cannot resolve class '" + defMeta.type + "' for parameter " + key + " of " + descr);
        }

        Object value = holder.get(key);
        if (value == null) {
            value = defMeta.defaults;
        }

        if (value == null) {
            return null;
        }

        if (Number.class.isAssignableFrom(clazz)) {
            if (value instanceof Number) {
                return (T) value;
            }
            try {
                Constructor c = clazz.getConstructor(String.class);
                return (T) c.newInstance(value);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Bad numeric value '" + value + "' for " + clazz.getSimpleName() + " parameter " + key + " of " + descr);
            }
        } else if (clazz.isEnum() && (value instanceof Enum)) {
            return (T) value;
        } else {
            String stringValue = String.valueOf(value);
            if (String.class == clazz) {
                return (T) stringValue;
            } else if (Boolean.class == clazz) {
                return (T) Boolean.valueOf(stringValue);
            } else if (clazz.isEnum()) {
                return (T) Enum.valueOf((Class) clazz, stringValue);
            } else if (String[].class == clazz) {
                if (value instanceof Object[]) {
                    return (T) Arrays.stream((Object[]) value).map(String::valueOf).toArray(String[]::new);
                }
                return (T) Arrays.stream(stringValue.split(",")).map(String::trim).toArray(String[]::new);
            }
        }

        throw new InvalidConfigurationException("Improper type '" + clazz.getName() + "' of a parameter " + key + " of " + descr);
    }

    public boolean containsKey(String key) {
        return holder.containsKey(key);
    }
}
