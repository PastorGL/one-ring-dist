/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.metadata;

import java.util.Map;

public class AdapterMeta {
    public final String name;
    public final String descr;

    public final String pattern;

    public final Map<String, DefinitionMeta> settings;

    public AdapterMeta(String name, String descr, String pattern, Map<String, DefinitionMeta> settings) {
        this.name = name;
        this.descr = descr;

        this.pattern = pattern;

        this.settings = settings;
    }
}
