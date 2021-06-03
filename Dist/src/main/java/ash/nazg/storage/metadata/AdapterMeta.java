package ash.nazg.storage.metadata;

import ash.nazg.config.tdl.metadata.DefinitionMeta;

import java.util.Map;

public class AdapterMeta {
    public final String name;
    public final String description;

    public final String pattern;

    public final Map<String, DefinitionMeta> settings;

    public AdapterMeta(String name, String description, String pattern, Map<String, DefinitionMeta> settings) {
        this.name = name;
        this.description = description;

        this.pattern = pattern;

        this.settings = settings;
    }
}
