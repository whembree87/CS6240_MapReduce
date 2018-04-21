package rucha_tfidf;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by rucha on 4/20/18.
 */
public class WikiWordInfoUpdater implements Comparable<WikiWordInfoUpdater> {
    private ArrayList<WikiWordInfoEntry> entries = new ArrayList<WikiWordInfoEntry>();

    public void reset() {
        entries.clear();
    }

    public void add(WikiWordInfo info) {
        Collections.addAll(entries, info.getEntries());
    }

    public void update(WikiWordInfo info) {
        WikiWordInfoEntry[] entriesArray = entries.toArray(new WikiWordInfoEntry[entries.size()]);
        info.set(entriesArray);
    }

    public int compareTo(WikiWordInfoUpdater o) {
        Integer s1 = entries.size();
        Integer s2 = o.entries.size();
        return s1.compareTo(s2);
    }

    @Override
    public String toString() {
        return String.format("WikiWordInfoUpdater[size=%1$d]", entries.size());
    }
}
