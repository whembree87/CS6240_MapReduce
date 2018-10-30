package wiki_tfidf;

import java.util.ArrayList;
import java.util.Collections;

public class WordInformationUpdater implements Comparable<WordInformationUpdater> {
    private ArrayList<WordInformationEntry> entries = new ArrayList<WordInformationEntry>();

    public void reset() {
        entries.clear();
    }

    public void add(WordInformation info) {
        Collections.addAll(entries, info.getEntries());
    }

    public void update(WordInformation info) {
        WordInformationEntry[] entriesArray = entries.toArray(new WordInformationEntry[entries.size()]);
        info.set(entriesArray);
    }

    public int compareTo(WordInformationUpdater o) {
        Integer s1 = entries.size();
        Integer s2 = o.entries.size();
        return s1.compareTo(s2);
    }

    @Override
    public String toString() {
        return String.format("WikiWordInfoUpdater[size=%1$d]", entries.size());
    }
}
