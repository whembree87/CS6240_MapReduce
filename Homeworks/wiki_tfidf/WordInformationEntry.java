package wiki_tfidf;

public class WordInformationEntry {
    public String id;
    public Float tf;
    public Integer freq;

    public WordInformationEntry(String id, Float tf, Integer freq) {
        this.id = id;
        this.tf = tf;
        this.freq = freq;
    }
}
