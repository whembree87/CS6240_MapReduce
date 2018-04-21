package rucha_tfidf;

/**
 * Created by rucha on 4/20/18.
 */
public class WikiWordInfoEntry {
    public String id;
    public Float tf;
    public Integer freq;

    public WikiWordInfoEntry(String id, Float tf, Integer freq) {
        this.id = id;
        this.tf = tf;
        this.freq = freq;
    }
}
