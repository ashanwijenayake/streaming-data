package dto;

/**
 * The following method contains the payload schema
 */
public class PayLoad {

    private String tweet;
    private String language;
    private String createdAt;
    private int sentiment;
    private String place;
    private String location;

    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "{" +
                "tweet:'" + tweet + '\'' +
                ", language:'" + language + '\'' +
                ", createdAt:'" + createdAt + '\'' +
                ", sentiment:" + sentiment +
                ", place:'" + place + '\'' +
                ", location:'" + location + '\'' +
                '}';
    }
}
