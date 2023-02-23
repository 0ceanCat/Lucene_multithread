package mytest;

public class Image {
    Integer id;
    String hashcode;
    String caption;
    Double mos;

    public Image(Integer id, String hashcode, String caption, Double mos) {
        this.id = id;
        this.hashcode = hashcode;
        this.caption = caption;
        this.mos = mos;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getHashcode() {
        return hashcode;
    }

    public void setHashcode(String hashcode) {
        this.hashcode = hashcode;
    }

    public String getCaption() {
        return caption;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    public Double getMos() {
        return mos;
    }

    public void setMos(Double mos) {
        this.mos = mos;
    }
}
