package io.millesabords.demo.streamingsql.producer;


import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.Random;


public class Log {

    private static Gson GSON = new Gson();
    private static Random RAND = new Random() {
        public int nextInt() {
            return next(Integer.SIZE - 1);
        }
    };

    @SerializedName("ID") private int id = RAND.nextInt(); // Not a very good idea ... :-(
    @SerializedName("TS") private long ts;
    @SerializedName("IP_ADDRESS") private String ipAddress;
    @SerializedName("URL") private String url;
    @SerializedName("STATUS") private int status;
    @SerializedName("NB_BYTES") private int nbBytes;

    public static void main(String[] a) {
        while (true) {
            int i = RAND.nextInt();
            if (i < 0) {
                System.err.println("BOOM");
            }
        }
    }

    public Log() {
    }

    public Log(long ts, String ipAddress, String url, int status, int nbBytes) {
        this.ts = ts;
        this.ipAddress = ipAddress;
        this.url = url;
        this.status = status;
        this.nbBytes = nbBytes;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getNbBytes() {
        return nbBytes;
    }

    public void setNbBytes(int nbBytes) {
        this.nbBytes = nbBytes;
    }

    public String toJson() {
        return GSON.toJson(this);
    }
}
