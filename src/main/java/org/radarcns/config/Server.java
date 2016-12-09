package org.radarcns.config;

/**
 * POJO representing a Server configuration
 */
public class Server {

    private String host;
    private Integer port;
    private String protocol;

    public Server() {}

    public Server(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    public Server(String host, Integer port, String protocol) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getPath() {
        if (protocol != null) {
            return protocol +"://"+ host + ":" + port;
        } else {
            return host + ":" + port;
        }
    }

    public String info(){
        String result = host + ":" + port;
        return (protocol == null || protocol.isEmpty()) ? result : protocol + "://" + result;
    }

    @Override
    public String toString() {
        if(protocol == null || protocol.isEmpty()){
            return "Server{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }

        return "Server{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", protocol='" + protocol + '\'' +
                '}';
    }
}
