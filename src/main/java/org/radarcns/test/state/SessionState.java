package org.radarcns.test.state;

/**
 * Implement the session state of a web client
 * Created by Francesco Nobilia on 27/09/2016.
 */
public class SessionState {
    private long lastConnection;
    private int sessionId;

    public SessionState(long lastConnection, int sessionId) {
        this.lastConnection = lastConnection;
        this.sessionId = sessionId;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public long getLastConnection() {
        return lastConnection;
    }

    public void setLastConnection(long lastConnection) {
        this.lastConnection = lastConnection;
    }
}
