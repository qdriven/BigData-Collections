package io.qkits.sparklivy.livy;

import org.apache.livy.sessions.SessionState;

import java.util.Map;

import static io.qkits.sparklivy.livy.LivySessionStates.State.*;

//from griffin

public class LivySessionStates {

    /**
     * UNKNOWN is used to represent the state that server get null from Livy.
     * the other state is just same as com.cloudera.livy.sessions.SessionState.
     */
    public enum State {
        NOT_STARTED,
        STARTING,
        RECOVERING,
        IDLE,
        RUNNING,
        BUSY,
        SHUTTING_DOWN,
        ERROR,
        DEAD,
        SUCCESS,
        UNKNOWN,
        STOPPED,
        FINDING,
        NOT_FOUND,
        FOUND
    }

    public static SessionState toSessionState(State state) {
        if (state == null) {
            return null;
        }
        return SessionState.apply(state.name());
    }

    public static State toLivyState(Map<String, Object> object) {
        if (object != null) {
            if(object.get("state")!=null) {
                String state = object.get("state").toString();
                return parseState(state);
            }
        }
        return UNKNOWN;
    }

    private static State parseState(String state) {
        if (state == null) {
            return null;
        }
        switch (state) {
            case "NEW":
            case "NEW_SAVING":
            case "SUBMITTED":
                return NOT_STARTED;
            case "ACCEPTED":
                return STARTING;
            case "RUNNING":
                return RUNNING;
            case "SUCCEEDED":
                return SUCCESS;
            case "FAILED":
                return DEAD;
            case "KILLED":
                return SHUTTING_DOWN;
            case "FINISHED":
                return null;
            default:
                return UNKNOWN;
        }
    }

    public static boolean isCompleted(String state){
        switch (state.toUpperCase()) {
            case "NEW":
            case "NEW_SAVING":
            case "SUBMITTED":
            case "BUSY":
            case "IDLE":
            case "RECOVERING":
                return false;
            case "ACCEPTED":
                return false;
            case "RUNNING":
                return false;
            case "SUCCEEDED":
            case "ShUTTINGDOWN":
                return true;
            case "FAILED":
                return true;
            case "KILLED":
                return true;
            case "FINISHED":
                return true;
            default:
                return true;
        }
    }


    public static boolean isActive(String state){
        return isActive(parseState(state));
    }
    public static boolean isActive(State state) {
        if (UNKNOWN.equals(state) || STOPPED.equals(state) || NOT_FOUND.equals
                (state) || FOUND.equals(state)) {
            // set UNKNOWN isActive() as false.
            return false;
        } else if (FINDING.equals(state)) {
            return true;
        }
        SessionState sessionState = toSessionState(state);
        return sessionState != null && sessionState.isActive();
    }

    public static String convert2QuartzState(State state) {
        SessionState sessionState = toSessionState(state);
        if (STOPPED.equals(state) || SUCCESS.equals(state)) {
            return "COMPLETE";
        }
        if (UNKNOWN.equals(state) || NOT_FOUND.equals(state)
                || FOUND.equals(state) || sessionState == null
                || !sessionState.isActive()) {
            return "ERROR";
        }
        return "NORMAL";

    }

    public static boolean isHealthy(State state) {
        return !(State.ERROR.equals(state) || State.DEAD.equals(state)
                || State.SHUTTING_DOWN.equals(state)
                || State.FINDING.equals(state)
                || State.NOT_FOUND.equals(state)
                || State.FOUND.equals(state));
    }
}
