package nio.model;

/**
 * Created by root on 16.06.16.
 */
public abstract class StateMachineCommand {
    private Boolean result = null;

    public boolean apply(StateMachine stateMachine) {
        result = justApply(stateMachine);
        return result;
    }

    protected abstract boolean justApply(StateMachine stateMachine);

    public Boolean getResult() {
        return result;
    }
}
