package nio.model;

/**
 * Created by root on 16.06.16.
 */
public class SetCommand extends StateMachineCommand {
    private String key;
    private String value;

    public SetCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    protected boolean justApply(StateMachine stateMachine) {
        stateMachine.set(key, value);
        return true;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
