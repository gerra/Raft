package nio.model;

/**
 * Created by root on 16.06.16.
 */
public class DeleteCommand extends StateMachineCommand {
    private String key;

    public DeleteCommand(String key) {
        this.key = key;
    }

    @Override
    protected boolean justApply(StateMachine stateMachine) {
        return stateMachine.delete(key);
    }

    public String getKey() {
        return key;
    }
}
