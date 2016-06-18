package nio;

/**
 * Created by root on 17.06.16.
 */
public abstract class UpdateLogRunnable implements Runnable {
    private int updateFollowerId;

    public UpdateLogRunnable(int updateFollowerId) {
        this.updateFollowerId = updateFollowerId;
    }

    public UpdateLogRunnable(boolean isForAll) {
        this.updateFollowerId = -1;
    }

    public int getUpdateFollowerId() {
        return updateFollowerId;
    }

    public boolean isForAll() {
        return updateFollowerId == -1;
    }
}
