package com.mesosphere.sdk.specification;

/**
 * The allowed goal states for a Task. These are treated as the desired outcome for executing a given task.
 */
public enum GoalState {

    /**
     * Running continuously. Should be restarted if it exits, or if its configuration changes.
     */
    RUNNING(true, true),

    /**
     * Running and then exiting successfully. Will be restarted until it has successfully exited, and will be run again
     * if the service configuration changes.
     */
    FINISH(false, true),

    /**
     * Running only once over the lifetime of a service. After it has successfully exited, it will not be run again for
     * the lifespan of the service, regardless of any changes to service configuration.
     */
    ONCE(false, false);

    private final boolean shouldRunContinuously;
    private final boolean relaunchOnConfigChange;

    private GoalState(boolean shouldRunContinuously, boolean relaunchOnConfigChange) {
        this.shouldRunContinuously = shouldRunContinuously;
        this.relaunchOnConfigChange = relaunchOnConfigChange;
    }

    /**
     * Returns whether this task should be kept running ({@code true}), or if it should be considered completed once it
     * has exited successfully ({@code false}).
     */
    public boolean shouldRunContinuously() {
        return shouldRunContinuously;
    }

    /**
     * Returns whether the task should be relaunched after the service configuration has changed.
     */
    public boolean shouldRelaunchOnConfigChange() {
        return relaunchOnConfigChange;
    }
}
