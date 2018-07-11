package com.mesosphere.sdk.scheduler.plan;

import com.mesosphere.sdk.state.StateStore;

import java.util.*;

/**
 * Step which implements the deployment of a pod.
 */
public class LaunchStep extends DeploymentStep {

    private final Map<String, String> environment = new HashMap<>();

    /**
     * Creates a new instance with the provided {@code name}, initial {@code status}, associated pod instance required
     * by the step, and any {@code errors} to be displayed to the user.
     */
    public LaunchStep(
            String name,
            PodInstanceRequirement podInstanceRequirement,
            StateStore stateStore,
            Optional<String> namespace) {
        super(name, podInstanceRequirement, stateStore, namespace);
    }

    public void setEnvironment(Map<String, String> environment) {
        this.environment.clear();
        this.environment.putAll(environment);
    }

    @Override
    public Optional<PodInstanceRequirement> getPodInstanceRequirement() {
        return Optional.of(PodInstanceRequirement.newBuilder(podInstanceRequirement)
                .environment(environment)
                .build());
    }
}
