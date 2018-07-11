package com.mesosphere.sdk.scheduler.plan;

import com.mesosphere.sdk.offer.TaskUtils;
import com.mesosphere.sdk.specification.PodInstance;
import com.mesosphere.sdk.state.StateStore;

import java.util.*;

/**
 * Step which implements the deployment of a pod.
 */
public class FootprintStep extends DeploymentStep {

    /**
     * Creates a new instance with the provided {@code name}, initial {@code status}, associated pod instance required
     * by the step, and any {@code errors} to be displayed to the user.
     */
    public FootprintStep(PodInstance podInstance, StateStore stateStore, Optional<String> namespace) {
        super(
                TaskUtils.getStepName(podInstance, Collections.singleton("footprint")),
                PodInstanceRequirement.newBuilder(podInstance, Collections.emptyList()).build(),
                stateStore,
                namespace);
    }

    @Override
    public Optional<PodInstanceRequirement> getPodInstanceRequirement() {
        return Optional.of(
                PodInstanceRequirement.newBuilder(podInstanceRequirement)
                        .build());
    }
}
