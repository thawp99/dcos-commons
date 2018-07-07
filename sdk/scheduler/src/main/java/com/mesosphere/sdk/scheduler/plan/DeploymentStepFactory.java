package com.mesosphere.sdk.scheduler.plan;

import com.google.common.annotations.VisibleForTesting;
import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.offer.TaskUtils;
import com.mesosphere.sdk.offer.taskdata.TaskLabelReader;
import com.mesosphere.sdk.scheduler.recovery.FailureUtils;
import com.mesosphere.sdk.specification.GoalState;
import com.mesosphere.sdk.specification.PodInstance;
import com.mesosphere.sdk.specification.TaskSpec;
import com.mesosphere.sdk.state.ConfigStoreException;
import com.mesosphere.sdk.state.ConfigTargetStore;
import com.mesosphere.sdk.state.StateStore;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class is a default implementation of the {@link StepFactory} interface.
 */
public class DeploymentStepFactory {
    private static final Logger LOGGER = LoggingUtils.getLogger(DeploymentStepFactory.class);

    private final ConfigTargetStore configTargetStore;
    private final StateStore stateStore;
    private final Optional<String> namespace;

    public DeploymentStepFactory(
            ConfigTargetStore configTargetStore,
            StateStore stateStore,
            Optional<String> namespace) {
        this.configTargetStore = configTargetStore;
        this.stateStore = stateStore;
        this.namespace = namespace;
    }

    /**
     * Returns a new {@link DeploymentStep} which is configured to launch the specified {@code tasksToLaunch} within the
     * specified {@code podInstance}.
     */
    public Step getStep(PodInstance podInstance, Collection<String> tasksToLaunch) {
        try {
            LOGGER.info("Generating step for pod: {}, with tasks: {}", podInstance.getName(), tasksToLaunch);
            validate(podInstance, tasksToLaunch);

            List<Protos.TaskInfo> taskInfos = TaskUtils.getTaskNames(podInstance, tasksToLaunch).stream()
                    .map(taskName -> stateStore.fetchTask(taskName))
                    .filter(taskInfoOptional -> taskInfoOptional.isPresent())
                    .map(taskInfoOptional -> taskInfoOptional.get())
                    .collect(Collectors.toList());

            return new DeploymentStep(
                    TaskUtils.getStepName(podInstance, tasksToLaunch),
                    PodInstanceRequirement.newBuilder(podInstance, tasksToLaunch).build(),
                    stateStore,
                    namespace)
                    .updateInitialStatus(taskInfos.isEmpty() ? Status.PENDING : getStepStatus(podInstance, taskInfos));
        } catch (Exception e) {
            LOGGER.error("Failed to generate Step with exception: ", e);
            return new DeploymentStep(
                    podInstance.getName(),
                    PodInstanceRequirement.newBuilder(podInstance, Collections.emptyList()).build(),
                    stateStore,
                    namespace)
                    .addError(ExceptionUtils.getStackTrace(e));
        }
    }

    private void validate(PodInstance podInstance, Collection<String> tasksToLaunch) throws Exception {
        List<TaskSpec> taskSpecsToLaunch = podInstance.getPod().getTasks().stream()
                .filter(taskSpec -> tasksToLaunch.contains(taskSpec.getName()))
                .collect(Collectors.toList());

        List<String> resourceSetIds = taskSpecsToLaunch.stream()
                .map(taskSpec -> taskSpec.getResourceSet().getId())
                .collect(Collectors.toList());

        if (hasDuplicates(resourceSetIds)) {
            throw new Exception(String.format(
                    "Attempted to simultaneously launch tasks: %s in pod: %s using the same resource set id: %s. " +
                            "These tasks should either be run in separate steps or use different resource set ids",
                    tasksToLaunch, podInstance.getName(), resourceSetIds));
        }

        List<String> dnsPrefixes = taskSpecsToLaunch.stream()
                .map(taskSpec -> taskSpec.getDiscovery())
                .filter(discovery -> discovery.isPresent())
                .map(discovery -> discovery.get().getPrefix())
                .filter(prefix -> prefix.isPresent())
                .map(prefix -> prefix.get())
                .collect(Collectors.toList());

        if (hasDuplicates(dnsPrefixes)) {
            throw new Exception(String.format(
                    "Attempted to simultaneously launch tasks: %s in pod: %s using the same DNS name: %s. " +
                            "These tasks should either be run in separate steps or use different DNS names",
                    tasksToLaunch, podInstance.getName(), dnsPrefixes));
        }
    }

    private static <T> boolean hasDuplicates(Collection<T> collection) {
        return new HashSet<T>(collection).size() < collection.size();
    }

    private Status getStepStatus(PodInstance podInstance, List<Protos.TaskInfo> taskInfos)
            throws ConfigStoreException, TaskException {

        List<Status> taskStatuses = new ArrayList<>();
        UUID targetConfigId = configTargetStore.getTargetConfig();
        for (Protos.TaskInfo taskInfo : taskInfos) {
            taskStatuses.add(getStatus(podInstance, taskInfo, targetConfigId));
        }

        for (Status status : taskStatuses) {
            if (!status.equals(Status.COMPLETE)) {
                return Status.PENDING;
            }
        }

        return Status.COMPLETE;
    }

    private Status getStatus(PodInstance podInstance, Protos.TaskInfo taskInfo, UUID targetConfigId)
            throws TaskException {
        GoalState goalState = TaskUtils.getGoalState(podInstance, taskInfo.getName());
        boolean hasReachedGoal = hasReachedGoalState(taskInfo, goalState);

        boolean isOnTarget;
        if (hasReachedGoal && !goalState.shouldRelaunchOnConfigChange()) {
            LOGGER.info("Automatically on target configuration due to having reached {} goal.", goalState);
            isOnTarget = true;
        } else {
            isOnTarget = new TaskLabelReader(taskInfo).getTargetConfiguration().equals(targetConfigId);
        }

        boolean hasPermanentlyFailed = FailureUtils.isPermanentlyFailed(taskInfo);

        String bitsLog = String.format("onTarget=%s reachedGoal=%s permanentlyFailed=%s",
                isOnTarget, hasReachedGoal, hasPermanentlyFailed);
        if ((isOnTarget && hasReachedGoal) || hasPermanentlyFailed) {
            LOGGER.info("Deployment of task '{}' is COMPLETE: {}", taskInfo.getName(), bitsLog);
            return Status.COMPLETE;
        } else {
            LOGGER.info("Deployment of task '{}' is PENDING: {}", taskInfo.getName(), bitsLog);
            return Status.PENDING;
        }
    }

    @VisibleForTesting
    protected boolean hasReachedGoalState(Protos.TaskInfo taskInfo, GoalState goalState) throws TaskException {
        Optional<Protos.TaskStatus> statusOptional = stateStore.fetchStatus(taskInfo.getName());
        if (!statusOptional.isPresent()) {
            return false;
        }
        Protos.TaskStatus status = statusOptional.get();
        if (!goalState.shouldRunContinuously()) {
            // The goal state is for the task to have finished running. Did it do that?:
            return status.getState().equals(Protos.TaskState.TASK_FINISHED);
        } else if (status.getState().equals(Protos.TaskState.TASK_RUNNING)) {
            // The goal state is for the task to be running (and to have passed any readiness check). Did it do that?
            return new TaskLabelReader(taskInfo).isReadinessCheckSucceeded(status);
        } else {
            return false;
        }
    }
}
