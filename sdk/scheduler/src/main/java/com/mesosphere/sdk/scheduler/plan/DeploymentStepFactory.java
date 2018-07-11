package com.mesosphere.sdk.scheduler.plan;

import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.offer.TaskUtils;
import com.mesosphere.sdk.offer.taskdata.TaskLabelReader;
import com.mesosphere.sdk.scheduler.recovery.FailureUtils;
import com.mesosphere.sdk.specification.GoalState;
import com.mesosphere.sdk.specification.PodInstance;
import com.mesosphere.sdk.specification.TaskSpec;
import com.mesosphere.sdk.state.ConfigTargetStore;
import com.mesosphere.sdk.state.GoalStateOverride;
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

    public Step getFootprintStep(PodInstance podInstance) {
        DeploymentStep step = new FootprintStep(podInstance, stateStore, namespace);
        try {
            LOGGER.info("Generating footprint step for pod: {}", podInstance.getName());
            boolean stepComplete = isFootprintAcquired(stateStore, podInstance, configTargetStore.getTargetConfig());
            return step.updateInitialStatus(stepComplete ? Status.COMPLETE : Status.PENDING);
        } catch (Exception e) {
            LOGGER.error("Failed to generate FootprintStep", e);
            return step.addError(ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * Returns a new {@link DeploymentStep} which is configured to launch the specified {@code tasksToLaunch} within the
     * specified {@code podInstance}.
     */
    public Step getLaunchStep(PodInstance podInstance, Collection<String> tasksToLaunch) {
        DeploymentStep step = new LaunchStep(
                TaskUtils.getStepName(podInstance, tasksToLaunch),
                PodInstanceRequirement.newBuilder(podInstance, tasksToLaunch).build(),
                stateStore,
                namespace);
        try {
            LOGGER.info("Generating launch step for pod: {}, with tasks: {}", podInstance.getName(), tasksToLaunch);
            validateLaunch(podInstance, tasksToLaunch);
            boolean stepComplete =
                    isDeploymentComplete(stateStore, podInstance, configTargetStore.getTargetConfig(), tasksToLaunch);
            return step.updateInitialStatus(stepComplete ? Status.COMPLETE : Status.PENDING);
        } catch (Exception e) {
            LOGGER.error("Failed to generate LaunchStep", e);
            return step.addError(ExceptionUtils.getStackTrace(e));
        }
    }

    private static void validateLaunch(PodInstance podInstance, Collection<String> tasksToLaunch) throws Exception {
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
                .filter(taskSpec -> taskSpec.getDiscovery().isPresent()
                        && taskSpec.getDiscovery().get().getPrefix().isPresent())
                .map(taskSpec -> taskSpec.getDiscovery().get().getPrefix().get())
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

    private static boolean isFootprintAcquired(StateStore stateStore, PodInstance podInstance, UUID targetConfigId)
            throws TaskException {
        for (String taskName : TaskUtils.getTaskNames(podInstance)) {
            GoalStateOverride.Status goalOverrideStatus = stateStore.fetchGoalOverrideStatus(taskName);
            if (goalOverrideStatus.target.equals(GoalStateOverride.FOOTPRINT)
                    && !goalOverrideStatus.progress.equals(GoalStateOverride.Progress.COMPLETE)) {
                // Task is pending on an existing footprint operation -- resume footprint operation
                return false;
            }

            Optional<Protos.TaskInfo> taskInfoOptional = stateStore.fetchTask(taskName);
            if (!taskInfoOptional.isPresent()) {
                // Task is not present -- needs initial footprint
                return false;
            }
            if (!new TaskLabelReader(taskInfoOptional.get()).getTargetConfiguration().equals(targetConfigId)) {
                // Task configuration does not match target configuration -- needs footprint update
                return false;
            }
        }

        // All tasks in the pod have a configuration matching the target, and none of them are resuming an ongoing
        // footprint operation.
        return true;
    }

    private static boolean isDeploymentComplete(
            StateStore stateStore, PodInstance podInstance, UUID targetConfigId, Collection<String> tasksToLaunch)
            throws TaskException {
        if (tasksToLaunch.isEmpty()) {
            return false;
        }
        for (String taskName : TaskUtils.getTaskNames(podInstance, tasksToLaunch)) {
            if (!isTaskDeployed(stateStore, podInstance, targetConfigId, taskName)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTaskDeployed(
            StateStore stateStore, PodInstance podInstance, UUID targetConfigId, String taskName) throws TaskException {
        Optional<Protos.TaskInfo> taskInfoOptional = stateStore.fetchTask(taskName);
        if (!taskInfoOptional.isPresent()) {
            return false;
        }
        Protos.TaskInfo taskInfo = taskInfoOptional.get();
        GoalState goalState = TaskUtils.getGoalState(podInstance, taskInfoOptional.get().getName());

        boolean hasReachedGoal = hasReachedGoalState(goalState, taskInfo, stateStore.fetchStatus(taskName));
        boolean isOnTarget;
        if (hasReachedGoal && !goalState.shouldRelaunchOnConfigChange()) {
            // The task has a goal state of ONCE: If the task EVER successfully finished running, don't launch it again,
            // regardless of any changes to target configuration. Pretend that it's already on the target configuration.
            LOGGER.info("Automatically on target configuration due to having reached {} goal", goalState);
            isOnTarget = true;
        } else {
            isOnTarget = new TaskLabelReader(taskInfo).getTargetConfiguration().equals(targetConfigId);
        }

        // If the task has permanently failed, it's being handled by the recovery plan.
        // Leave it alone for deploy plan purposes.
        boolean hasPermanentlyFailed = FailureUtils.isPermanentlyFailed(taskInfo);

        String bitsLog = String.format("onConfigTarget=%s reachedGoalState=%s permanentlyFailed=%s",
                isOnTarget, hasReachedGoal, hasPermanentlyFailed);
        if ((isOnTarget && hasReachedGoal) || hasPermanentlyFailed) {
            LOGGER.info("Deployment of task '{}' is COMPLETE: {}", taskInfo.getName(), bitsLog);
            return true;
        } else {
            LOGGER.info("Deployment of task '{}' is PENDING: {}", taskInfo.getName(), bitsLog);
            return false;
        }
    }

    private static boolean hasReachedGoalState(
            GoalState goalState, Protos.TaskInfo taskInfo, Optional<Protos.TaskStatus> statusOptional) {
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
