package com.mesosphere.sdk.scheduler.plan;

/**
 * Status of an {@link Element}.  The Status indicates to {@link PlanManager}s how to handle {@link Plan} elements.
 *
 * For a {@link Step} the normal progression of states is:
 * <ol>
 * <li>RESERVING: found a suitable place to create footprint, waiting for outcome</li>
 * <li>RESERVED: footprint has been obtained, waiting for plan strategy to launch the task</li>
 * <li>PENDING: initial state after obtaining footprint. waiting for plan strategy to </li>
 * <li>PREPARED: step is now actively searching for the matching footprint to launch the task</li>
 * <li>STARTING: found the matching footprint, waiting for launch operations to occur</li>
 * <li>STARTED: waiting for any readiness checks to pass, if any are defined</li>
 * <li>COMPLETE: operation has completed</li>
 * </ol>
 *
 * When something goes wrong, and a retry is desirable returning to PENDING from PREPARED or STARTING indicates that an
 * {@link Element} should be restarted.  COMPLETE is a terminal state which should not be changed, once it is reached
 * for any given processing of an {@link Element}.
 *
 * {@code ERROR} indicates that the initial construction or validation of the {@link Element} failed. This normally
 * occurs when an invalidation configuration is attempted. For example, changing a volume's type in a Pod is generally
 * not a legal operation, so a {@link Plan} attempting to accomplish this work would end up in an {@code ERROR} state.
 *
 * {@code WAITING} is a special state only used to show propagating effect of elements that were manually interrupted by
 * the operator. It indicates that the current {@link Element} is interrupted, or at least one of its children is
 * interrupted.
 */
public enum Status {

    /**
     * Execution experienced an error.
     */
    ERROR,

    /**
     * Execution has been manually interrupted by the operator.
     */
    WAITING,

    /**
     * Searching for footprint in the cluster. This only applies to steps which define resources to be reserved.
     */
    RESERVING,

    /**
     * Footprint has been acquired in the cluster. This only applies to steps which define resources to be reserved.
     */
    RESERVED,

    /**
     * Initial plan state (after any reservations have been completed), waiting for plan strategy to mark the step
     * active so that it can perform its work.
     */
    PENDING,

    /**
     * The Element has been evaluated, and any Tasks relevant to it have been killed if necessary.
     */
    PREPARED,

    /**
     * Execution has returned {@link com.mesosphere.sdk.offer.OfferRecommendation}s to be performed and is waiting to
     * determine the outcome of those Operations.
     */
    STARTING,

    /**
     * Execution has performed {@link com.mesosphere.sdk.offer.OfferRecommendation}s and has received feedback, but not
     * all success requirements (e.g. readiness checks) have been satisfied to consider the operation {@code COMPLETE}.
     */
    STARTED,

    /**
     * Execution has completed.
     */
    COMPLETE,

    /**
     * Only returned by {@link ParentElement}s to state that at least one child is complete and at least one child is in
     * progress, e.g. {@code PENDING} or {@code PREPARED}.
     */
    IN_PROGRESS;

    /**
     * Status is in one of the running states.
     */
    public boolean isRunning() {
        switch (this) {
        case PREPARED:
        case STARTING:
        case STARTED:
        case IN_PROGRESS:
            return true;
        default:
            return false;
        }
    }
}
