package com.mesosphere.sdk.specification;

import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.scheduler.plan.*;
import com.mesosphere.sdk.scheduler.plan.strategy.CanaryStrategy;
import com.mesosphere.sdk.scheduler.plan.strategy.DependencyStrategy;
import com.mesosphere.sdk.scheduler.plan.strategy.DependencyStrategyHelper;
import com.mesosphere.sdk.scheduler.plan.strategy.ParallelStrategy;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.scheduler.plan.strategy.Strategy;
import com.mesosphere.sdk.scheduler.plan.strategy.StrategyGenerator;
import com.mesosphere.sdk.specification.yaml.RawPhase;
import com.mesosphere.sdk.specification.yaml.RawPlan;
import com.mesosphere.sdk.specification.yaml.WriteOnceLinkedHashMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Generates {@link Plan}s as defined in a YAML plan specification.
 */
public class PlanGenerator {

    private static final String FOOTPRINT_PHASE_NAME = "footprint";
    private static final String DEFAULT_POD_INDEX_LABEL = "default";

    // Note: We avoid reusing strategies, because they keep internal state. Instead we only keep StrategyGenerators here
    private static final Map<String, StrategyGenerator<Step>> PHASE_STRATEGY_GENERATORS = new HashMap<>();
    private static final Map<String, StrategyGenerator<Phase>> PLAN_STRATEGY_GENERATORS = new HashMap<>();
    static {
        StrategyGenerator<Step> generator = new SerialStrategy.Generator<>();
        PHASE_STRATEGY_GENERATORS.put("serial", generator);
        PHASE_STRATEGY_GENERATORS.put("serial-canary", new CanaryStrategy.Generator(generator));
        PHASE_STRATEGY_GENERATORS.put("canary", new CanaryStrategy.Generator(generator));
        generator = new ParallelStrategy.Generator<>();
        PHASE_STRATEGY_GENERATORS.put("parallel", generator);
        PHASE_STRATEGY_GENERATORS.put("parallel-canary", new CanaryStrategy.Generator(generator));

        PLAN_STRATEGY_GENERATORS.put("parallel", new ParallelStrategy.Generator<>());
        PLAN_STRATEGY_GENERATORS.put("serial", new SerialStrategy.Generator<>());
    }
    private static final Set<String> PARALLEL_STRATEGY_TYPES =
            new HashSet<>(Arrays.asList("parallel", "parallel-canary"));

    private final DeploymentStepFactory deploymentStepFactory;

    public PlanGenerator(DeploymentStepFactory deploymentStepFactory) {
        this.deploymentStepFactory = deploymentStepFactory;
    }

    /**
     * Generates a default {@code deploy} plan based on the content of the provided pods. This creates a serial plan
     * where the pods are deployed in the order that they were declared.
     *
     * @param podSpecs the pods as defined in the service spec, for which a default deployment plan will be created
     * @return a deploy plan describing a default serial deployment in the order the pods were provided
     */
    public Plan generateDeployFromPods(Collection<PodSpec> podSpecs) {
        // TODO(nick) the deploymentStepFactory is able to tell whether steps are complete are not
        List<Phase> phases = podSpecs.stream()
                .map(podSpec -> new DefaultPhase(
                        podSpec.getType(),
                        generatePodSteps(deploymentStepFactory, podSpec),
                        new SerialStrategy<>(),
                        Collections.emptyList()))
                .collect(Collectors.toList());
        addFootprintPhase(podSpecs, phases);
        return new DefaultPlan(Constants.DEPLOY_PLAN_NAME, phases);
    }

    /**
     * Generates a custom plan based on the provided YAML {@link RawPlan} specification. The resulting output is
     * effectively a rendered version of that specification.
     *
     * @param rawPlan the YAML plan to be rendered
     * @param planName the name to use for the returned plan
     * @param podSpecs specifications of pods referenced by the plan
     * @return a new plan object representing the provided plan spec
     */
    public Plan generateFromYamlSpec(RawPlan rawPlan, String planName, Collection<PodSpec> podSpecs) {
        // TODO(nick) the deploymentStepFactory is able to tell whether steps are complete are not
        List<Phase> phases = rawPlan.getPhases().entrySet().stream()
                .map(entry-> generatePhase(deploymentStepFactory, entry.getValue(), entry.getKey(), podSpecs))
                .collect(Collectors.toList());
        if (planName.equals(Constants.DEPLOY_PLAN_NAME)) {
            addFootprintPhase(podSpecs, phases);
        }
        return new DefaultPlan(
                planName,
                phases,
                getPlanStrategyGenerator(rawPlan.getStrategy()).generate(phases),
                Collections.emptyList());
    }

    private static void addFootprintPhase(Collection<PodSpec> podSpecs, List<Phase> phases) {
        List<Step> footprintSteps = Collections.emptyList(); // TODO(nick) generate using podSpecs
        if (!footprintSteps.isEmpty()) {
            // Insert an initial footprint phase:
            phases.add(0, new DefaultPhase(
                    FOOTPRINT_PHASE_NAME,
                    footprintSteps,
                    new ParallelStrategy<>(),
                    Collections.emptyList()));
        }
    }

    private static List<Step> generatePodSteps(DeploymentStepFactory deploymentStepFactory, PodSpec podSpec) {
        List<Step> steps = new ArrayList<>();
        for (int i = 0; i < podSpec.getCount(); i++) {
            PodInstance podInstance = new DefaultPodInstance(podSpec, i);

            // By default, launch all tasks specified in the pod:
            List<String> tasksToLaunch = podInstance.getPod().getTasks().stream()
                    .map(taskSpec -> taskSpec.getName())
                    .collect(Collectors.toList());

            steps.add(deploymentStepFactory.getStep(podInstance, tasksToLaunch));
        }
        return steps;
    }

    private static Phase generatePhase(
            DeploymentStepFactory deploymentStepFactory,
            RawPhase rawPhase,
            String phaseName,
            Collection<PodSpec> podSpecs) {
        Optional<PodSpec> podSpecOptional = podSpecs.stream()
                .filter(podSpec -> podSpec.getType().equals(rawPhase.getPod()))
                .findFirst();
        if (!podSpecOptional.isPresent()) {
            throw new IllegalStateException(String.format(
                    "Unable to find pod '%s' referenced by phase '%s'", rawPhase.getPod(), phaseName));
        }
        PodSpec podSpec = podSpecOptional.get();

        if (rawPhase.getSteps() == null || rawPhase.getSteps().isEmpty()) {
            // No custom steps: Generate default behavior based on pod content
            return generatePhaseWithDefaultSteps(deploymentStepFactory, rawPhase.getStrategy(), phaseName, podSpec);
        }
        // Flatten map data: pod index (or 'default') to task deployment within that pod
        Map<String, List<List<String>>> podIndexToTasks = mapPodIndexesToTasks(phaseName, rawPhase.getSteps());

        if (PARALLEL_STRATEGY_TYPES.contains(rawPhase.getStrategy())) {
            // Custom steps with a parallel strategy: Custom dependencies are required
            return generatePhaseWithCustomParallelSteps(
                    deploymentStepFactory, rawPhase.getStrategy(), phaseName, podSpec, podIndexToTasks);
        } else {
            // Custom steps with a serial strategy: No custom dependencies needed
            return generatePhaseWithCustomSerialSteps(
                    deploymentStepFactory, rawPhase.getStrategy(), phaseName, podSpec, podIndexToTasks);
        }
    }

    /**
     * When no custom steps are defined, we use a default plan. Each pod instance will deploy its tasks in parallel with
     * one step per pod instance, and any cross-pod deployment determined by the configured phase strategy.
     */
    private static Phase generatePhaseWithDefaultSteps(
            DeploymentStepFactory deploymentStepFactory,
            String strategy,
            String phaseName,
            PodSpec podSpec) {
        // Shortcut: No custom steps are defined. By default, each pod instance will deploy its tasks in parallel
        // (one step per pod instance), with cross-pod deployment determined by the configured phase strategy.
        List<Step> steps = new ArrayList<>();
        for (int i = 0; i < podSpec.getCount(); i++) {
            List<String> allTaskNames = podSpec.getTasks().stream()
                    .map(taskSpec -> taskSpec.getName())
                    .collect(Collectors.toList());
            steps.add(deploymentStepFactory.getStep(new DefaultPodInstance(podSpec, i), allTaskNames));
        }
        return new DefaultPhase(
                phaseName, steps, getPhaseStrategyGenerator(strategy).generate(steps), Collections.emptyList());
    }

    /**
     * Custom steps are defined, but with a serial (or serial-canary) phase. We can make do without needing any custom
     * dependency logic. The phase steps will each launch one or more tasks in the various pods.
     */
    private static Phase generatePhaseWithCustomSerialSteps(
            DeploymentStepFactory deploymentStepFactory,
            String strategy,
            String phaseName,
            PodSpec podSpec,
            Map<String, List<List<String>>> podIndexToTasks) {
        List<Step> steps = new ArrayList<>();
        for (int i = 0; i < podSpec.getCount(); ++i) {
            List<List<String>> taskLists = podIndexToTasks.get(String.valueOf(i));
            if (taskLists == null) {
                taskLists = podIndexToTasks.get(DEFAULT_POD_INDEX_LABEL);
                if (taskLists == null) {
                    // Missing both matching pod index and 'default'
                    throw new IllegalStateException(String.format(
                            "Malformed steps in phase '%s': Missing '%d' step entry, and no 'default' defined",
                            phaseName, i));
                }
            }
            // Add steps to the sequence, where each step may launch one or more tasks. Because the phase strategy
            // is serial, this is all we need to do. For example, [[a, b], c] => step[a, b], step[c]
            for (List<String> taskNames : taskLists) {
                steps.add(deploymentStepFactory.getStep(new DefaultPodInstance(podSpec, i), taskNames));
            }
        }
        return new DefaultPhase(
                phaseName, steps, getPhaseStrategyGenerator(strategy).generate(steps), Collections.emptyList());
    }

    /**
     * Custom steps are defined and pods will be deploying in parallel. As a result, the pods will be deploying in
     * parallel, but any tasks within those pods may deploy serially. To support this, we configure a custom dependency
     * graph that enforces any serial deployment within each of the pods, while still allowing cross-pod deployment to
     * run in parallel.
     */
    private static Phase generatePhaseWithCustomParallelSteps(
            DeploymentStepFactory deploymentStepFactory,
            String strategy,
            String phaseName,
            PodSpec podSpec,
            Map<String, List<List<String>>> podIndexToTasks) {
        DependencyStrategyHelper<Step> dependencies = new DependencyStrategyHelper<>(Collections.emptyList());
        List<Step> phaseSteps = new ArrayList<>();
        for (int i = 0; i < podSpec.getCount(); ++i) {
            List<Step> podSteps = new ArrayList<>();
            List<List<String>> taskLists = podIndexToTasks.get(String.valueOf(i));
            if (taskLists == null) {
                taskLists = podIndexToTasks.get(DEFAULT_POD_INDEX_LABEL);
                if (taskLists == null) {
                    // Missing both matching pod index and 'default'
                    throw new IllegalStateException(String.format(
                            "Malformed steps in phase '%s': Missing '%d' step entry, and no 'default' defined",
                            phaseName, i));
                }
            }
            for (List<String> taskNames : taskLists) {
                Step step = deploymentStepFactory.getStep(new DefaultPodInstance(podSpec, i), taskNames);
                if (podSteps.isEmpty()) {
                    // If there are no parent steps, we should at least ensure that this step is listed in the strategy.
                    dependencies.addElement(step);
                } else {
                    // Mark this new step as dependent on all preceding steps within the pod. We MUST mark against ALL
                    // parent steps because the DependencyStrategyHelper doesn't check chained dependencies.
                    for (Step podStep : podSteps) {
                        dependencies.addDependency(step, podStep);
                    }
                }
                podSteps.add(step);
            }
            phaseSteps.addAll(podSteps);
        }
        Strategy<Step> phaseStrategy = new DependencyStrategy<>(dependencies);
        if (strategy.endsWith("-canary")) {
            phaseStrategy = new CanaryStrategy(phaseStrategy, phaseSteps);
        }
        return new DefaultPhase(phaseName, phaseSteps, phaseStrategy, Collections.emptyList());
    }

    /**
     * Processes and flattens the step entries in the provided {@link RawPhase}.
     */
    private static Map<String, List<List<String>>> mapPodIndexesToTasks(
            String phaseName, List<WriteOnceLinkedHashMap<String, List<List<String>>>> rawSteps) {
        // Check that each YAML step "map" entry has exactly one element
        for (WriteOnceLinkedHashMap<String, List<List<String>>> stepsEntry : rawSteps) {
            if (stepsEntry.size() != 1) {
                throw new IllegalStateException(String.format(
                        "Malformed step in phase '%s': Map should contain a single entry, but has %d: %s",
                        phaseName, stepsEntry.size(), stepsEntry));
            }
        }

        // Flatten map data: pod index (or 'default') to task deployment within that pod
        return rawSteps.stream()
                .map(stepMap -> stepMap.entrySet().stream().findFirst().get())
                .collect(Collectors.toMap(
                        stringListEntry -> stringListEntry.getKey(),
                        stringListEntry -> stringListEntry.getValue()));
    }

    private static StrategyGenerator<Phase> getPlanStrategyGenerator(String strategyType) {
        if (strategyType == null) {
            return new SerialStrategy.Generator<>();
        }
        StrategyGenerator<Phase> generator = PLAN_STRATEGY_GENERATORS.get(strategyType);
        if (generator == null) {
            throw new IllegalStateException(String.format("Unsupported plan strategy '%s', expected one of: %s",
                    strategyType, PLAN_STRATEGY_GENERATORS.keySet()));
        }
        return generator;
    }

    private static StrategyGenerator<Step> getPhaseStrategyGenerator(String strategyType) {
        if (strategyType == null) {
            return new SerialStrategy.Generator<>();
        }
        StrategyGenerator<Step> generator = PHASE_STRATEGY_GENERATORS.get(strategyType);
        if (generator == null) {
            throw new IllegalStateException(String.format("Unsupported phase strategy '%s', expected one of: %s",
                    strategyType, PHASE_STRATEGY_GENERATORS.keySet()));
        }
        return generator;
    }
}
