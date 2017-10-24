package org.qcri.rheem.apps.util.counterfeit;

import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.Collection;
import java.util.Collections;

/**
 * Maps {@link Operator}s to {@link CounterfeitExecutionOperator}s.
 */
public class CounterfeitMapping implements Mapping {

    /**
     * Keeps around the {@link PlanTransformation}s of this instance.
     */
    private final Collection<PlanTransformation> transformations;

    /**
     * Creates a new instance that maps the given source {@link Operator} to a {@link CounterfeitExecutionOperator} of the
     * given {@link CounterfeitPlatform}.
     *
     * @param sourceOperator that should be matched
     * @param platform       with which the mapped {@link CounterfeitExecutionOperator} should be associated
     */
    public CounterfeitMapping(Operator sourceOperator, CounterfeitPlatform platform) {
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(new OperatorPattern<>("original", sourceOperator, false));
        ReplacementSubplanFactory replacementSubplanFactory = new ReplacementSubplanFactory.OfSingleOperators<>(
                (match, epoch) -> (match.isLoopHead() ?
                        new CounterfeitLoopHeadExecutionOperator((LoopHeadOperator) match, platform) :
                        new CounterfeitExecutionOperator(match, platform)
                ).at(epoch)

        );
        PlanTransformation transformation = new PlanTransformation(
                subplanPattern, replacementSubplanFactory, platform
        );

        this.transformations = Collections.singleton(transformation);
    }

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return this.transformations;
    }

}
