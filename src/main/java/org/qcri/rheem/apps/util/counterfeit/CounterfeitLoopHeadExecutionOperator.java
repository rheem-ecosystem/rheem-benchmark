package org.qcri.rheem.apps.util.counterfeit;

import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An {@link ExecutionOperator} implementation for {@link CounterfeitPlatform}s that imitates other {@link Operator}s.
 */
public class CounterfeitLoopHeadExecutionOperator extends OperatorBase implements ExecutionOperator, LoopHeadOperator {

    /**
     * The {@link CounterfeitPlatform} this instance belongs to.
     */
    private final CounterfeitPlatform platform;

    /**
     * {@link ChannelDescriptor}s that are supported as output.
     */
    private List<ChannelDescriptor> outputChannelDescriptors;

    /**
     * Special {@link InputSlot} of this instance.
     */
    private final Collection<InputSlot<?>> initializationInputs, conditionInputs, loopBodyInputs;

    /**
     * Special {@link OutputSlot} of this instance.
     */
    private final Collection<OutputSlot<?>> loopBodyOutputs, conditionOutputs, finalOutputs;

    /**
     * The number of iterations expected to be performed by this instance.
     */
    private final int numExpectedIterations;

    /**
     * Creates a new instance that imitates the {@code blueprint}. Broadcast {@link InputSlot}s are neglected, though.
     *
     * @param blueprint that should be imitated w.r.t. {@link Slot}s
     * @param platform  with that the new instance should be associated
     */
    @SuppressWarnings("unchecked")
    public CounterfeitLoopHeadExecutionOperator(LoopHeadOperator blueprint, CounterfeitPlatform platform) {
        super(blueprint.getNumRegularInputs(), blueprint.getNumOutputs(), blueprint.isSupportingBroadcastInputs());
        this.platform = platform;
        this.numExpectedIterations = blueprint.getNumExpectedIterations();

        for (int i = 0; i < blueprint.getNumRegularInputs(); i++) {
            InputSlot<?> inputSlot = blueprint.getAllInputs()[i];
            this.inputSlots[i] = new InputSlot(inputSlot, this);
        }
        this.initializationInputs = translateSlots(blueprint.getLoopInitializationInputs(), this.inputSlots);
        this.conditionInputs = translateSlots(blueprint.getConditionInputSlots(), this.inputSlots);
        this.loopBodyInputs = translateSlots(blueprint.getLoopBodyInputs(), this.inputSlots);

        for (int i = 0; i < blueprint.getAllOutputs().length; i++) {
            OutputSlot<?> outputSlot = blueprint.getAllOutputs()[i];
            this.outputSlots[i] = new OutputSlot<>(outputSlot, this);
        }
        this.loopBodyOutputs = translateSlots(blueprint.getLoopBodyOutputs(), this.outputSlots);
        this.conditionOutputs = translateSlots(blueprint.getConditionOutputSlots(), this.outputSlots);
        this.finalOutputs = translateSlots(blueprint.getFinalLoopOutputs(), this.outputSlots);
        this.outputChannelDescriptors = (List<ChannelDescriptor>) (List) this.platform.getOutputChannels();
    }

    /**
     * Translate the {@code originalSlots}, taking from the {@code copySlots}.
     *
     * @param originalSlots that should be copied
     * @param copySlots     are the {@link Slot} copies; they are matched to the {@code originalSlots} via their index
     * @return the copied {@link Slot}
     */
    private static <T extends Slot> Collection<T> translateSlots(Collection<T> originalSlots, T[] copySlots) {
        return originalSlots.stream()
                .map(slot -> copySlots[slot.getIndex()])
                .collect(Collectors.toList());
    }

    /**
     * Calls {@link #setName(String)} and returns this instance.
     *
     * @param name the new name for this instance
     * @return this instancex
     */
    public CounterfeitLoopHeadExecutionOperator withName(String name) {
        this.setName(name);
        return this;
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return this.getInput(index).isBroadcast() ?
                (List<ChannelDescriptor>) (List) this.platform.getBroadcastChannels() :
                (List<ChannelDescriptor>) (List) this.platform.getInputChannels();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return this.outputChannelDescriptors;
    }

    @Override
    public String toString() {
        return String.format("Counterfeit[platform %d, %s]", this.platform.getNumber(), this.getName());
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return this.loopBodyOutputs;
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return this.finalOutputs;
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return this.loopBodyInputs;
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return this.initializationInputs;
    }

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        return this.conditionInputs;
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        return this.conditionOutputs;
    }

    @Override
    public int getNumExpectedIterations() {
        return this.numExpectedIterations;
    }
}
