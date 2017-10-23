package org.qcri.rheem.apps.util.counterfeit;

import org.hsqldb.lib.Collection;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collections;
import java.util.List;

/**
 * An {@link ExecutionOperator} implementation for {@link CounterfeitPlatform}s that imitates other {@link Operator}s.
 */
public class CounterfeitExecutionOperator extends OperatorBase implements ExecutionOperator {

    /**
     * The {@link CounterfeitPlatform} this instance belongs to.
     */
    private final CounterfeitPlatform platform;

    /**
     * {@link ChannelDescriptor}s that are supported as output.
     */
    private List<ChannelDescriptor> outputChannelDescriptors;

    /**
     * Creates a new instance that imitates the {@code blueprint}. Broadcast {@link InputSlot}s are neglected, though.
     *
     * @param blueprint that should be imitated w.r.t. {@link Slot}s
     * @param platform  with that the new instance should be associated
     */
    @SuppressWarnings("unchecked")
    public CounterfeitExecutionOperator(Operator blueprint, CounterfeitPlatform platform) {
        super(blueprint.getNumRegularInputs(), blueprint.getNumOutputs(), blueprint.isSupportingBroadcastInputs());
        this.platform = platform;
        for (int i = 0; i < blueprint.getNumRegularInputs(); i++) {
            InputSlot<?> inputSlot = blueprint.getAllInputs()[i];
            this.inputSlots[i] = new InputSlot(inputSlot, this);
        }
        for (int i = 0; i < blueprint.getAllOutputs().length; i++) {
            OutputSlot<?> outputSlot = blueprint.getAllOutputs()[i];
            this.outputSlots[i] = new OutputSlot<>(outputSlot, this);
        }
        this.outputChannelDescriptors = (List<ChannelDescriptor>) (List) this.platform.getOutputChannels();
    }

    /**
     * Creates a new instance.
     *
     * @see OperatorBase#OperatorBase(int, int, boolean)
     */
    public CounterfeitExecutionOperator(int numInputs,
                                        int numOutputs,
                                        boolean isSupportingBroadcastInputs,
                                        CounterfeitPlatform platform,
                                        ChannelDescriptor outputChannelDescriptor) {
        super(numInputs, numOutputs, isSupportingBroadcastInputs);
        this.platform = platform;
        for (int i = 0; i < this.inputSlots.length; i++) {
            this.inputSlots[i] = new InputSlot<>(String.format("in%d", i), this, DataSetType.none());
        }
        for (int i = 0; i < this.outputSlots.length; i++) {
            this.outputSlots[i] = new OutputSlot<>(String.format("out%d", i), this, DataSetType.none());
        }
        this.outputChannelDescriptors = Collections.singletonList(outputChannelDescriptor);
    }

    /**
     * Calls {@link #setName(String)} and returns this instance.
     *
     * @param name the new name for this instance
     * @return this instancex
     */
    public CounterfeitExecutionOperator withName(String name) {
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
}
