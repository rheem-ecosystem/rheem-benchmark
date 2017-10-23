package org.qcri.rheem.apps.util.counterfeit;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;

import java.util.Objects;

/**
 * {@link Channel} implementation for {@link CounterfeitPlatform}s.
 */
public class CounterfeitChannel extends Channel {

    /**
     * @see Channel#Channel(ChannelDescriptor,OutputSlot)
     */
    public CounterfeitChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
    }

    /**
     * @see Channel#Channel(Channel)
     */
    public CounterfeitChannel(CounterfeitChannel counterfeitChannel) {
        super(counterfeitChannel);
    }

    @Override
    public CounterfeitChannel copy() {
        return new CounterfeitChannel(this);
    }

    @Override
    public ChannelInstance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        throw new RuntimeException("Execution not supported.");
    }

    /**
     * {@link Descriptor} implementation for {@link CounterfeitChannel}s.
     */
    public static class Descriptor extends ChannelDescriptor {

        /**
         * Identifies this instance.
         */
        private final int id;

        /**
         * Creates a new instance.
         *
         * @param isReusable   whether the represented {@link Channel}s are reusable
         * @param id           identifies the new instance among other instances
         */
        public Descriptor(boolean isReusable, int id) {
            super(CounterfeitChannel.class, isReusable, false);
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            final Descriptor that = (Descriptor) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), id);
        }

        @Override
        public String toString() {
            return String.format("Counterfeit channel %d [%s]", this.id, this.isReusable() ? "r" : "-");
        }
    }
}
