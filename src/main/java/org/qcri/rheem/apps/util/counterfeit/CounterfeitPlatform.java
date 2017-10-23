package org.qcri.rheem.apps.util.counterfeit;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A counterfeit {@link Platform} implementation that does not support execution.
 */
public class CounterfeitPlatform extends Platform {

    /**
     * {@link ChannelDescriptor}s that are supported by {@link CounterfeitExecutionOperator}s of this instance.
     */
    private List<CounterfeitChannel.Descriptor> inputChannels, broadcastChannels, outputChannels;

    protected final CounterfeitChannel.Descriptor nonReusableDescriptor, reusableDescriptor, broadcastDescriptor;

    private final int number;

    /**
     * Creates a new instance.
     *
     * @param number      {@code counterfeit-<number>} will be used as {@link Configuration} {@link #getConfigurationName() name}
     * @param idGenerator an {@link AtomicInteger} that is increased to provide {@link CounterfeitChannel.Descriptor} IDs; or {@code null}
     */
    public CounterfeitPlatform(int number, AtomicInteger idGenerator) {
        super(String.format("Counterfeit platform %d", number), String.format("counterfeit-%d", number));
        this.number = number;

        // Create the supported channels.
        if (idGenerator == null) idGenerator = new AtomicInteger(0);
        this.nonReusableDescriptor = new CounterfeitChannel.Descriptor(false, idGenerator.getAndIncrement());
        this.reusableDescriptor = new CounterfeitChannel.Descriptor(true, idGenerator.getAndIncrement());
        this.broadcastDescriptor = new CounterfeitChannel.Descriptor(true, idGenerator.getAndIncrement());

        this.inputChannels = Arrays.asList(this.reusableDescriptor, this.nonReusableDescriptor);
        this.broadcastChannels = Collections.singletonList(this.broadcastDescriptor);
        this.outputChannels = Collections.singletonList(this.nonReusableDescriptor);
    }

    @Override
    protected void configureDefaults(Configuration configuration) {
        // Nothing to do.
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        throw new RuntimeException("The counterfeit platform does not support execution.");
    }


    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(String.format("rheem.%s.cpu.mhz", this.getConfigurationName()), 3000);
        int numCores = (int) configuration.getLongProperty(String.format("rheem.%s.cpu.cores", this.getConfigurationName()), 1);
        double hdfsMsPerMb = configuration.getDoubleProperty(String.format("rheem.%s.hdfs.ms-per-mb", this.getConfigurationName()), 100);
        double stretch = configuration.getDoubleProperty(String.format("rheem.%s.stretch", this.getConfigurationName()), 1);
        return LoadProfileToTimeConverter.createTopLevelStretching(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate),
                stretch
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty(String.format("rheem.%s.costs.fix", this.getConfigurationName()), 0d),
                configuration.getDoubleProperty(String.format("rheem.%s.per-ms", this.getConfigurationName()), 1d)
        );
    }

    /**
     * Create {@link CounterfeitMapping}s for the given {@link Operator}s towards this instance.
     *
     * @param operators the {@link Operator}s
     * @return the {@link CounterfeitMapping}s
     */
    public Collection<Mapping> createMappings(Collection<Operator> operators) {
        return operators.stream().map(op -> new CounterfeitMapping(op, this)).collect(Collectors.toList());
    }

    /**
     * Create default {@link ChannelConversion}s within this instance.
     *
     * @return the default {@link ChannelConversion}s
     */
    public Collection<ChannelConversion> createDefaultChannelConversions() {
        ChannelConversion nonReusableToReusable = new DefaultChannelConversion(
                this.nonReusableDescriptor,
                this.reusableDescriptor,
                () -> new CounterfeitExecutionOperator(1, 1, false, this, this.reusableDescriptor)
                        .withName("Convert non-reusable to reusable")
        );
        ChannelConversion reusableToBroadcast = new DefaultChannelConversion(
                this.reusableDescriptor,
                this.broadcastDescriptor,
                () -> new CounterfeitExecutionOperator(1, 1, false, this, this.broadcastDescriptor)
                        .withName("Convert reusable to broadcast")
        );
        ChannelConversion reusableToHdfs = new DefaultChannelConversion(
                this.reusableDescriptor,
                FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
                () -> new CounterfeitExecutionOperator(1, 1, false, this, FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR)
                        .withName("Write reusable to HDFS")
        );
        ChannelConversion nonReusableToHdfs = new DefaultChannelConversion(
                this.nonReusableDescriptor,
                FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
                () -> new CounterfeitExecutionOperator(1, 1, false, this, FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR)
                        .withName("Write non-reusable to HDFS")
        );
        ChannelConversion hdfsToNonReusable = new DefaultChannelConversion(
                FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
                this.nonReusableDescriptor,
                () -> new CounterfeitExecutionOperator(1, 1, false, this, this.reusableDescriptor)
                        .withName("Read non-reusable from HDFS")
        );
        return Arrays.asList(nonReusableToReusable, reusableToBroadcast, reusableToHdfs, nonReusableToHdfs, hdfsToNonReusable);
    }

    public List<CounterfeitChannel.Descriptor> getInputChannels() {
        return this.inputChannels;
    }

    public List<CounterfeitChannel.Descriptor> getBroadcastChannels() {
        return this.broadcastChannels;
    }

    public List<CounterfeitChannel.Descriptor> getOutputChannels() {
        return this.outputChannels;
    }

    public List<CounterfeitChannel.Descriptor> getChannels() {
        return Arrays.asList(this.broadcastDescriptor, this.reusableDescriptor, this.nonReusableDescriptor);
    }


    public int getNumber() {
        return this.number;
    }
}
