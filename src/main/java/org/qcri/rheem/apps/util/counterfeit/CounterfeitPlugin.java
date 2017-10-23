package org.qcri.rheem.apps.util.counterfeit;

import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This {@link Plugin} implementation provides a configurable number of counterfeit {@link Platform}s along with
 * counterfeit {@link ChannelConversion}s and counterfeit {@link Mapping}s.
 */
public class CounterfeitPlugin implements Plugin {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final List<CounterfeitPlatform> platforms = new ArrayList<>();

    private final Collection<Mapping> operatorMappings = new ArrayList<>();

    private final Collection<ChannelConversion> channelConversions = new ArrayList<>();


    /**
     * Create a new instance.
     *
     * @param numPlatforms the number of {@link Platform} that should be required
     * @param ccgDensity   the desired density of the {@link ChannelConversionGraph}
     */
    public CounterfeitPlugin(int numPlatforms, double ccgDensity) {
        // Create platforms.
        AtomicInteger idGenerator = new AtomicInteger(0);
        for (int platformNumber = 0; platformNumber < numPlatforms; platformNumber++) {
            this.platforms.add(new CounterfeitPlatform(platformNumber, idGenerator));
        }
        this.logger.info("Added {} counterfeit platforms.", this.platforms.size());

        // Create operator mappings.
        Collection<Operator> mappedOperators = Arrays.asList(
                new CartesianOperator<>(DataSetType.none(), DataSetType.none()),
                new CoGroupOperator<>(null, null, DataSetType.none(), DataSetType.none()),
                new CollectionSource<>(null, DataSetType.none()),
                new CountOperator<>(DataSetType.none()),
                new DistinctOperator<>(DataSetType.none()),
                new DoWhileOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor<Collection<Void>>) null, 20),
                new FilterOperator<>(null, DataSetType.none()),
                new FlatMapOperator<>(null, DataSetType.none(), DataSetType.none()),
                new GlobalMaterializedGroupOperator<>(DataSetType.none(), DataSetType.groupedNone()),
                new GlobalReduceOperator<>(null, DataSetType.none()),
                new GroupByOperator<>(null, DataSetType.none(), DataSetType.groupedNone()),
                new IntersectOperator<>(DataSetType.none()),
                new JoinOperator<>(null, null, DataSetType.none(), DataSetType.none()),
                new LocalCallbackSink<>(null, DataSetType.none()),
                new LoopOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor<Collection<Void>>) null, 20),
                new MapOperator<>(null, DataSetType.none(), DataSetType.none()),
                new MapPartitionsOperator<>(null, DataSetType.none(), DataSetType.none()),
                new MaterializedGroupByOperator<>(null, DataSetType.none(), DataSetType.groupedNone()),
                new ReduceByOperator<>(null, null, DataSetType.none()),
                new ReduceOperator<>(null, DataSetType.none(), DataSetType.none()),
                new RepeatOperator<>(20, DataSetType.none()),
                new SampleOperator<>(10, DataSetType.none()),
                new SortOperator<>(null, DataSetType.none()),
                new TableSource(""),
                new TextFileSink<>(null, null, DataSetType.none().getDataUnitType().getTypeClass()),
                new TextFileSource(""),
                new UnionAllOperator<>(DataSetType.none()),
                new ZipWithIdOperator<>(DataSetType.none())
        );
        for (CounterfeitPlatform platform : this.platforms) {
            this.operatorMappings.addAll(platform.createMappings(mappedOperators));
        }
        this.logger.info("Added {} operator mappings per counterfeit platform.", mappedOperators.size());

        // Create channel conversions.
        int numChannels = 1; // HDFS object file channel.
        for (CounterfeitPlatform platform : this.platforms) {
            this.channelConversions.addAll(platform.createDefaultChannelConversions());
            numChannels += platform.getChannels().size();
        }
        this.logger.info("Default channel conversion graph comprises {} channels and {} conversions (density = {}).",
                numChannels,
                this.channelConversions.size(),
                this.channelConversions.size() / (double) (numChannels * (numChannels - 1))
        );
        int maxConversions = (int) Math.round(numChannels * (numChannels - 1) * ccgDensity);
        if (maxConversions > this.channelConversions.size()) {
            int numRandomConversions = maxConversions - this.channelConversions.size();
            this.logger.info("Adding {} more random channel conversions to reach a channel conversion graph density of {}.",
                    numRandomConversions,
                    ccgDensity
            );
            // Determine random channel pairs from different platforms.
            Random random = new Random();
            Set<Tuple<ChannelDescriptor, ChannelDescriptor>> channelPairs = new HashSet<>();
            while (channelPairs.size() < numRandomConversions) {
                int r1 = random.nextInt(numPlatforms), r2;
                do {
                    r2 = random.nextInt(numPlatforms);
                } while (r1 == r2);
                channelPairs.add(new Tuple<>(
                        pickRandomChannelDescriptor(this.platforms.get(r1), random),
                        pickRandomChannelDescriptor(this.platforms.get(r2), random)
                ));
            }
            for (Tuple<ChannelDescriptor, ChannelDescriptor> channelPair : channelPairs) {
                // TODO: Pick some specific platform?
                CounterfeitPlatform platform = this.platforms.get(random.nextInt(this.platforms.size()));
                this.channelConversions.add(new DefaultChannelConversion(
                        channelPair.field0,
                        channelPair.field1,
                        () -> new CounterfeitExecutionOperator(1, 1, false, platform, channelPair.field1)
                                .withName("Random conversion")
                ));
            }
        }
    }

    /**
     * Pick a random {@link ChannelDescriptor} from the given {@link CounterfeitPlatform}.
     *
     * @param platform from which to pick
     * @param random   provides randomness
     * @return a random {@link ChannelDescriptor}
     */
    private static CounterfeitChannel.Descriptor pickRandomChannelDescriptor(CounterfeitPlatform platform, Random random) {
        int r = random.nextInt(platform.getChannels().size());
        return platform.getChannels().get(r);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return (Collection<Platform>) (Collection) this.platforms;
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.operatorMappings;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return this.channelConversions;
    }

    @Override
    public void setProperties(Configuration configuration) {

    }
}
