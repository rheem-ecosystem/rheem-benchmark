package org.qcri.rheem.apps.util.counterfeit;

import org.junit.Test;
import org.qcri.rheem.api.CountDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.LoadCollectionDataQuantaBuilder;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;

import java.util.Arrays;

/**
 * Test suite for the {@link CounterfeitPlugin}.
 */
public class CounterfeitPluginTest {

    @Test
    public void testOptimizationWithCounterfeitPlugin() {
        // Set up Rheem context without execution and with the CounterfeitPlugin.
        Configuration configuration = new Configuration();
        configuration.setProperty("rheem.core.debug.skipexecution", "true");
        CounterfeitPlugin counterfeitPlugin = new CounterfeitPlugin(5, 0.1);
        RheemContext rheemContext = new RheemContext(configuration).with(counterfeitPlugin);

        // Build and start a Rheem plan.
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext, "Test job");
        LoadCollectionDataQuantaBuilder<String> testData = planBuilder
                .loadCollection(Arrays.asList("Just", "a", "test"))
                .withName("Load data");
        CountDataQuantaBuilder<String> testDataCount = testData.count().withName("Count");
        testData.map(String::length)
                .withBroadcast(testDataCount, "count")
                .withName("String::length")
                .collect();

    }

}
