package org.eulerdb.tinkerpop.rexster;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import com.tinkerpop.rexster.util.MockTinkerTransactionalGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConversionException;
import org.apache.log4j.Logger;
import org.eulerdb.kernel.EdbGraph;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EdbGraphConfiguration implements GraphConfiguration {
	
	private Logger logger = Logger.getLogger(this.getClass().getCanonicalName());

    public Graph configureGraphInstance(final Configuration properties) throws GraphConfigurationException {

        final String graphFile = properties.getString(Tokens.REXSTER_GRAPH_LOCATION, null);
        logger.debug("graphFile:"+graphFile);
        // determines if a mock transactional graph should be used for testing purposes.
        boolean mockTx;
        try {
            mockTx = properties.getBoolean("graph-mock-tx", false);
        } catch (ConversionException ce) {
            throw new GraphConfigurationException(ce);
        }

        try {
            if (graphFile == null || graphFile.length() == 0) {
                // pure in memory if graph file is specified
                return new EdbGraph(graphFile);
            } else {
                return new EdbGraph(graphFile);
            }
        } catch (Exception ex) {
            throw new GraphConfigurationException(ex);
        }
    }
}