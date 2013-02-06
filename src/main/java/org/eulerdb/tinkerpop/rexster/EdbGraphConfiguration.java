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
import org.eulerdb.kernel.EdbTransactionalGraph;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EdbGraphConfiguration implements GraphConfiguration {

	private Logger logger = Logger
			.getLogger(this.getClass().getCanonicalName());

	public Graph configureGraphInstance(final Configuration properties)
			throws GraphConfigurationException {

		final String graphFile = properties.getString(
				Tokens.REXSTER_GRAPH_LOCATION, null);
		logger.debug("graphFile:" + graphFile);
		// determines if a mock transactional graph should be used for testing
		// purposes.
		boolean transactional, autoindex;
		try {
			transactional = properties.getBoolean("transactional", false);
			autoindex = properties.getBoolean("autoindex", false);
		} catch (ConversionException ce) {
			throw new GraphConfigurationException(ce);
		}

		try {

			return new EdbTransactionalGraph(graphFile, transactional,
					autoindex);

		} catch (Exception ex) {
			throw new GraphConfigurationException(ex);
		}
	}
}