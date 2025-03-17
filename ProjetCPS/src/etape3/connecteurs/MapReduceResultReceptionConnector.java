package etape3.connecteurs;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

public class MapReduceResultReceptionConnector extends AbstractConnector implements MapReduceResultReceptionCI {

	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		((MapReduceResultReceptionCI) this.offering).acceptResult(computationURI, emitterId, acc);
	}

}
