package etape3.connecteurs;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class ResultReceptionConnector extends AbstractConnector implements ResultReceptionCI {

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		((ResultReceptionCI) this.offering).acceptResult(computationURI, result);
		
	}

}
