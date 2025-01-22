import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

public class Livre implements ContentDataI {

	private static final long serialVersionUID = 1L;
	
	String nomLivre;

	public Livre(String nomLivre) {
		super();
		this.nomLivre = nomLivre;
	}

	public Serializable getValue(String p) {
		if(p == "titre") return nomLivre;
		return null;
	}
	
	

}
