package etape4.composants;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI;

public class IgnoreChordsPolicy implements ParallelismPolicyI {
	
	private static final long serialVersionUID = 1L;
	public int nbreChordsIgnores;
	public IgnoreChordsPolicy(int nbreChordsIgnores) {
		this.nbreChordsIgnores = nbreChordsIgnores;
	}
	public int getNbreChordsIgnores() {
		return nbreChordsIgnores;
	}
	
}
