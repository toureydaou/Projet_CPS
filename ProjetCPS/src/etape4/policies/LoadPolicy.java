package etape4.policies;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;

/**
 * La classe {@code LoadPolicy} implémente une politique de gestion de charge
 * pour un nœud dans une DHT (Distributed Hash Table) dans le cadre de MapReduce.
 * <p>
 * Cette politique détermine quand un nœud doit être scindé en deux
 * ou fusionné avec un nœud adjacent en fonction de sa taille actuelle
 * en se basant sur la stratégie à gros grain.
 * </p>
 * 
 * @see LoadPolicyI
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO 
 */
public class LoadPolicy implements LoadPolicyI {
	private static final long serialVersionUID = 1L;

	/** Facteur de charge d'un noeud. */
	private static final int CRITICAL_SIZE = 20;
	
	/** Taille minimale à partir de laquelle un noeud est éligible à la fusion. */
	private static final int  MINIMAL_SIZE = 2;
		
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI#shouldSplitInTwoAdjacentNodes(int)
	 */
	@Override
	public boolean shouldSplitInTwoAdjacentNodes(int currentSize) {
		return currentSize >= CRITICAL_SIZE * 2;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI#shouldMergeWithNextNode(int, int)
	 */
	@Override
	public boolean shouldMergeWithNextNode(int thisNodeCurrentSize, int nextNodeCurrentSize) {
		return ((nextNodeCurrentSize + thisNodeCurrentSize) < CRITICAL_SIZE) && thisNodeCurrentSize > MINIMAL_SIZE && nextNodeCurrentSize > MINIMAL_SIZE ;
	}

}
