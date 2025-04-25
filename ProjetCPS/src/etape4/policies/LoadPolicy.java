package etape4.policies;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;


public class LoadPolicy implements LoadPolicyI {

	private static final int CRITICAL_SIZE = 30;
	
	private static final int MERGE_GAP_POLICY = 15;
	
	private static final long serialVersionUID = 1L;

	@Override
	public boolean shouldSplitInTwoAdjacentNodes(int currentSize) {

		return currentSize >= CRITICAL_SIZE;
	}

	@Override
	public boolean shouldMergeWithNextNode(int thisNodeCurrentSize, int nextNodeCurrentSize) {

		return (nextNodeCurrentSize - thisNodeCurrentSize) > MERGE_GAP_POLICY;
	}

}
