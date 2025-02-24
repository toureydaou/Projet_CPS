package test;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import etape1.EntierKey;
import etape1.Facade;
import etape1.Livre;
import etape1.Node;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;


public class MultiNodeTest {

	@Test
	public void putHashMaptest() throws ConnectionException {
		Node[] noeuds = new Node[4];
		POJOContentNodeCompositeEndPoint[] pojos = new POJOContentNodeCompositeEndPoint[5];
		for(int i = 0; i < pojos.length;i++) {
			pojos[i] = new POJOContentNodeCompositeEndPoint();
		}

		

		
		noeuds[0] = new Node(new IntInterval(0, 49), pojos[0], pojos[4], pojos[1]);
		noeuds[1] = new Node(new IntInterval(50, 99), pojos[1], pojos[2]);
		noeuds[2] = new Node(new IntInterval(100, 149), pojos[2], pojos[3]);
		noeuds[3] = new Node(new IntInterval(150, 199), pojos[3], pojos[4]);
		Facade f = new Facade(pojos[0]);
	
		try {
			EntierKey k_22 = new EntierKey(22); 
			f.put(k_22, new Livre("soif", 100));		
			assertEquals(f.get(k_22).getValue("nbPages"), 100);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
