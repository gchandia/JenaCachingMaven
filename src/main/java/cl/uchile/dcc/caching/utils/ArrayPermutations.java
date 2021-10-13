package cl.uchile.dcc.caching.utils;

import java.util.ArrayList;

public class ArrayPermutations {
	@SuppressWarnings("unchecked")
	public static <E> ArrayList<ArrayList<E>> generatePerm(ArrayList<E> original) {
		if (original.isEmpty()) {
			ArrayList<ArrayList<E>> result = new ArrayList<>();
			result.add((ArrayList<E>) new ArrayList<>());
			return result; 
	    }
		E firstElement = original.remove(0);
	    ArrayList<ArrayList<E>> returnValue = new ArrayList<>();
	    ArrayList<ArrayList<E>> permutations = generatePerm(original);
	    for (ArrayList<E> smallerPermutated : permutations) {
	      for (int index=0; index <= smallerPermutated.size(); index++) {
	        ArrayList<E> temp = new ArrayList<>(smallerPermutated);
	        temp.add(index, firstElement);
	        returnValue.add(temp);
	      }
	    }
	    return returnValue;
	}
}