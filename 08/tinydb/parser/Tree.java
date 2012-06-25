package parser;

import java.util.ArrayList;
import java.util.List;

class Tree<T> {
	T value;
	Tree<T> left;
	Tree<T> right;
	long costs;
	long cardinality;
	
	Tree(Tree<T> left, Tree<T> right, T value){
		this.left = left;
		this.right = right;
		this.value = value;
	}
	
	int size(){
		if(left == null && right == null) // leaf
			return 1;
		return 1+left.size()+right.size();
	}
	
	int nleaves(){
		if(left == null && right == null) // leaf
			return 1;
		return left.nleaves()+right.nleaves();	
	}
	
	List<T> values(){
		List<T> list = new ArrayList<T>();
		if(left == null && right == null) // leaf
			list.add(value);
		else{
			list.addAll(left.values());
			list.addAll(right.values());
		}
		return list;
	}
	
	public String toString(){
		String r = " ";
		if(left != null && right != null){
			r+= "("+left.toString();
			r+= " |><|";
			r += right.toString()+")";
//			r += "|><|";
//			r += left.toString();
//			r += right.toString();
		}else{
			r += value;
		}
		return r;
	}
}