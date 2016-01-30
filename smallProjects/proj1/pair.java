import java.util.*;
import java.io.*;
import java.lang.*;

public class pair{
	private int key;
	private int value;
	public pair(int k, int v){
         key = k; value = v;
	}
	public int getKey(){
		return key;
	}
	public int getValue(){
		return value;
	}
	public int hashCode(){
        return key + value;
	}
	public boolean equals(Object obj){
		if(obj instanceof pair)
		{
			if(((pair)obj).getKey() == key && ((pair)obj).getValue() == value)
				return true;
			if(((pair)obj).getKey() == value && ((pair)obj).getValue() == key)
				return true;
		}
		return false;
	}
}