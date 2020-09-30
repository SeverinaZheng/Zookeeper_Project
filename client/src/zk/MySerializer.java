package zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class MySerializer implements org.I0Itec.zkclient.serialize.ZkSerializer{
	
	public byte[] serialize(Object data) throws org.I0Itec.zkclient.exception.ZkMarshallingError{
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		byte[] bt = null;
		try{
			ObjectOutputStream os = new ObjectOutputStream(bs);
			os.writeObject(data);
			os.flush();
			bt = bs.toByteArray();
			bs.close();
			os.close();
		}catch(Exception e){}
		return bt;
	}
	
	public Object deserialize(byte[] bytes) throws org.I0Itec.zkclient.exception.ZkMarshallingError{
		Object o = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream (bytes);        
	        ObjectInputStream ois = new ObjectInputStream (bis); 
	        o = (Object)ois.readObject();
	        ois.close();
	        bis.close();
		}catch(Exception e) {}
        return o;
	}
	
}