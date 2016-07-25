import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MyKey implements WritableComparable{
	private Text SensorType,timestamp,status;
	
	public MyKey(){
		System.out.println("In Default Constructor ");
		this.SensorType = new Text();
		this.timestamp = new Text();
		this.status = new Text();
	}
	public MyKey(Text SensorType,Text timestamp,Text status){
		System.out.println(" In Constructor  MyKey(Text SensorType,Text timestamp,Text status)  ");
		this.SensorType = SensorType;
		this.timestamp = timestamp;
		this.status = status;		
	}
	public void readFields(DataInput in) throws IOException{
		System.out.println(" In Read Filed");
		SensorType.readFields(in);
		timestamp.readFields(in);
		status.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException
	{
		System.out.println("In Write Field");
	
		SensorType.write(out);
		timestamp.write(out);
		status.write(out);
	}
	public int compareTo(Object o)
	{
		System.out.println(" In Compare TO");
		MyKey other = (MyKey)o;
		int cmp = SensorType.compareTo(other.SensorType);
		if(cmp != 0){
				return cmp;
		}
		cmp = timestamp.compareTo(other.timestamp);
		if(cmp != 0){
				return cmp;
		}
		return status.compareTo(other.status);
		
	}
	public Text getSensorType() {
		return SensorType;
	}
	public void setSensorType(Text sensorType) {
		SensorType = sensorType;
	}
	public Text getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}
	public Text getStatus() {
		return status;
	}
	public void setStatus(Text status) {
		this.status = status;
	}
	

}
