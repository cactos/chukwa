package eu.cactosfp7.datacollection.chukwa.demux.mapper.vmplacement;

public class VMData {
	public String IPNAME;
	public String STATE;
	public String UUID;
	public String IMAGE_UUID;
	public long TIMESTAMP;
	public int RAM;
	public int CORES;

	public VMData(String ipname, String State, String Uuid, String image_uuid, long time, int ram, int cores) {
		IPNAME=ipname;
		STATE=State;
		UUID=Uuid;
		IMAGE_UUID=image_uuid;
		TIMESTAMP=time;
		RAM=ram;
		CORES=cores;
	}

	public void setVMData(String ipname, String State, String Uuid, String image_uuid, long time, int ram, int cores) {
		IPNAME=ipname;
		STATE=State;
		UUID=Uuid;
		IMAGE_UUID=image_uuid;
		TIMESTAMP=time;
		RAM=ram;
		CORES=cores;
	}
}

