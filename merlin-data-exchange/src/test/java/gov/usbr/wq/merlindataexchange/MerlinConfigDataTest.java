package gov.usbr.wq.merlindataexchange;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MerlinConfigDataTest
{
	@Test
	void testConfigData()
	{
		String merlinId = "";
		String dssFilePath = "";
		String dssPathname = "";
		boolean mustExist = true;

		MerlinConfigData data = new MerlinConfigData(merlinId, dssFilePath, dssPathname, mustExist);
		assertEquals(merlinId, data.getMerlinId());
		assertEquals(dssFilePath, data.getDssFilePath());
		assertEquals(dssPathname, data.getDssPathname());
		assertTrue(data.isMustExist());
	}
}