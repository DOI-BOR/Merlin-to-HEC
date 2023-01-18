/*
 * Copyright 2022 United States Bureau of Reclamation (USBR).
 * United States Department of the Interior
 * All Rights Reserved. USBR PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from USBR
 */

package gov.usbr.wq.merlindataexchange;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Class intended to represent the merlin configuration file.
 * Created by Ryan Miles
 */
public class MerlinConfigFile
{
	public MerlinConfigFile()
	{

	}

	public List<MerlinConfigDssData> readConfigFile(Path configPath) throws IOException
	{
		List<MerlinConfigDssData> output = new ArrayList<>();

		if (!Files.exists(configPath))
		{
			throw new IOException("Configuration file provided doesn't exist:" + System.lineSeparator() + "\t" + configPath.toAbsolutePath().toString());
		}

		return output;
	}
}
