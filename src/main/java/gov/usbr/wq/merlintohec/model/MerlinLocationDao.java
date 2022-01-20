/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.model.ProfileWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import gov.usbr.wq.merlintohec.exceptions.MerlinNoDataFoundException;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Created by Ryan Miles
 */
public final class MerlinLocationDao extends MerlinDao
{
	/**
	 * Returns a complete collection of locations available for the given office id.
	 *
	 * @param officeId Office ID used for the location template - I'm not 100% sure this will stick around.
	 * @return Collection of locations in the merlin web access.
	 * @throws MerlinAccessException Thrown when access to the merlin web access is interrupted or
	 */
	public List<LocationTemplate> catalogLocationTemplates(OfficeId officeId) throws MerlinAccessException, MerlinNoDataFoundException
	{
		MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
		List<ProfileWrapper> profiles;
		try
		{
			profiles = access.getProfiles(getAccessToken());
		}
		catch (IOException ex)
		{
			throw new MerlinAccessException("Unable to access merlin web services.", ex);
		}

		if (profiles.isEmpty())
		{
			throw new MerlinNoDataFoundException("No locations were found.");
		}

		return profiles.stream()
					   .map(profile -> MerlinDataConverter.profileToLocationTemplate(profile, officeId))
					   .collect(toList());
	}

	public LocationTemplate retrieveLocationTemplate(LocationID locationId, OfficeId officeId) throws MerlinNoDataFoundException, MerlinAccessException
	{
		List<LocationTemplate> templates = catalogLocationTemplates(officeId);
		Optional<LocationTemplate> output = templates.stream().filter(template -> template.getDbOfficeId().equals(officeId)).filter(template -> template.getLocationId().equalsIgnoreCase(locationId.getLocation())).findFirst();
		if (output.isPresent())
		{
			return output.get();
		}
		throw new MerlinNoDataFoundException("Unable to find locations for the given location ID: " + locationId.getLocation() + " and office ID: " + officeId.getOfficeId());
	}
}
