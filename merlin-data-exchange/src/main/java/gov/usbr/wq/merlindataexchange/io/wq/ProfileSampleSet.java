package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.TreeSet;

final class ProfileSampleSet extends TreeSet<ProfileSample>
{
    private final String _station;

    ProfileSampleSet(String station)
    {
        _station = station;
    }

    String getStation()
    {
        return _station;
    }
}
