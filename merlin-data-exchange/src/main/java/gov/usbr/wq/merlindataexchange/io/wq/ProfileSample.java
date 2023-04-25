package gov.usbr.wq.merlindataexchange.io.wq;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

final class ProfileSample implements Comparable<ProfileSample>
{
    private final ZonedDateTime _dateTime;
    private final List<ProfileConstituent> _constituentDataList;

    public ProfileSample(ZonedDateTime dateTime, List<ProfileConstituent> constituentDataList)
    {
        _dateTime = dateTime;
        _constituentDataList = constituentDataList;
    }

    ZonedDateTime getDateTime()
    {
        return _dateTime;
    }

    List<ProfileConstituent> getConstituents()
    {
        return _constituentDataList;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ProfileSample that = (ProfileSample) o;
        return Objects.equals(_dateTime, that._dateTime) && Objects.equals(_constituentDataList, that._constituentDataList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_dateTime, _constituentDataList);
    }

    @Override
    public int compareTo(ProfileSample o)
    {
        return getDateTime().compareTo(o.getDateTime());
    }
}
