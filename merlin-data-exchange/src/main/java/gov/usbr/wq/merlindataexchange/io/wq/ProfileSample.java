package gov.usbr.wq.merlindataexchange.io.wq;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

final class ProfileSample
{
    private final ZonedDateTime _dateTime;
    private final List<ProfileConstituentData> _constituentDataList;

    public ProfileSample(ZonedDateTime dateTime, List<ProfileConstituentData> constituentDataList)
    {
        _dateTime = dateTime;
        _constituentDataList = constituentDataList;
    }

    ZonedDateTime getDateTime()
    {
        return _dateTime;
    }

    List<ProfileConstituentData> getConstituentDataList()
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
}
