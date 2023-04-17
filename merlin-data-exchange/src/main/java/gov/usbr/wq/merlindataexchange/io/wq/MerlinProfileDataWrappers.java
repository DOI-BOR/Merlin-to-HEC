package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.DataWrapper;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Used to combine retrieved Data Wrappers for constituents for a given profile. Also includes flags for removing first/last
 * profile if necessary (used in cases where time window is cutting off the first/last profiles).
 */
final class MerlinProfileDataWrappers extends ArrayList<DataWrapper>
{
    private boolean _removeFirstProfile = false;
    private boolean _removeLastProfile = false;

    boolean removeFirstProfile()
    {
        return _removeFirstProfile;
    }

    void setRemoveFirstProfile()
    {
        _removeFirstProfile = true;
    }

    boolean removeLastProfile()
    {
        return _removeLastProfile;
    }

    void setRemoveLastProfile()
    {
        _removeLastProfile = true;
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
        MerlinProfileDataWrappers that = (MerlinProfileDataWrappers) o;
        return super.equals(o) && _removeFirstProfile == that._removeFirstProfile && _removeLastProfile == that._removeLastProfile;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), _removeFirstProfile, _removeLastProfile);
    }
}
