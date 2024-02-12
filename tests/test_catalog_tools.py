import os
import pytest

from catalog_tools import countParentUUIDList


@pytest.mark.tools
def test_countParentUUIDList(filesDir):
    """Test that the returned list shows the correct number of parent
    datasets.
    """
    parent_file = os.path.join(filesDir, "parent-uuid-list.xml")
    n_parents = countParentUUIDList(parent_file)
    assert n_parents == 22
