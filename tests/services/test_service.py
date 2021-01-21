# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 CERN.
# Copyright (C) 2020 Northwestern University.
#
# Invenio-Records-Resources is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Service tests.

Test to add:
- Read a tombstone page
- Read with missing permissions
- Read with missing pid
"""

import pytest
from invenio_cache import current_cache
from invenio_pidstore.errors import PIDDeletedError
from invenio_search import current_search, current_search_client
from marshmallow import ValidationError
from mock_module.api import Record


def test_simple_flow(app, consumer, service, identity_simple, input_data):
    """Create a record."""
    # Create an item
    item = service.create(identity_simple, input_data)
    id_ = item.id

    # Read it
    read_item = service.read(id_, identity_simple)
    assert item.id == read_item.id
    assert item.data == read_item.data

    # Refresh to make changes live
    Record.index.refresh()

    # Search it
    res = service.search(identity_simple, q=f"id:{id_}", size=25, page=1)
    assert res.total == 1
    assert list(res.hits)[0] == read_item.data

    # Scan it
    res = service.scan(identity_simple, q=f"id:{id_}")
    assert res.total is None
    assert list(res.hits)[0] == read_item.data

    # Reindex
    ret = service.reindex(identity_simple, q=f"id:{id_}")
    assert ret is True
    assert len(list(consumer.iterqueue())) == 1

    # Update it
    data = read_item.data
    data['metadata']['title'] = 'New title'
    update_item = service.update(id_, identity_simple, data)
    assert item.id == update_item.id
    assert update_item['metadata']['title'] == 'New title'

    # Delete it
    assert service.delete(id_, identity_simple)

    # Refresh to make changes live
    Record.index.refresh()

    # Retrieve it - deleted so cannot
    # - db
    pytest.raises(PIDDeletedError, service.read, id_, identity_simple)
    # - search
    res = service.search(identity_simple, q=f"id:{id_}", size=25, page=1)
    assert res.total == 0


def test_read_all_cache(app, service, identity_simple, input_data):
    # Create an items
    item_one = service.create(identity_simple, input_data)
    item_two = service.create(identity_simple, input_data)

    records = service.read_all(identity_simple, "metadata.title", "Test")
    assert records.total == 2

    cached = current_cache.get("metadata.title-Test")
    # FIXME: This is a problem because from cache is a dict
    # From search is a ResultList. Otherwise some sort of
    # from_dict method needs to be implemented in ResultsList (not trivial)
    assert cached['hits']['total'] == 2

    # Clean cache for future tests
    cached = current_cache.delete("metadata.title-Test")


def test_read_all_no_cache(app, service, identity_simple, input_data):
    # Create an items
    item_one = service.create(identity_simple, input_data)
    item_two = service.create(identity_simple, input_data)

    records = service.read_all(identity_simple, "metadata.title", "Test")
    assert records.total == 2

    cached = current_cache.get("metadata.title-Test")
    assert not cached
