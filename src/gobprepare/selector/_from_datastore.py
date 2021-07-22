class FromDatastoreSelector():
    """From Generic Datastore Selector"""

    def _read_rows(self, query, **kwargs):
        return self._src_datastore.query(query, **kwargs)
