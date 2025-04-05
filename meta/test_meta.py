from meta import TableMeta, merge_models, DatabaseMeta


def test_simple_merge():
    meta_from_db = TableMeta(
        source="source",
        source_url="http://example.com",
    )
    meta_from_file = TableMeta()
    merged = merge_models(meta_from_db, meta_from_file)
    print(merged)
    assert merged == meta_from_db
    assert merged != meta_from_file

    meta_from_file = TableMeta(about="about")
    merged = merge_models(meta_from_db, meta_from_file)
    assert merged == TableMeta(
        about="about",
        source="source",
        source_url="http://example.com",
    )
    meta_from_file = TableMeta(source="source2")
    merged = merge_models(meta_from_db, meta_from_file)
    assert merged == TableMeta(
        source="source2",
        source_url="http://example.com",
    )


def test():
    meta_from_db = DatabaseMeta(
        source="source",
        tables={"bla": TableMeta(source="source1")}
    )
    print(type(meta_from_db.tables))


def test_complex_merge():
    meta_from_db = DatabaseMeta(
        source="source",
        tables={"bla": TableMeta(source="source1")}
    )
    merged = merge_models(meta_from_db, DatabaseMeta(
        tables={"bla": TableMeta(about="about")}
    ))
    print(merged)
    assert merged == DatabaseMeta(
        source="source",
        tables={"bla": TableMeta(source="source1", about="about")},
    )

    merged = merge_models(meta_from_db, DatabaseMeta(
        tables={"bla2": TableMeta(about="about")}
    ))
    print(merged)
    assert merged == DatabaseMeta(
        source="source",
        tables={"bla": TableMeta(source="source1"),"bla2": TableMeta(about="about")},
    )
