from .codec import field, loads, name, scalar_token
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import visible_rows


def _definitions(connection, collection):
    return connection.execute("SELECT * FROM index_defs WHERE collection=?", (collection,)).fetchall()


def _tokens(definitions, document):
    if document is None:
        return []
    tokens = []
    for definition in definitions:
        token = scalar_token(field(document, definition["field"]))
        if token is not None:
            tokens.append((definition["name"], definition["unique_flag"], token))
    return tokens


def _check_unique(connection, collection, index_names):
    for index_name in sorted(index_names):
        duplicate = connection.execute(
            "SELECT 1 FROM index_values WHERE collection=? AND name=? GROUP BY token HAVING COUNT(*)>1 LIMIT 1",
            (collection, index_name)).fetchone()
        if duplicate is not None:
            raise Conflict("unique index conflict: " + index_name)


def apply_changes(connection, changes):
    """Project the final document state of a whole batch into the indexes.

    ``changes`` contains one ``(collection, key, document)`` per written key,
    with ``None`` standing for a deletion. Every changed key is removed from the
    projection before the new entries are inserted, so swaps, cycles and
    delete-then-reuse inside one commit are judged only on the state that would
    be committed. Keys absent from ``changes`` keep their existing entries and
    therefore keep occupying their unique values.

    A collection may carry several unique indexes. Each one of them is checked
    against the final occupancy, and any surviving duplicate aborts the whole
    batch through the caller's transaction.
    """
    grouped = {}
    for collection, key, document in changes:
        grouped.setdefault(collection, []).append((key, document))
    for collection, items in grouped.items():
        definitions = _definitions(connection, collection)
        for key, _ in items:
            connection.execute("DELETE FROM index_values WHERE collection=? AND key=?", (collection, key))
        for key, document in items:
            for index_name, _, token in _tokens(definitions, document):
                connection.execute("INSERT INTO index_values VALUES (?,?,?,?)",
                                   (collection, index_name, token, key))
        unique_names = {definition["name"] for definition in definitions if definition["unique_flag"]}
        _check_unique(connection, collection, unique_names)


def rebuild_collection(connection, collection, revision):
    """Recreate one collection's projection from documents visible at revision.

    Rebuilding also revalidates every unique index on the same final state that
    readers will observe, which keeps replicas and reopened handles under the
    same constraint as the primary.
    """
    connection.execute("DELETE FROM index_values WHERE collection=?", (collection,))
    definitions = _definitions(connection, collection)
    for row in visible_rows(connection, collection, revision):
        for index_name, _, token in _tokens(definitions, loads(row["body"])):
            connection.execute("INSERT INTO index_values VALUES (?,?,?,?)",
                               (collection, index_name, token, row["key"]))
    unique_names = {definition["name"] for definition in definitions if definition["unique_flag"]}
    _check_unique(connection, collection, unique_names)


class Indexes:
    def __init__(self, store):
        self.store = store

    def create(self, collection, index_name, path, unique=False):
        name(index_name)
        field({}, path)
        if not isinstance(unique, bool):
            raise InvalidDocument("unique must be boolean")
        with self.store.atomic(writable=True) as connection:
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection")
            if connection.execute("SELECT 1 FROM index_defs WHERE collection=? AND name=?", (collection, index_name)).fetchone():
                raise Conflict("index already exists")
            connection.execute("INSERT INTO index_defs VALUES (?,?,?,?)", (collection, index_name, path, int(unique)))
            rebuild_collection(connection, collection, int(self.store.get("head")))
            self.store.set("catalog_epoch", int(self.store.get("catalog_epoch")) + 1)
        return {"name": index_name, "field": path, "unique": unique}

    def drop(self, collection, index_name):
        with self.store.atomic(writable=True) as connection:
            changed = connection.execute("DELETE FROM index_defs WHERE collection=? AND name=?", (collection, index_name)).rowcount
            if not changed:
                raise NotFound("unknown index")
            self.store.set("catalog_epoch", int(self.store.get("catalog_epoch")) + 1)

    def list(self, collection):
        with self.store.read() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM index_defs WHERE collection=? ORDER BY name", (collection,))]


def equality_candidates(connection, collection, path, value):
    token = scalar_token(value)
    if token is None:
        return None
    definition = connection.execute("SELECT name FROM index_defs WHERE collection=? AND field=? ORDER BY name LIMIT 1",
                                    (collection, path)).fetchone()
    if definition is None:
        return None
    keys = {row[0] for row in connection.execute("SELECT key FROM index_values WHERE collection=? AND name=? AND token=?",
                                               (collection, definition[0], token))}
    return definition[0], keys
