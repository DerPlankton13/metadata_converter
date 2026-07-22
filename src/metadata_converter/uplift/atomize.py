"""Pure transform that atomizes blank nested schema.org nodes into standalone entities.

A "blank node" in JSON-LD is a typed property which does not have its own @id. In the
 pydantic models this corresponds to a nested ``SchemaOrgBase`` instance with no
``@id`` of its own. Atomizing replaces the nested model with a bare reference
(mirroring the ref shape ``LinkApplier`` produces via ``target_cls(id=...)``) and
returns it as a separate entity an ``@id`` built from its content hash. This hash is
identical for identical content, independent of which models contain the nested model.
That determinism is what turns a mechanical "pull it out" into atomizing:
content-identical blank nodes collapse onto one shared entity for all models in the
graph gaining deduplication and cross-linking in the graph which was not present before
when each blank node property was directly embedded.
"""

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.hashing import hashed_id


def atomize_blank_nodes(
    entity: SchemaOrgBase,
) -> tuple[SchemaOrgBase, list[SchemaOrgBase]]:
    """Atomize every nested blank node reachable from `entity` into standalone entities.

    Recurses bottom-up: a blank node's own nested blank children are atomized first, so
    its content hash — and therefore its `@id` — is computed from its fully-resolved
    form, the same shape it will eventually be serialised as. A nested node that already
    carries an `@id` is left untouched, together with its own nested content: it is
    already a standalone identity, not something to atomize further.

    Newly atomized entities are collected into one dict, local to this call, shared by
    every recursive step (`_rebuild_entity_with_references` / `_atomize_nodes_in_value` /
    `_atomize_node`) and keyed by `@id`. Passing it down and mutating it in place —
    rather than returning a list from every call and merging those lists back together
    at each level — is what keeps the recursive helpers simple: they only ever need to
    return the value they resolved, never a `(value, found_so_far)` pair.

    Parameters
    ----------
    entity : SchemaOrgBase
        The entity to atomize. Never mutated; a new instance is returned whenever any
        field changes.

    Returns
    -------
    tuple[SchemaOrgBase, list[SchemaOrgBase]]
        The entity with every blank field replaced by a bare reference, and the list of
        newly atomized entities discovered anywhere in its tree, each carrying its own
        content-hashed `@id`. Content-identical blank nodes collapse to a single entry,
        since the hash — and therefore the `@id` — is the same regardless of where they
        were nested.
    """
    atomized: dict[str, SchemaOrgBase] = {}
    result = _rebuild_entity_with_references(entity, atomized)
    return result, list(atomized.values())


def _rebuild_entity_with_references(
    entity: SchemaOrgBase, atomized: dict[str, SchemaOrgBase]
) -> SchemaOrgBase:
    """Rebuild `entity`, replacing each blank nested node with a reference where applicable.

    "Resolved" means: unchanged for a scalar, walked element-by-element for a list, and
    for a nested node either left as-is (already has an `@id`) or replaced by a bare
    reference to a newly atomized entity — see `_atomize_nodes_in_value` and
    `_atomize_node`. Not every field ends up as a reference: only the ones that held a
    blank node do.
    Iterating `entity` directly (rather than `type(entity).model_fields`) walks both
    declared fields and any `extra="allow"` properties outside the modelled schema.org
    vocabulary, so a blank node nested under either is atomized the same way.

    Parameters
    ----------
    entity : SchemaOrgBase
        The entity whose fields should be resolved. Never mutated.
    atomized : dict[str, SchemaOrgBase]
        Accumulator of newly atomized entities discovered so far in this call tree,
        keyed by `@id`. Mutated in place by `_atomize_node` as new blank nodes are
        atomized during the walk.

    Returns
    -------
    SchemaOrgBase
        A new instance of `entity` with every field set to its resolved value.
    """
    updates = {name: _atomize_nodes_in_value(value, atomized) for name, value in entity}
    return entity.model_copy(update=updates)


def _atomize_nodes_in_value(
    value: object, atomized: dict[str, SchemaOrgBase]
) -> object:
    """Atomize any nested node(s) in one field value; pass anything else through unchanged.

    Routes on `value`'s shape only — it makes no atomize-or-leave-alone decision itself,
    that's `_atomize_node`'s job. A single nested node is handed to `_atomize_node`
    directly; a list is walked element-by-element so every item gets the same treatment
    regardless of whether the field is single- or multi-valued; anything else (a plain
    scalar, the raw `@context` dict) passes through untouched.

    Parameters
    ----------
    value : object
        A single field's value, as read off an entity — a nested `SchemaOrgBase`, a
        list (of nested nodes, plain scalars, or a mix), or a plain scalar.
    atomized : dict[str, SchemaOrgBase]
        Accumulator of newly atomized entities discovered so far in this call tree; see
        `_rebuild_entity_with_references`. Mutated in place.

    Returns
    -------
    object
        `value` unchanged if it is neither a `SchemaOrgBase` nor a list; otherwise the
        result of `_atomize_node` or a new list of such results.
    """
    if isinstance(value, SchemaOrgBase):
        return _atomize_node(value, atomized)
    if isinstance(value, list):
        return [_atomize_nodes_in_value(item, atomized) for item in value]
    return value


def _atomize_node(
    node: SchemaOrgBase, atomized: dict[str, SchemaOrgBase]
) -> SchemaOrgBase:
    """Leave an already-identified node as-is; otherwise atomize it and return a ref to it.

    Atomizing a blank `node` means: resolve its own nested blank children first (so its
    content hash reflects its fully-resolved form), compute its `@id` from that content,
    register it in `atomized` under that `@id` — a repeat `@id` from content-identical
    blank content is dropped, since an equal entity is already registered — and return a
    bare reference to it instead of the node itself.

    Parameters
    ----------
    node : SchemaOrgBase
        The nested node to resolve. Never mutated.
    atomized : dict[str, SchemaOrgBase]
        Accumulator of newly atomized entities discovered so far in this call tree; see
        `_rebuild_entity_with_references`. Mutated in place: gains one entry keyed by
        `node`'s new `@id` when `node` is blank.

    Returns
    -------
    SchemaOrgBase
        `node` unchanged if it already carries an `@id`; otherwise a bare instance of
        `type(node)` with only `id` set, pointing at the newly atomized entity.
    """
    if node.id is not None:
        return node

    resolved = _rebuild_entity_with_references(node, atomized)
    node_id = hashed_id(resolved.model_dump(by_alias=True, exclude_none=True))
    resolved = resolved.model_copy(update={"id": node_id})
    atomized.setdefault(node_id, resolved)
    return type(resolved)(id=node_id)
