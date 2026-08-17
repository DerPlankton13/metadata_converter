"""Pure transform that atomizes blank nested schema.org nodes into standalone entities.

A "blank node" in JSON-LD is a typed property which does not have its own @id. In the
 pydantic models this corresponds to a nested ``SchemaOrgBase`` instance with no
``@id`` of its own. Atomizing replaces the nested model with a bare reference
(mirroring the ref shape ``LinkApplier`` produces via ``target_cls(id=...)``) and
returns it as a separate entity with an ``@id`` built from its content hash. This hash
is identical for identical content, independent of which models contain the nested model.
That determinism is what turns a mechanical "pull it out" into atomizing:
content-identical blank nodes collapse onto one shared entity for all models in the
graph gaining deduplication and cross-linking in the graph which was not present before
when each blank node property was directly embedded.
"""

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.utils.hashing import hashed_id
from metadata_converter.utils.jsonld import SCHEMA_ORG_DEFAULT_CONTEXT


def atomize_blank_nodes(
    entity: SchemaOrgBase,
) -> tuple[SchemaOrgBase, list[SchemaOrgBase]]:
    """Atomize every blank node reachable from `entity` into standalone entities.

    Recurses bottom-up: a blank node's own nested blank children are atomized first, so
    its content hash — and therefore its `@id` — is computed from its fully-atomized
    form, the same shape it will eventually be serialised as. A nested node that already
    carries an `@id` is left untouched, together with its own nested content: it is
    already a standalone identity, not something to atomize further.

    Parameters
    ----------
    entity : SchemaOrgBase
        The entity to atomize. Never mutated: the original `entity`, and everything
        nested inside it, is left untouched.

    Returns
    -------
    tuple[SchemaOrgBase, list[SchemaOrgBase]]
        The entity with every blank field replaced by a bare reference, and the list of
        newly atomized entities discovered anywhere in its tree, each carrying its own
        content-hashed `@id`. Content-identical blank nodes collapse to a single entry,
        since the hash — and therefore the `@id` — is the same regardless of where they
        were nested.
    """
    # collector for all atomized models, indexing via @id does the deduplication
    atomized: dict[str, SchemaOrgBase] = {}
    result = _rebuild_entity_with_references(entity, atomized)
    return result, list(atomized.values())


def _rebuild_entity_with_references(
    entity: SchemaOrgBase, atomized: dict[str, SchemaOrgBase]
) -> SchemaOrgBase:
    """Rebuild `entity`, replacing each blank node with a reference where applicable.

    "Resolved" means: unchanged for a scalar, walked element-by-element for a list, and
    for a nested node either left as-is (already has an `@id`) or replaced by a bare
    reference to a newly atomized entity — see `_atomize_nodes_in_value` and
    `_atomize_node`. Not every field ends up as a reference: only the ones that held a
    blank node do. This covers any `extra="allow"` properties outside the modelled
    schema.org vocabulary too, so a blank node nested under one of those is atomized the
    same way as a declared field. Note that if the extra property contains invalid types
    (i.e. not a SchemaOrgBase subclass) it will stay a dict during Pydantics validation
    and will thus not be atomized, but be kept as the typed dict it is. 

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

    Dispatches on `value`'s shape only: a single nested node goes to `_atomize_node`
    directly, and a list is walked element-by-element so every item gets the same
    treatment regardless of whether the field is single- or multi-valued. Anything
    else — a plain scalar, the raw `@context` dict — passes through unchanged; that
    passthrough case doubles as the base case ending the recursion.

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
    # this is the case where the recursion stops
    return value


def _atomize_node(
    node: SchemaOrgBase, atomized: dict[str, SchemaOrgBase]
) -> SchemaOrgBase:
    """Leave an already-identified node as-is; otherwise atomize it and return a ref to it.

    Atomizing a blank `node` means: resolve its own nested blank children first (so its
    content hash reflects its fully-resolved form), compute its `@id` from that content,
    set `context` to schema.org's vocab now that the node is being promoted to a
    standalone top-level entity (nested blank nodes carry none — see `build_root`),
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

    # if node contains no nested blank node, this just returns node
    # otherwise this returns the node containing only references
    resolved = _rebuild_entity_with_references(node, atomized)
    node_id = hashed_id(
        resolved.model_dump(by_alias=True, exclude_none=True, exclude={"context"})
    )
    resolved = resolved.model_copy(
        update={"id": node_id, "context": dict(SCHEMA_ORG_DEFAULT_CONTEXT)}
    )
    atomized.setdefault(node_id, resolved)
    return type(resolved)(id=node_id)


class AtomizeApplier:
    """Atomize every entity in an `EntityStore`, replacing its contents in place."""

    def __init__(self, store: EntityStore) -> None:
        self.store = store

    def apply(self) -> None:
        """Atomize every entity in the store and replace `store.by_type` with the result.

        Runs `atomize_blank_nodes` over every entity currently in the store (a snapshot
        taken up front, so rebuilding entities during the walk never affects what's still
        to be processed), collecting every rebuilt top-level entity and every newly
        atomized entity into one `@id`-keyed map, then writes that map back to the store,
        regrouped by `@type`.

        Raises
        ------
        ValueError
            If two entities being merged into the store — top-level or newly atomized —
            share an `@id` but have different content. A shared `@id` with matching
            content is deduplicated silently instead.
        """
        final_by_id: dict[str, SchemaOrgBase] = {}
        for entity in self.store.all_entities():
            rebuilt, atoms = atomize_blank_nodes(entity)
            self._register_atoms(final_by_id, [rebuilt, *atoms])

        self.store.set_entities(list(final_by_id.values()))

    @staticmethod
    def _register_atoms(
        final_by_id: dict[str, SchemaOrgBase], atoms: list[SchemaOrgBase]
    ) -> None:
        """Insert each of `atoms` under its `@id`, or raise on a genuine content mismatch.

        Parameters
        ----------
        final_by_id : dict[str, SchemaOrgBase]
            Accumulator of entities keyed by `@id`, built up across the whole store.
            Mutated in place: gains an entry for each of `atoms` unless already present
            with equal content.
        atoms : list[SchemaOrgBase]
            The entities to register: a rebuilt top-level entity together with every
            newly atomized entity discovered in its tree.

        Raises
        ------
        ValueError
            If an `@id` in `atoms` is already present in `final_by_id` under different
            content.
        """
        for atom in atoms:
            existing = final_by_id.get(atom.id)
            if existing is None:
                final_by_id[atom.id] = atom
            elif existing != atom:
                raise ValueError(
                    f"@id {atom.id!r} is claimed by two entities with different "
                    "content during atomize."
                )
