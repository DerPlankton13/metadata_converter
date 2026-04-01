"""
Verify that all generated schema.org Pydantic models can be instantiated.

Run directly after generation to confirm the generated module is valid:

    python check_schemaorg_models.py
"""

import sys

import schemaorg_models as mod
from schemaorg_models import SchemaOrgBase

classes = [
    (name, cls)
    for name, cls in vars(mod).items()
    if isinstance(cls, type)
    and issubclass(cls, SchemaOrgBase)
    and cls is not SchemaOrgBase
]

failed = []

for name, cls in classes:
    instance = None
    try:
        instance = cls()
    except Exception as e:
        failed.append((name, str(e)))
    finally:
        del instance

total = len(classes)
if failed:
    print(f"  {len(failed)} of {total} class(es) failed instantiation:")
    for name, reason in failed:
        print(f"    {name}: {reason}")
    sys.exit(1)
else:
    print(f"  All {total} classes instantiated successfully.")
