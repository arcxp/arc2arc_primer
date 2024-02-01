import base64
import hashlib
import json
import uuid

import six


def generate_arc_id(*args, **kwargs):
    r"""from_hash(*args, *, as_uuid=False, **kwargs)
    Computes a hash to generate an Arc ID

    Values given to \*args and \*\*kwargs must be JSON-compatible.

    It is recommended to make the organization one of the factors for computing
    the hash IDs.  This will ensure IDs are not duplicated across organizations.

    It is also recommended that this function be hidden behind another
    function that validates input to ensure consistent behavior in practice.

    For example, if Arc IDs are to be based off of an integer value, if a
    string representation of that value is used, a different ID will result.
    Fronting this function will allow control over the inputs by implementing
    validation, specific to the client's use case."""

    uuid_object = uuid.UUID(
        bytes=hashlib.blake2b(
            json.dumps((args, kwargs), sort_keys=1, separators=(",", ":")).encode(
                "utf-8"
            ),
            digest_size=16,
        ).digest()
    )
    return six.text_type(base64.b32encode(uuid_object.bytes), encoding="utf-8").replace(
        "=", ""
    )  # to remove padding
