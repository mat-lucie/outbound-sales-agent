"""CRM provider factory — the single construction seam for the engine.

``get_crm_provider()`` is where the engine decides *which* CRM adapter to build.
It reads ``config/crm.yaml`` (via :mod:`clients.settings`) when that file is
present and resolves the ``vendor`` key; for ``vendor: attio`` it builds an
:class:`~clients.crm.attio_provider.AttioProvider` over a freshly constructed
:class:`~clients.attio.AttioClient`.

``attio`` is the bundled reference adapter. Other vendors are added via the
``/onboard`` skill, which generates a ``clients/crm/<vendor>_provider.py`` and
registers a branch for it here; when you add a vendor, also update the
``Supported vendors`` list in the unknown-vendor ``ConfigError`` below so it
names the new adapter.

Backward-compatible default
---------------------------
When ``config/crm.yaml`` is **absent**, the factory defaults to ``vendor=attio``
and builds the Attio adapter straight from the environment — exactly what
``cli.py`` did before this seam existed (``AttioClient()`` reading
``ATTIO_API_KEY`` from ``os.environ``). Existing deployments and the test suite
therefore keep working with no config file and no behavior change.

Transition seam — the raw inner client
---------------------------------------
This is the *first* increment of threading the CRM seam. ``workflows/`` still
receive a raw :class:`~clients.attio.AttioClient` (not yet the ``CRMProvider``);
migrating them is a later increment. So the factory returns a :class:`CRMBundle`
carrying BOTH the neutral ``provider`` and the underlying ``attio`` client. The
factory owns the inner-client construction, so callers never reach into the
provider's private ``_attio`` — when the workflow migration lands, callers drop
``bundle.attio`` and keep ``bundle.provider``, and this accessor disappears with
no further surgery on the adapter.

The bundle is itself a context manager: entering yields the bundle, exiting
closes the provider (which closes the inner client) — matching today's
``with AttioClient() as attio:`` lifecycle exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import clients.attio
from clients.crm.attio_provider import AttioProvider
from clients.crm.mapping import load_crm_mapping
from clients.settings import ConfigError, config_dir, load_yaml, resolve_env_ref

if TYPE_CHECKING:
    from clients.attio import AttioClient
    from clients.crm.base import CRMProvider

_DEFAULT_VENDOR = "attio"


@dataclass
class CRMBundle:
    """Construction result: the neutral provider plus the raw inner client.

    ``provider`` is the vendor-neutral :class:`CRMProvider` the engine will
    standardize on. ``attio`` is the underlying :class:`AttioClient` that
    ``workflows/`` still consume directly during the seam-threading transition;
    it is ``None`` for any non-Attio vendor (none exist yet). Entering the
    bundle as a context manager yields the bundle; exiting closes the inner
    client, preserving today's ``with AttioClient() as attio:`` lifecycle.
    """

    provider: CRMProvider
    attio: AttioClient | None = None

    def __enter__(self) -> CRMBundle:
        return self

    def __exit__(self, *args: object) -> None:
        # Close the underlying resource directly. For Attio that's the inner
        # client (the exact object the pre-seam `with AttioClient()` closed);
        # the provider is a thin wrapper over the same client. `close()` is not
        # part of the CRMProvider ABC, so we close the concrete resource the
        # factory built rather than reaching through the neutral interface.
        if self.attio is not None:
            self.attio.close()


def _build_attio(api_key: str | None = None, *, client_factory=None) -> CRMBundle:
    """Build an Attio-backed bundle.

    Constructs the inner ``AttioClient`` here (so the factory, not the provider,
    owns the raw client the transition still needs) and composes the
    :class:`AttioProvider` around that exact instance. When ``api_key`` is
    ``None`` the client reads ``ATTIO_API_KEY`` from the environment itself —
    byte-identical to the pre-seam ``AttioClient()`` call ``cli.py`` made.

    ``clients.attio.AttioClient`` is referenced through the module (not a
    from-import binding) so the engine's canonical construction patch target —
    ``patch("clients.attio.AttioClient")``, used throughout the suite — keeps
    intercepting construction now that it flows through the factory.
    """
    # Load the field/stage mapping LIVE from config/crm.yaml (identity default
    # when the file is absent → byte-identical to the pre-mapping adapter).
    mapping = load_crm_mapping()
    # Thread the field_mapping into the INNER client too, so the client's own
    # hardcoded-slug reads/filters (person linkedin/name, company domain) honor
    # the operator's renamed slugs — the same mapping the provider uses for
    # stage translation, loaded once. Under the identity default this is the
    # canonical Attio map, so construction stays byte-identical.
    cls = client_factory or clients.attio.AttioClient
    attio = (
        cls(api_key=api_key, field_mapping=mapping.field_mapping)
        if api_key is not None
        else cls(field_mapping=mapping.field_mapping)
    )
    return CRMBundle(provider=AttioProvider(attio, mapping=mapping), attio=attio)


def get_crm_provider(*, client_factory=None) -> CRMBundle:
    """Construct the engine's CRM provider from ``config/crm.yaml`` (or env).

    Resolution order:

    1. If ``config/crm.yaml`` exists, load it and read ``vendor``. For
       ``vendor: attio``, resolve the optional ``credentials.api_key_env``
       env-var name and build the Attio adapter with it.
    2. If ``config/crm.yaml`` is absent, default to ``vendor=attio`` built from
       the environment (backward-compatible — the pre-seam behavior).

    An unrecognized ``vendor`` raises :class:`ConfigError` naming the vendor.

    Returns:
        A :class:`CRMBundle` carrying the neutral provider and (for Attio) the
        raw inner client the workflow-migration transition still threads.

    Raises:
        ConfigError: On an unknown vendor, or (transitively, via the inner
            client / ``resolve_env_ref``) a missing required credential.
    """
    config_path = config_dir() / "crm.yaml"
    if not config_path.exists():
        # No config file → backward-compatible Attio-from-env default.
        return _build_attio(client_factory=client_factory)

    config = load_yaml("crm")
    vendor = config.get("vendor", _DEFAULT_VENDOR)

    if vendor == "attio":
        # credentials.api_key_env is the *name* of the env var holding the key.
        # Resolve it when the block is present; otherwise let AttioClient read
        # ATTIO_API_KEY itself (identical to the no-config default path).
        credentials = config.get("credentials") or {}
        if not isinstance(credentials, dict):
            raise ConfigError(
                "config/crm.yaml 'credentials' must be a mapping, "
                f"got {type(credentials).__name__}."
            )
        api_key = resolve_env_ref(credentials, "api_key_env", required=False)
        return _build_attio(api_key=api_key, client_factory=client_factory)

    raise ConfigError(
        f"Unknown CRM vendor {vendor!r} in config/crm.yaml. "
        f"Supported vendors: 'attio'."
    )
