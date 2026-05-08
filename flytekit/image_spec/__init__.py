"""
==========
ImageSpec
==========

.. currentmodule:: flytekit.image_spec

This module contains the ImageSpec class parameters and methods.

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   ImageSpec
"""

from .default_builder import DefaultImageBuilder
from .image_spec import ImageBuildEngine, ImageSpec
from .nix_builder import NixImageSpecBuilder, nix_image_spec

# Set this to a lower priority compared to `envd` to maintain backward compatibility
ImageBuildEngine.register("default", DefaultImageBuilder(), priority=1)
ImageBuildEngine.register("nix", NixImageSpecBuilder(), priority=0)
