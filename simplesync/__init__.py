# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

__version__ = '0.1'

def register(model, cls=None):
    from .models import ModelSyncer, __registry__
    if not cls:
        cls = ModelSyncer
    __registry__.register(model, cls)

