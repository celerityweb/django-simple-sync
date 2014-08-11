# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import json

try:
    from celery import shared_task as task
except ImportError:
    from celery import task
from django.db import models, Error as DatabaseError
try:
    from django.db.transaction import atomic
except ImportError:
    # Django <= 1.6
    from django.db.transaction import commit_on_success as atomic #noqa
from django.conf import settings

NULLIFY_PK = getattr(settings, 'SIMPLESYNC_NULLIFY_PK', False)

@task(name='simplesync-task')
def do_sync(operation, app_label, model_name, json_str):
    model_cls = models.get_model(app_label, model_name)
    json_obj = json.loads(json_str)
    if operation == 'delete':
        with atomic():
            model_cls.objects.filter(**json_obj).delete()
        return
    from .models import ModelSyncer
    syncer = ModelSyncer(model_cls)
    if operation == 'create':
        new_obj, m2m_data = syncer.from_json(json_obj)
        if NULLIFY_PK:
            new_obj.pk = None
        try:
            with atomic():
                new_obj.save(force_insert=True)
                for attr, value_list in m2m_data:
                    setattr(new_obj, attr, value_list)
                new_obj.save(force_update=True)
        except DatabaseError, e:
            logger.exception('Database error')
            do_sync.retry(exc=e)
    elif operation == 'update':
        updated_obj, m2m_data = syncer.from_json(json_obj)
        slug_field = syncer.find_slug_field(model_cls)
        try:
            with atomic():
                if slug_field:
                    # If we're tracking slug, PK shouldn't be part of the update
                    del updated_obj[model_cls._meta.pk.attrname]
                    slug = updated_obj.pop(slug_field.attname)
                    obj = model_cls.objects.get(**{slug_field.attname:slug})
                else:
                    pk = updated_obj.pop(model_cls._meta.pk.attrname)
                    obj = model_cls.objects.get(pk=pk)
                obj.__dict__.update(updated_obj)
                for attr, value_list in m2m_data:
                    setattr(obj, attr, value_list)
                obj.save(force_update=True)
        except DatabaseError, e:
            do_sync.retry(exc=e)
            logger.exception('Database error')



