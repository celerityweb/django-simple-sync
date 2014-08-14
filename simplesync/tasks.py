# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import json

try:
    from celery import shared_task as task
except ImportError:
    from celery import task
from django.db import models, Error as DatabaseError, connection
try:
    from django.db.transaction import atomic
except ImportError:
    # Django <= 1.6
    from django.db.transaction import commit_on_success as atomic #noqa
from django.conf import settings

NULLIFY_PK = getattr(settings, 'SIMPLESYNC_NULLIFY_PK', False)

@task(name='simplesync-task', ignore_result=True)
def do_sync(operation, app_label, model_name, json_str):
    if settings.DEBUG:
        logger.debug('audit: %s: %s on %s.%s - %s', do_sync.request.id, operation,
                     app_label, model_name, json_str)
        connection.queries = []
    model_cls = models.get_model(app_label, model_name)
    json_obj = json.loads(json_str)
    if operation == 'delete':
        with atomic():
            # there may be natural keys in here
            for key, value in json_obj.items():
                if hasattr(value, '__iter__'):
                    field_name = key[:-3] if key.endswith('_id') else key
                    try:
                        field = model_cls._meta.get_field(field_name)
                    except models.FieldDoesNotExist:
                        continue
                    if not field.rel:
                        continue
                    if not hasattr(field.rel.to._default_manager,
                                   'get_by_natural_key'):
                        continue
                    obj = field.rel.to._default_manager.get_by_natural_key(*value)
                    json_obj[key] = obj.pk
            model_cls.objects.filter(**json_obj).delete()
    from .models import ModelSyncer
    syncer = ModelSyncer(model_cls)
    if operation == 'create':
        new_obj, m2m_data = syncer.from_json(json_obj)
        if NULLIFY_PK:
            new_obj.pk = None
        try:
            with atomic():
                new_obj.save(force_insert=True)
                for attr, value_list in m2m_data.items():
                    if value_list:
                        setattr(new_obj, attr, value_list)
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
                    setattr(updated_obj, model_cls._meta.pk.attrname,
                            model_cls.objects.get(
                                **{slug_field.attname: getattr(updated_obj,
                                                               slug_field.attname)}))
                updated_obj.save(force_update=True)
        except DatabaseError, e:
            do_sync.retry(exc=e)
            logger.exception('Database error')
    if settings.DEBUG:
        for query in connection.queries:
            logger.debug('audit: %s: %s', do_sync.request.id, query)
        connection.queries = []



