# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import json

from celery import current_app
from django.core.serializers.base import DeserializationError
from django.db import models
try:
    from django.db import Error as DatabaseError
except ImportError:
    # Django < 1.6
    from django.db import DatabaseError
try:
    from django.db.transaction import atomic
except ImportError:
    # Django < 1.6
    from django.db.transaction import commit_on_success as atomic #noqa
from django.conf import settings

NULLIFY_PK = getattr(settings, 'SIMPLESYNC_NULLIFY_PK', False)

@current_app.task(name='simplesync-task', ignore_result=True, max_retries=5)
def do_sync(operation, app_label, model_name, json_str):
    model_cls = models.get_model(app_label, model_name)
    if operation == 'delete':
        json_obj = json.loads(json_str)
        with atomic():
            # there may be natural keys in here
            for key, value in json_obj.items():
                if hasattr(value, '__iter__'):
                    if field_name == 'pk':
                        json_obj[key] = model_cls._default_manager.get_by_natural_key(*value).pk
                        continue
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
            try:
                model_cls.objects.filter(**json_obj).delete()
            except TypeError:
                logger.exception('%s', json_obj)
        logger.info('DELETED - %s - %s', model_cls, json_obj)
    from .models import ModelSyncer
    syncer = ModelSyncer(model_cls)
    if operation == 'create':
        try:
            with atomic():
                new_obj, m2m_data = syncer.from_json(json_str)
                if NULLIFY_PK:
                    new_obj.pk = None
                new_obj.save(force_insert=True)
                # for attr, value_list in m2m_data.items():
                #     if value_list:
                #         setattr(new_obj, attr, value_list)
        except (models.ObjectDoesNotExist,
                DatabaseError,
                DeserializationError), e:
            try:
                logger.warning('Create failed: %s - %s', unicode(new_obj), e)
            except Exception, _:
                logger.warning('Create failed: %s - %s - %s', model_cls, json_str, e)
            try:
                raise do_sync.retry(exc=e)
            except do_sync.MaxRetriesExceededError, e:
                try:
                    logger.error('Create failed permanently: %s', unicode(new_obj))
                except Exception, _:
                    logger.error('Create failed permanently: %s', json_str)
        else:
            logger.info('CREATED - %s', unicode(new_obj))
    if operation == 'update':
        try:
            with atomic():
                updated_obj, m2m_data = syncer.from_json(json_str)
                # If there's a natural key, get the object in the database
                # matching this natural key and use its local pk value
                if hasattr(updated_obj, 'natural_key') and callable(updated_obj.natural_key) \
                        and hasattr(type(updated_obj)._default_manager, 'get_by_natural_key'):
                    local_obj = type(updated_obj)._default_manager.get_by_natural_key(*updated_obj.natural_key())
                    updated_obj.pk = local_obj.pk
                # This shouldn't affect M2M relationships
                updated_obj.save(force_update=True)
        except (models.ObjectDoesNotExist,
                DatabaseError,
                DeserializationError), e:
            try:
                logger.warning('Update failed: %s - %s', unicode(updated_obj), e)
            except Exception, _:
                logger.warning('Update failed: %s - %s - %s', model_cls, json_str, e)
            try:
                raise do_sync.retry(exc=e)
            except do_sync.MaxRetriesExceededError, e:
                try:
                    logger.error('Update failed permanently: %s', unicode(updated_obj))
                except Exception, _:
                    logger.error('Update failed permanently: %s', json_str)
        else:
            logger.info('UPDATED - %s', unicode(updated_obj))



