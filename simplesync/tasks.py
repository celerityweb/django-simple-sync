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

NULLIFY_ALL_PKS = getattr(settings, 'SIMPLESYNC_NULLIFY_ALL_PKS', False)
LEGACY_PK_FIELD = getattr(settings, 'SIMPLESYNC_LEGACY_PK_FIELD', None)

@current_app.task(name='simplesync-task', ignore_result=True, max_retries=5)
def do_sync(operation, app_label, model_name, original_key, json_str):
    model_cls = models.get_model(app_label, model_name)
    logger.info('%s - %s.%s - %s', do_sync.request.id, app_label, model_name, original_key)
    from .models import __registry__
    syncer = __registry__.registered[model_cls](model_cls)
    if operation == 'delete':
        json_obj = json.loads(json_str)
        with atomic():
            # there may be natural keys in here
            for key, value in json_obj.items():
                if hasattr(value, '__iter__'):
                    field_name = key[:-3] if key.endswith('_id') else key
                    if field_name == 'pk':
                        try:
                            json_obj[key] = model_cls._default_manager.get_by_natural_key(*value).pk
                        except model_cls.DoesNotExist:
                            logger.warning('%s - DELETE - Could not find %s '
                                           'instance with natural key %s - aborting.',
                                           do_sync.request.id, model_cls, value)
                            return
                        continue
                    try:
                        field = model_cls._meta.get_field(field_name)
                    except models.FieldDoesNotExist:
                        continue
                    if not field.rel or not syncer.uses_natural_key(field.rel.to):
                        continue
                    try:
                        obj = field.rel.to._default_manager.get_by_natural_key(*value)
                    except field.rel.to.DoesNotExist:
                        logger.warning('%s - DELETE - Could not find related %s '
                                       'instance with natural key %s - aborting.',
                                       do_sync.request.id, field.rel.to, value)
                        return
                    json_obj[key] = obj.pk
            try:
                model_cls.objects.filter(**json_obj).delete()
            except TypeError:
                logger.exception('%s - %s', do_sync.request.id, json_obj)
        logger.info('%s - DELETED - %s - %s', do_sync.request.id, model_cls, json_obj)
    if operation == 'create':
        new_obj = None
        try:
            with atomic():
                new_obj, m2m_data = syncer.from_json(json_str)
                # If we're relying on natural keys, drop the pk value
                if syncer.uses_natural_key(new_obj) or NULLIFY_ALL_PKS:
                    logger.info('%s - %s.%s - before create, nulling PK',
                                do_sync.request.id, app_label, model_name)
                    if LEGACY_PK_FIELD and hasattr(new_obj, LEGACY_PK_FIELD):
                        setattr(new_obj, LEGACY_PK_FIELD, new_obj.pk)
                    new_obj.pk = None
                new_obj.save(force_insert=True)
                # for attr, value_list in m2m_data.items():
                #     if value_list:
                #         setattr(new_obj, attr, value_list)
        except (models.ObjectDoesNotExist,
                DatabaseError,
                DeserializationError), e:
            if new_obj:
                logger.warning('%s - Create failed: %s - %s', do_sync.request.id,
                               unicode(new_obj), e)
            else:
                logger.warning('%s - Create failed: %s - %s - %s', do_sync.request.id,
                               model_cls, json_str, e)
            try:
                raise do_sync.retry(exc=e)
            except do_sync.MaxRetriesExceededError, e:
                if new_obj:
                    logger.error('%s - Create failed permanently: %s', do_sync.request.id,
                                 unicode(new_obj))
                else:
                    logger.error('%s - Create failed permanently: %s', do_sync.request.id,
                                 json_str)
        else:
            logger.info('%s - CREATED - %s %s (%s)', do_sync.request.id, model_cls,
                        unicode(new_obj), new_obj.pk)
    if operation == 'update':
        updated_obj = None
        try:
            with atomic():
                updated_obj, m2m_data = syncer.from_json(json_str)
                if syncer.uses_natural_key(updated_obj):
                    original_obj = model_cls._default_manager.get_by_natural_key(*original_key)
                else:
                    original_obj = model_cls._default_manager.get(pk=original_key)
                logger.info('%s - %s.%s - before update, using PK %d',
                            do_sync.request.id, app_label, model_name, original_obj.pk)
                updated_obj.pk = original_obj.pk
                updated_obj.save(force_update=True)
        except (models.ObjectDoesNotExist,
                DatabaseError,
                DeserializationError), e:
            if updated_obj:
                logger.warning('%s - Update failed: %s - %s', do_sync.request.id, unicode(updated_obj), e)
            else:
                logger.warning('%s - Update failed: %s - %s - %s', do_sync.request.id, model_cls, json_str, e)
            try:
                raise do_sync.retry(exc=e)
            except do_sync.MaxRetriesExceededError, e:
                if updated_obj:
                    logger.error('%s - Update failed permanently: %s', do_sync.request.id, unicode(updated_obj))
                else:
                    logger.error('%s - Update failed permanently: %s', do_sync.request.id, json_str)
        else:
            logger.info('%s - UPDATED - %s %s (%s)', do_sync.request.id, model_cls,
                        unicode(updated_obj), updated_obj.pk)
